use axum::{
    routing::{get, post},
    Router,
};
use foundationdb::Database;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tantivy::{
    doc,
    schema::Schema,
    Index,
};
use tokio::net::TcpListener;

mod api;
mod fdb_directory;
pub mod test_utils;

// Import handlers and state from the API module
use api::{AppState, queue_worker};
use fdb_directory::FdbDirectory;

/// The main entry point of the asynchronous application.
/// `#[tokio::main]` is a macro that sets up the Tokio async runtime, allowing us to use `async/await`.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the FoundationDB network. 
    // `unsafe` is required because this FFI call initializes global state.
    // In a production app, the network thread needs to be managed carefully (spawned in a separate thread),
    // but `boot()` simplifies this for standard usage.
    let network = unsafe { foundationdb::boot() };
    
    // Connect to the FoundationDB cluster using the default `fdb.cluster` configuration file.
    // `Arc` (Atomic Reference Counted) allows safely sharing the database handle across multiple threads/tasks.
    let db = Arc::new(Database::new(None)?);

    // ---------------------------------------------------------
    // Define the Search Schema
    // ---------------------------------------------------------
    // We load the schema from FDB if it exists, otherwise we create it and save it.
    let schema = load_or_create_schema(&db).await?;
    let id_field = schema.get_field("id").unwrap();
    let body_field = schema.get_field("body").unwrap();

    // ---------------------------------------------------------
    // Setup Directory (Storage Layer)
    // ---------------------------------------------------------
    // We use a custom `FdbDirectory` backend that stores index files in FoundationDB.
    // It also uses a local file system cache (`/tmp/concrete_cache`) to speed up reads.
    let cache_path = PathBuf::from("/tmp/concrete_cache");
    let fdb_directory = FdbDirectory::new(db.clone(), b"concrete_index", cache_path)?;

    // Open or create the index. If it doesn't exist in FDB, it will be initialized.
    let index = Index::open_or_create(fdb_directory, schema.clone())?;

    // ---------------------------------------------------------
    // Setup Index Writer and Reader
    // ---------------------------------------------------------
    // The IndexWriter is responsible for adding documents to the index.
    // It allocates a 50MB RAM buffer for holding documents before they are flushed to storage.
    // In Tantivy, only one writer can exist at a time (single writer, multiple readers).
    let index_writer = index.writer(50_000_000)?;

    // The IndexReader is used to search the index. It provides a point-in-time view of the index.
    // We use Manual policy and explicitly reload after commits to ensure freshness.
    let index_reader = index
        .reader_builder()
        .reload_policy(tantivy::ReloadPolicy::Manual)
        .try_into()?;

    // ---------------------------------------------------------
    // Shared Application State
    // ---------------------------------------------------------
    // This struct holds all resources that need to be accessed by API handlers.
    // `Arc` allows us to share this state across all incoming web requests.
    // `Mutex` protects the `index_writer` because it assumes single-threaded access, while our web server is multi-threaded.
    let app_state = Arc::new(AppState {
        index_writer: Arc::new(Mutex::new(index_writer)),
        index_reader,
        db: db.clone(),
        schema,
        id_field,
        body_field,
    });

    // ---------------------------------------------------------
    // Background Worker
    // ---------------------------------------------------------
    // Spawn a background task (lightweight thread) to process the async indexing queue.
    // This allows the `/index/async` endpoint to return immediately while the actual indexing happens here.
    let worker_state = app_state.clone();
    tokio::spawn(async move {
        queue_worker(worker_state).await;
    });

    // ---------------------------------------------------------
    // Web Server Setup (Axum)
    // ---------------------------------------------------------
    // Define the routes and attach the handlers.
    // `.with_state(app_state)` injects our shared state into every handler.
    let app = Router::new()
        .route("/index/sync", post(api::sync_index))     // POST /index/sync
        .route("/index/async", post(api::async_index))   // POST /index/async
        .route("/delete/sync", post(api::sync_delete))   // POST /delete/sync
        .route("/delete/async", post(api::async_delete)) // POST /delete/async
        .route("/search", post(api::search_handler))     // POST /search
        .route("/health", get(api::health_check))
        .with_state(app_state);

    // Bind the server to all interfaces (0.0.0.0) on port 3000.
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("ConcreteSearch listening on {}", addr);
    
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    // Drop the network guard when the server stops (mostly for clean shutdown logic).
    drop(network);
    Ok(())
}

/// Loads the schema from FDB or creates/saves default if missing.
async fn load_or_create_schema(db: &Database) -> Result<Schema, Box<dyn std::error::Error>> {
    use foundationdb::tuple::Subspace;
    use tantivy::schema::{TEXT, STORED, STRING};

    let subspace = Subspace::from(&b"system"[..]);
    let key = subspace.pack(&"schema");
    
    let maybe_schema_json: Option<Vec<u8>> = db.run(|trx, _maybe_committed| {
        let key = key.clone();
        async move {
            Ok(trx.get(&key, false).await.map(|v| v.map(|s| s.to_vec()))?)
        }
    }).await?;

    if let Some(json_bytes) = maybe_schema_json {
        println!("Loaded schema from FDB.");
        let json_str = String::from_utf8(json_bytes)?;
        let schema_json: serde_json::Value = serde_json::from_str(&json_str)?;
        let schema: Schema = serde_json::from_value(schema_json)?;
        Ok(schema)
    } else {
        println!("Schema not found in FDB. Creating default.");
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("id", STRING | STORED);
        schema_builder.add_json_field("body", TEXT | STORED);
        let schema = schema_builder.build();
        
        // Save it
        let schema_json = serde_json::to_vec(&schema)?;
        db.run(|trx, _maybe_committed| {
            let key = key.clone();
            let val = schema_json.clone();
            async move {
                trx.set(&key, &val);
                Ok(())
            }
        }).await?;
        
        Ok(schema)
    }
}
