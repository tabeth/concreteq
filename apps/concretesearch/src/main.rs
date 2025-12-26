use axum::{
    routing::post,
    Router,
};
use foundationdb::Database;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tantivy::schema::{Schema, TEXT, STORED};
use tantivy::Index;
use tokio::net::TcpListener;

mod api;
mod fdb_directory;
pub mod test_utils;

// Import handlers and state from the API module
use api::{AppState, sync_index, async_index, search_handler, queue_worker};
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
    // Tantivy requires a strict schema definition for the index.
    let mut schema_builder = Schema::builder();
    
    // `TEXT | STORED`:
    // - TEXT: The field is tokenized and indexed for full-text search.
    // - STORED: The original field value is stored in the index so it can be retrieved in results.
    let id_field = schema_builder.add_text_field("id", TEXT | STORED);
    let body_field = schema_builder.add_json_field("body", TEXT | STORED);
    
    // Build the final schema object.
    let schema = schema_builder.build();

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
    // It needs to be reloaded to see new changes committed by the writer.
    let index_reader = index.reader()?;

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
        .route("/index/sync", post(sync_index))     // POST /index/sync
        .route("/index/async", post(async_index))   // POST /index/async
        .route("/search", post(search_handler))     // POST /search
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
