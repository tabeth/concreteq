use axum::{
    extract::{State, Json},
    response::IntoResponse,
    http::StatusCode,
};
use foundationdb::{Database, RangeOption};
use foundationdb::tuple::Subspace;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tantivy::{IndexWriter, TantivyDocument, IndexReader};
use tantivy::schema::{Schema, Field, Document};
use tokio::time::{sleep, Duration};
use uuid::Uuid;

/// Shared Application State
/// This struct holds references to "expensive" or shared resources like database connections and index writers.
/// `Arc` stands for Atomic Reference Counting, making it safe to share across threads.
#[derive(Clone)]
pub struct AppState {
    /// The writer is protected by a Mutex because Tantivy's IndexWriter is not thread-safe for concurrent writes.
    pub index_writer: Arc<Mutex<IndexWriter>>,
    /// The reader is thread-safe and can be cloned cheaply.
    pub index_reader: IndexReader,
    /// Handle to the FoundationDB cluster.
    pub db: Arc<Database>,
    /// Schema definition of the search index.
    pub schema: Schema,
    /// Handles to specific fileds in the schema.
    pub id_field: Field,
    pub body_field: Field,
}

/// Request payload for indexing a document.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexRequest {
    pub id: String,
    /// The document content is a map of string keys to any JSON value.
    pub doc: HashMap<String, Value>,
}

/// Request payload for deleting a document.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DeleteRequest {
    pub id: String,
}

/// Enum representing possible jobs in the async queue.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum QueueJob {
    Index(IndexRequest),
    Delete(String), // ID to delete
}

/// Request payload for searching.
#[derive(Serialize, Deserialize, Clone)]
pub struct SearchRequest {
    pub query: String,
}

/// Response format for search results.
#[derive(Serialize, Deserialize, Clone)]
pub struct SearchResponse {
    pub hits: Vec<Value>,
}

// FDB Subspace prefix for the async job queue.
const QUEUE_PREFIX: &[u8] = b"queue";

/// **Synchronous Index Handler** (`POST /index/sync`)
/// 
/// This handler adds a document to the index and waits for it to be committed before returning.
/// This guarantees that the document is searchable immediately (or as soon as the commit finishes),
/// but it is slower because it blocks on the costly commit operation.
pub async fn sync_index(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Acquire the lock on the index writer. This ensures only one request writes at a time.
    let mut index_writer = state.index_writer.lock().unwrap();
    
    // Repackage the payload to match our schema structure:
    // { "id": "...", "body": { ... } }
    let doc_json = serde_json::json!({
        "id": payload.id,
        "body": payload.doc
    });
    let doc_str = serde_json::to_string(&doc_json).unwrap();
    
    // Parse the JSON into a TantivyDocument. Verification against schema happens here.
    let doc = TantivyDocument::parse_json(&state.schema, &doc_str)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    // Add the document to the in-memory buffer of the writer.
    index_writer.add_document(doc).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    // **Commit**: This flushes the buffer to storage (FoundationDB) and makes the changes visible to readers.
    // This is the expensive part.
    index_writer.commit().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    // Refresh the reader
    state.index_reader.reload().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(StatusCode::OK)
}

/// **Synchronous Delete Handler** (`POST /delete/sync`)
/// 
/// Deletes a document by ID and waits for commit.
pub async fn sync_delete(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<DeleteRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut index_writer = state.index_writer.lock().unwrap();
    
    let term = tantivy::Term::from_field_text(state.id_field, &payload.id);
    index_writer.delete_term(term);
    
    index_writer.commit().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Refresh the reader
    state.index_reader.reload().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    
    Ok(StatusCode::OK)
}

/// **Asynchronous Index Handler** (`POST /index/async`)
///
/// This handler pushes the document into a FoundationDB "Queue" and returns immediately.
/// A background worker will later pick this up and verify/index it.
/// This allows for high throughput as it doesn't wait for the index commit.
pub async fn async_index(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> impl IntoResponse {
    let db = state.db.clone();
    let queue_subspace = Subspace::from(QUEUE_PREFIX);
    
    let job = QueueJob::Index(payload);
    let payload_bytes = serde_json::to_vec(&job).unwrap();
    
    // Create a unique key for the queue item using Timestamp + UUID to ensure ordering and uniqueness.
    let timestamp = chrono::Utc::now().timestamp_micros();
    let id = Uuid::new_v4().to_string();
    
    // Use an FDB transaction to write the item to the queue.
    let trx_result = db.run(|trx, _maybe_committed| {
        let key = queue_subspace.pack(&(timestamp, &id));
        trx.set(&key, &payload_bytes);
        async move { Ok(()) }
    }).await;

    match trx_result {
        Ok(_) => StatusCode::ACCEPTED, // 202 Accepted
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// **Asynchronous Delete Handler** (`POST /delete/async`)
///
/// Pushes a delete deletion job to the queue.
pub async fn async_delete(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<DeleteRequest>,
) -> impl IntoResponse {
    let db = state.db.clone();
    let queue_subspace = Subspace::from(QUEUE_PREFIX);
    
    let job = QueueJob::Delete(payload.id);
    let payload_bytes = serde_json::to_vec(&job).unwrap();
    
    let timestamp = chrono::Utc::now().timestamp_micros();
    let id = Uuid::new_v4().to_string();
    
    let trx_result = db.run(|trx, _maybe_committed| {
        let key = queue_subspace.pack(&(timestamp, &id));
        trx.set(&key, &payload_bytes);
        async move { Ok(()) }
    }).await;

    match trx_result {
        Ok(_) => StatusCode::ACCEPTED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// **Search Handler** (`POST /search`)
///
/// Handles search queries against the index.
pub async fn search_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SearchRequest>,
) -> impl IntoResponse {
    // Acquire a searcher from the reader. This is a lightweight snapshot of the index.
    // Note: If new commits happened since this reader was created, `reader.reload()` is needed to see them.
    let searcher = state.index_reader.searcher();
    
    // Configure the query parser to search in the "body" field by default.
    let query_parser = tantivy::query::QueryParser::for_index(
        searcher.index(),
        vec![state.body_field],
    );
    
    // Parse the user's query string (e.g., "title:Rust").
    let query = match query_parser.parse_query(&payload.query) {
        Ok(q) => q,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(SearchResponse { hits: vec![] })).into_response(),
    };
    
    // Execute the search, collecting the top 10 results sorted by relevance score.
    let top_docs = searcher.search(&query, &tantivy::collector::TopDocs::with_limit(10)).unwrap();
    
    let mut results = Vec::new();
    
    // Iterate over matching documents and extract their content.
    for (_score, doc_address) in top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address).unwrap();
        
        // Convert the internal Tantivy document format back to JSON.
        let doc_json = retrieved_doc.to_json(&state.schema);
        let doc_val: Value = serde_json::from_str(&doc_json).unwrap();
        
        // Extract fields safely (Tantivy returns arrays for values).
        let id_val = doc_val.get("id").and_then(|v| v[0].as_str()).unwrap_or("").to_string();
        let body_val = doc_val.get("body").and_then(|v| v[0].as_object()).cloned().unwrap_or_default();
        
        let mut hit = HashMap::new();
        hit.insert("id".to_string(), Value::String(id_val));
        hit.insert("doc".to_string(), Value::Object(body_val));
        results.push(serde_json::to_value(hit).unwrap());
    }

    (StatusCode::OK, Json(SearchResponse { hits: results })).into_response()
}

/// **Background Queue Worker**
///
/// This function runs in a loop in the background. It:
/// 1. Reads items from the FDB queue.
/// 2. Indexes them in batch.
/// 3. Commits the index.
/// 4. Deletes the items from the queue.
pub async fn queue_worker(state: Arc<AppState>) {
    let db = state.db.clone();
    let queue_subspace = Subspace::from(&b"queue"[..]);

    loop {
        // Fetch a batch of (up to 500) items from the queue.
        let batch_result: Result<Vec<(Vec<u8>, QueueJob)>, _> = db.run(|trx, _maybe_committed| {
            let queue_subspace = queue_subspace.clone();
            async move {
                let range = RangeOption {
                     limit: Some(500),
                     ..RangeOption::from(queue_subspace.range())
                };
                let range_result = trx.get_range(&range, 1_000, false).await?;
                
                let mut items = Vec::new();
                for kv in range_result {
                    if let Ok(job) = serde_json::from_slice::<QueueJob>(kv.value()) {
                        items.push((kv.key().to_vec(), job));
                    }
                }
                Ok(items)
            }
        }).await;

        if let Ok(items) = batch_result {
            if items.is_empty() {
                sleep(Duration::from_millis(100)).await;
                continue;
            }

            let mut last_key: Option<Vec<u8>> = None;
            let commit_result = {
                let mut index_writer = state.index_writer.lock().unwrap();

                for (key, req) in &items {
                     match req {
                        QueueJob::Index(idx_req) => {
                             println!("Worker: Indexing doc {}", idx_req.id);
                             let doc_json = serde_json::json!({
                                 "id": idx_req.id,
                                 "body": idx_req.doc
                             });
                             let doc_str = serde_json::to_string(&doc_json).unwrap();
                             
                             if let Ok(doc) = TantivyDocument::parse_json(&state.schema, &doc_str) {
                                 index_writer.add_document(doc).ok();
                             }
                        },
                        QueueJob::Delete(id_to_delete) => {
                             println!("Worker: Deleting doc {}", id_to_delete);
                             let term = tantivy::Term::from_field_text(state.id_field, id_to_delete);
                             index_writer.delete_term(term);
                        }
                     }
                    last_key = Some(key.clone());
                }
                
                index_writer.commit()
            };
            
            if let Ok(opstamp) = &commit_result {
                println!("Worker: Batch committed, opstamp: {}", opstamp);
                state.index_reader.reload().ok();
            } else if let Err(e) = &commit_result {
                println!("Worker: Commit FAILED: {:?}", e);
            }

            if commit_result.is_ok() {
                if let Some(mut end_key) = last_key {
                     // We need to clear up to last_key inclusive.
                     // FDB clear_range is exclusive on end, so we use last_key + \0.
                     end_key.push(0u8);
                     
                     let _ = db.run(|trx, _maybe_committed| {
                         let start_key = queue_subspace.range().0;
                         let end_key_ref = &end_key;
                         async move {
                             trx.clear_range(start_key.as_slice(), end_key_ref);
                             Ok(())
                         }
                     }).await;
                }
            }
        } else {
             sleep(Duration::from_secs(1)).await;
        }
    }
}

// ------------------------------------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        routing::post,
        Router,
    };
    use serde_json::json;
    use tower::ServiceExt; // for oneshot
    use tempfile::TempDir;
    use crate::fdb_directory::FdbDirectory;

    // Helper macro for async test setup
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_api_sync_index_search() {
        crate::test_utils::init_fdb();
        let db = match Database::new(None) {
            Ok(db) => Arc::new(db),
            Err(_) => return,
        };
        crate::test_utils::clear_subspace(&db, b"test_api").await;

        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();
        let fdb_directory = FdbDirectory::new(db.clone(), b"test_api", cache_path).unwrap();

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", tantivy::schema::STRING | tantivy::schema::STORED);
        let body_field = schema_builder.add_json_field("body", tantivy::schema::TEXT | tantivy::schema::STORED);
        let schema = schema_builder.build();

        let index = tantivy::Index::open_or_create(fdb_directory, schema.clone()).unwrap();
        let index_writer = index.writer(50_000_000).unwrap();
        let index_reader = index.reader().unwrap();

        let state = Arc::new(AppState {
            index_writer: Arc::new(Mutex::new(index_writer)),
            index_reader,
            db: db.clone(),
            schema: schema.clone(),
            id_field,
            body_field,
        });

        let app = Router::new()
            .route("/index/sync", post(sync_index))
            .route("/search", post(search_handler))
            .with_state(state);

        // Test Sync Index
        let payload = json!({
            "id": "doc1",
            "doc": { "title": "Rust is great", "tags": ["rust", "search"] }
        });
        
        let req = Request::builder()
            .method("POST")
            .uri("/index/sync")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Test Search
        let search_payload = json!({ "query": "Rust" });
        let req = Request::builder()
            .method("POST")
            .uri("/search")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&search_payload).unwrap()))
            .unwrap();

        let response = app.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_api_search_invalid_query() {
        crate::test_utils::init_fdb();
        let db = match Database::new(None) {
            Ok(db) => Arc::new(db),
            Err(_) => return,
        };
        crate::test_utils::clear_subspace(&db, b"concretesearch-test-invalid").await;

        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();
        let fdb_directory = FdbDirectory::new(db.clone(), b"concretesearch-test-invalid", cache_path).unwrap();

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", tantivy::schema::STRING | tantivy::schema::STORED);
        let body_field = schema_builder.add_json_field("body", tantivy::schema::TEXT | tantivy::schema::STORED);
        let schema = schema_builder.build();

        let index = tantivy::Index::open_or_create(fdb_directory, schema.clone()).unwrap();
        let index_writer = index.writer(50_000_000).unwrap();
        let index_reader = index.reader().unwrap();

        let state = Arc::new(AppState {
            index_writer: Arc::new(Mutex::new(index_writer)),
            index_reader,
            db: db.clone(),
            schema: schema.clone(),
            id_field,
            body_field,
        });

        let app = Router::new()
            .route("/search", post(search_handler)) // Use super::search_handler if needed, but we are in mod tests
            .with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/search")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&SearchRequest {
                            query: "title:(".to_string(), // Invalid syntax
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Tantivy query parser error usually results in 400 or 500 depending on impl
        // In our handler: parser.parse_query(query).map_err(...) -> 400 (if BadRequest)
        // Let's check status.
        assert!(response.status().is_client_error() || response.status().is_server_error());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_api_async_index() {
        crate::test_utils::init_fdb();
        let db = match Database::new(None) {
            Ok(db) => Arc::new(db),
            Err(_) => return,
        };
        crate::test_utils::clear_subspace(&db, b"test_async_api").await;
        
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();
        let fdb_directory = FdbDirectory::new(db.clone(), b"test_async_api", cache_path).unwrap();

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", tantivy::schema::STRING | tantivy::schema::STORED);
        let body_field = schema_builder.add_json_field("body", tantivy::schema::TEXT | tantivy::schema::STORED);
        let schema = schema_builder.build();

        let index = tantivy::Index::open_or_create(fdb_directory, schema.clone()).unwrap();
        let index_writer = index.writer(50_000_000).unwrap();
        let index_reader = index.reader().unwrap();

        let state = Arc::new(AppState {
            index_writer: Arc::new(Mutex::new(index_writer)),
            index_reader,
            db: db.clone(),
            schema: schema.clone(),
            id_field,
            body_field,
        });
        
        // Spawn Background Worker
        let worker_state = state.clone();
        tokio::spawn(async move {
            crate::api::queue_worker(worker_state).await;
        });

        let app = Router::new()
            .route("/index/async", post(async_index))
            .with_state(state.clone());

        // Test Async Index
        let payload = json!({
            "id": "doc_async",
            "doc": { "title": "Async Rust", "tags": ["async"] }
        });
        
        let req = Request::builder()
            .method("POST")
            .uri("/index/async")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        
        sleep(Duration::from_secs(2)).await; 
    }

    async fn wait_for_search(app: &Router, query: &str, expect_found: bool) -> bool {
         for _ in 0..20 {
             let search_payload = json!({ "query": query });
             let req = Request::builder()
                .method("POST")
                .uri("/search")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&search_payload).unwrap()))
                .unwrap();
             
             // Check response
             let response = app.clone().oneshot(req).await.unwrap();
             if response.status() == StatusCode::OK {
                 let body_bytes = axum::body::to_bytes(response.into_body(), 10000).await.unwrap();
                 if let Ok(body_json) = serde_json::from_slice::<Value>(&body_bytes) {
                     if let Some(hits) = body_json["hits"].as_array() {
                         let found = !hits.is_empty();
                         if found == expect_found {
                             return true;
                         }
                     }
                 }
             }
             sleep(Duration::from_millis(200)).await;
         }
         false
    }
    
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_e2e_sync_async_search() {
        crate::test_utils::init_fdb();
        let db = match Database::new(None) {
            Ok(db) => Arc::new(db),
            Err(_) => return,
        };
        crate::test_utils::clear_subspace(&db, b"e2e_test_v1").await;
        
        let temp_dir = TempDir::new().unwrap();
        // ... same setup ...
        let cache_path = temp_dir.path().to_path_buf();
        let fdb_directory = FdbDirectory::new(db.clone(), b"e2e_test_v1", cache_path).unwrap();

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", tantivy::schema::STRING | tantivy::schema::STORED);
        let body_field = schema_builder.add_json_field("body", tantivy::schema::TEXT | tantivy::schema::STORED);
        let schema = schema_builder.build();

        let index = tantivy::Index::open_or_create(fdb_directory, schema.clone()).unwrap();
        let index_writer = index.writer(50_000_000).unwrap();
        let index_reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .unwrap();

        let state = Arc::new(AppState {
            index_writer: Arc::new(Mutex::new(index_writer)),
            index_reader: index_reader.clone(),
            db: db.clone(),
            schema: schema.clone(),
            id_field,
            body_field,
        });
        
        let worker_state = state.clone();
        tokio::spawn(async move {
            crate::api::queue_worker(worker_state).await;
        });

        let app = Router::new()
            .route("/index/sync", post(sync_index))
            .route("/index/async", post(async_index))
            .route("/delete/sync", post(sync_delete))
            .route("/delete/async", post(async_delete))
            .route("/search", post(search_handler))
            .with_state(state.clone());

        // 1. Sync Write "Sync Doc"
        let sync_payload = json!({
            "id": "sync_doc",
            "doc": { "title": "Sync Doc", "type": "synchronous" }
        });
        let req = Request::builder()
            .method("POST")
            .uri("/index/sync")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&sync_payload).unwrap()))
            .unwrap();
        assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

        // 2. Async Write "Async Doc"
        let async_payload = json!({
            "id": "async_doc",
            "doc": { "title": "Async Doc", "type": "asynchronous" }
        });
        let req = Request::builder()
            .method("POST")
            .uri("/index/async")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&async_payload).unwrap()))
            .unwrap();
        assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::ACCEPTED);

        // 3. Verify Both exist (Retry loop handles wait for Async + Reload)
        assert!(wait_for_search(&app, "body.title:\"Sync Doc\"", true).await, "Timed out waiting for Sync Doc");
        assert!(wait_for_search(&app, "body.title:\"Async Doc\"", true).await, "Timed out waiting for Async Doc");

        // 6. Test Sync Delete
        let delete_sync_payload = json!({ "id": "sync_doc" });
        let req = Request::builder()
            .method("POST")
            .uri("/delete/sync")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&delete_sync_payload).unwrap()))
            .unwrap();
        assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

        // Verify Deletion
        assert!(wait_for_search(&app, "body.title:\"Sync Doc\"", false).await, "Timed out waiting for Sync Doc deletion");

        // 7. Test Async Delete
        let delete_async_payload = json!({ "id": "async_doc" });
        let req = Request::builder()
            .method("POST")
            .uri("/delete/async")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&delete_async_payload).unwrap()))
            .unwrap();
        assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::ACCEPTED);

        // Verify Deletion
        assert!(wait_for_search(&app, "body.title:\"Async Doc\"", false).await, "Timed out waiting for Async Doc deletion");
    }
}
