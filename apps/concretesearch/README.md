# ConcreteSearch

ConcreteSearch is a distributed-ready full-text search engine built with **Rust**, **Tantivy**, and **FoundationDB**.

It demonstrates how to decouple the **Compute** (Indexing/Search) from the **Storage** (FoundationDB), allowing for stateless search nodes that can scale horizontally.

## 🏗 Architecture

### The Problem with Standard Search Engines
Traditional search engines (like Elasticsearch or Solr) often couple the index storage with the node processing it. If a node dies, you need replicas. Scaling involves complex sharding and data movement.

### The ConcreteSearch Solution
ConcreteSearch uses **Tantivy** (a blazingly fast Rust search library equivalent to Lucene) but intercepts how it writes data to disk.

1. **Storage Layer (FoundationDB)**: All index files are split into small chunks and stored as Key-Value pairs in FoundationDB. This is our "Source of Truth".
2. **Compute Layer (Stateless Nodes)**: The search application (`concretesearch`) behaves like a stateless web server.
   - **Lazy Loading**: When it needs to read an index file, it checks a local cache (`/tmp/concrete_cache`). If missing, it downloads it from FDB on demand.
   - **Write Buffering**: When writing, it buffers documents in memory and flushes them chunk-by-chunk to FDB asynchronously.

This means you can have 10 identical search nodes running behind a load balancer. Any node can answer any query because they all share the same FDB storage backend.

## 🚀 Features

- **Store-independent**: Index files live in FoundationDB.
- **Async Indexing**: Support for high-throughput, non-blocking writes using an internal queue pattern.
- **Full-Text Search**: Supports syntax like `title:rust AND body:awesome`.
- **Stateless**: easy to deploy on Kubernetes/Serverless.

## 🛠 Project Structure

- **`src/main.rs`**: The entry point. Initializes the database, Tantivy index, and starts the Web Server.
- **`src/api.rs`**: Defines the HTTP endpoints (`/index/sync`, `/index/async`, `/search`) and the background queue worker.
- **`src/fdb_directory.rs`**: The core magic. A custom implementation of Tantivy's `Directory` trait that bridges the gap between the search engine and FoundationDB.

## 🚥 How to Run

### Prerequisites
- **Rust Toolchain** (cargo, rustc)
- **FoundationDB Server** running locally (or configured via `fdb.cluster`).

### Running the Server
```bash
cargo run --release
```
 The server listens on `0.0.0.0:3000`.

## 🔌 API Usage

### 1. Synchronous Indexing (`POST /index/sync`)
Adds a document and waits for it to be safely committed to FDB. Slower per-request, but immediately searchable.

```bash
curl -X POST http://localhost:3000/index/sync \
  -H "Content-Type: application/json" \
  -d '{
    "id": "doc1",
    "doc": {
      "title": "Learning Rust",
      "body": "Rust is a systems programming language..."
    }
  }'
```

### 2. Asynchronous Indexing (`POST /index/async`)
Adds a document to a high-speed queue in FDB and returns immediately (`202 Accepted`). A background worker process picks it up and indexes it in batches. Perfect for high-ingestion rates.

```bash
curl -X POST http://localhost:3000/index/async \
  -H "Content-Type: application/json" \
  -d '{
    "id": "doc2",
    "doc": {
      "title": "Async IO",
      "body": "Non-blocking I/O is crucial for performance..."
    }
  }'
```

### 3. Search (`POST /search`)
Queries the index. Uses Lucene-like query syntax (provided by Tantivy).

```bash
curl -X POST http://localhost:3000/search \
  -H "Content-Type: application/json" \
  -d '{ "query": "body.title:Rust" }'
```

**Response:**
```json
{
  "hits": [
    {
      "id": "doc1",
      "doc": {
        "title": "Learning Rust",
        "body": "Rust is a systems programming language..."
      }
    }
  ]
}
```

## 🧠 Code Walkthrough (For Non-Rust Devs)

We have heavily commented the source code files to explain what's happening step-by-step.

- **Check `src/main.rs`** to see how the application wires together the database and web server.
- **Check `src/api.rs`** to understand how web requests are handled and how `Arc<Mutex<...>>` allows safe shared access to the index.
- **Check `src/fdb_directory.rs`** to dive deep into how we map a "File" concept onto a "Key-Value Store" concept.

## 🧪 Testing

We have a robust test suite covering:
1. **Unit Tests**: Verifying the FdbDirectory logic (write/read/delete).
2. **Integration Tests**: Creating a real index, writing data (sync & async), and verifying search results match expectations.
3. **Coverage**: >90% code coverage on core logic.

Run tests with:
```bash
cargo test
```
