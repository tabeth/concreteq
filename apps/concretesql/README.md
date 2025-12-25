# ConcreteSQL

ConcreteSQL is a high-availability, distributed SQLite VFS (Virtual File System) adapter backend by **FoundationDB**. It allows you to run standard SQLite applications while transparently persisting data to a distributed, transactional Key-Value store.

## Features

-   **Distributed Storage**: SQLite database files are chunked (paged) and stored in FoundationDB.
-   **High Availability**: Inherits FDB's fault tolerance and distributed nature.
-   **Lock Leasing & Recovery**: Implements a robust distributed locking protocol with heartbeats and lease expiration to handle client crashes.
-   **Optimistic & Pessimistic Locking**: Fully supports `BEGIN DEFERRED` (Lazy) and `BEGIN IMMEDIATE` (Pessimistic) transaction modes.
-   **Configurable Page Size**: Supports arbitrary page sizes (e.g., 8KB, 16KB) via `PRAGMA page_size`.
-   **Robust Vacuum**: Implements chunked, shadow-pruning garbage collection to clean up old MVCC versions without blocking.

## Prerequisites

To use ConcreteSQL, you need a running **FoundationDB** cluster (or single-node server).

1.  **Install FoundationDB**:
    Follow the official guide: [Getting Started with FoundationDB](https://apple.github.io/foundationdb/getting-started-linux.html).
    
    For Linux (Ubuntu/Debian):
    ```bash
    wget https://github.com/apple/foundationdb/releases/download/7.1.26/foundationdb-clients_7.1.26-1_amd64.deb
    wget https://github.com/apple/foundationdb/releases/download/7.1.26/foundationdb-server_7.1.26-1_amd64.deb
    sudo dpkg -i foundationdb-clients_7.1.26-1_amd64.deb foundationdb-server_7.1.26-1_amd64.deb
    ```

2.  **Verify FDB Connection**:
    Ensure you can connect to the database:
    ```bash
    fdbcli --exec "status"
    ```

## Installation

ConcreteSQL is a Go module.

```bash
go get github.com/tabeth/concretesql
```

## Usage

ConcreteSQL registers itself as a `database/sql` driver named `concretesql`.

### 1. Import the Driver

```go
import (
    "database/sql"
    _ "github.com/tabeth/concretesql/driver" // Registers the driver
)
```

### 2. Open a Connection

The DSN (Data Source Name) format is a standard SQLite URI, but you **MUST** specify `vfs=concretesql`.

```go
func main() {
    // Basic usage
    // "test.db" key in FDB, using custom VFS
    db, err := sql.Open("concretesql", "file:test.db?vfs=concretesql")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Test connection
    if err := db.Ping(); err != nil {
        log.Fatal(err)
    }
}
```

### 3. Advanced Configuration

#### Setting Page Size
You can optimize performance by setting a larger page size (default 4KB). This is done using standard SQLite `PRAGMA` commands immediately after creating the database.

```go
db, _ := sql.Open("concretesql", "file:mydb.db?vfs=concretesql")

// Set page size to 8KB (8192 bytes)
// This must be done BEFORE creating any tables.
_, err := db.Exec("PRAGMA page_size = 8192")
```

The adapter automatically detects this change and persists it in FDB metadata.

## Architecture

ConcreteSQL implements the `sqlite3vfs` interface.

1.  **PageStore**: Maps SQLite pages (Indices) to FDB Keys. It uses **Shadow Paging** (MVCC) to handle writes.
    -   Key Format: `(prefix, "data", PageID, VersionID)`
    -   Writes are buffered and committed transactionally to FDB on `Sync`.
    -   Old versions are kept temporarily to support snapshot isolation.

2.  **LockManager**: A distributed lock implementation using FDB.
    -   Maps SQLite locks (SHARED, RESERVED, PENDING, EXCLUSIVE) to FDB keys.
    -   Uses **Heartbeats** (2s interval) to keep locks alive.
    -   Locks expire after 5s if not renewed, allowing other clients to "steal" locks from crashed nodes.

3.  **Vacuum**:
    -   The `Vacuum(ctx)` method (exposed via internal API or potentially SQL trigger) cleans up obsolete MVCC versions.
    -   It runs in **chunks** to avoid FDB's 5-second transaction limit.

## Running Tests

Integration tests require a running FDB instance.

```bash
# Run all tests
go test ./...

# Run validaton of Production Features
go test -v ./driver -run TestTransactionModes
go test -v ./driver -run TestIntegration_PageSize_Pragma
go test -v ./store -run TestLockManager_LeaseStealing
```
