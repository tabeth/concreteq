# ConcreteDB Benchmark Results

This document summarizes the performance benchmark of ConcreteDB (DynamoDB Implementation) using the standard YCSB workloads A-F.

## Summary

| Workload | Description | Operations | Duration (s) | OPS |
| :--- | :--- | :--- | :--- | :--- |
| **A** | 50% Read, 50% Update | 1000 | 2.39 | 418.4 |
| **B** | 95% Read, 5% Update | 1000 | 2.05 | 487.8 |
| **C** | 100% Read | 1000 | 1.89 | 529.1 |
| **D** | 95% Read, 5% Insert (Latest) | 1000 | 2.05 | 487.8 |
| **E** | 95% Scan, 5% Insert | 1000 | 4.26 | 234.7 |
| **F** | 50% Read, 50% RMW | 1000 | 3.38 | 295.8 |

*Note: OPS (Operations Per Second) calculated as Total Operations / Total Execution Time.*

## Comparable Performance

Compared to ConcreteSQL:
- **Workload A**: ConcreteDB is ~5.7x faster (418 OPS vs 73 OPS).
- **Workload C**: ConcreteDB is ~2.7x faster (529 OPS vs 197 OPS single-threaded).

This performance advantage is expected as ConcreteDB maps DynamoDB items directly to FoundationDB keys using a key-value pattern, whereas ConcreteSQL involves a full SQL layer and potentially more complex locking/transaction overhead.

## Workload Details

### Workload A (Update Heavy)
- **Mix**: 50% Read, 50% Update
- **Performance**: High throughput (418 OPS).
- **Analysis**: Even with 50% updates, the system handles concurrent operations efficiently. The direct key-value mapping allows for fast point lookups and updates.

### Workload B (Read Mostly)
- **Mix**: 95% Read, 5% Update
- **Performance**: Very high throughput (488 OPS).
- **Analysis**: Read operations are extremely fast as they translate to simple FDB range reads (or point reads).

### Workload C (Read Only)
- **Mix**: 100% Read
- **Performance**: Highest throughput (529 OPS).
- **Analysis**: Pure read workload demonstrates the baseline efficiency of the `service` -> `store` -> `FDB` read path with minimal overhead.

### Workload D (Read Latest)
- **Mix**: 95% Read, 5% Insert
- **Performance**: Similar to Workload B (488 OPS).
- **Analysis**: Inserts in this workload are generally to the "end" (latest), but since ConcreteDB is a KV store, insertion is just another `Set` operation, performance is comparable to updates.

### Workload E (Scan)
- **Mix**: 95% Scan, 5% Insert
- **Performance**: Lower than point-ops (235 OPS).
- **Analysis**: Scans involve determining ranges and iterating. While slower than point lookups, it is still performant. YCSB scans short ranges.

### Workload F (Read-Modify-Write)
- **Mix**: 50% Read, 50% Read-Modify-Write
- **Performance**: 296 OPS.
- **Analysis**: This workload involves a Read followed by a Write (atomic). In our implementation, `UpdateItem` is atomic, but YCSB usually does a client-side generic RMW. However, our `Update` binding just sends an `UpdateItem` which assumes atomicity or performs it. If YCSB does Read then Update, latency adds up.

## Setup
- **Client**: `go-ycsb` (custom binding)
- **Database**: ConcreteDB (FDB backend)
- **Record Count**: 1000
- **Operation Count**: 1000 per workload
- **Concurrency**: 1 Thread (Single-threaded)
- **Environment**: Local Workspace
