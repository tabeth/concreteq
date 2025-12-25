# ConcreteSQL Benchmark Results

This document summarizes the performance benchmark of ConcreteSQL using the standard YCSB workloads A-F.

## Summary

| Workload | Description | Operations | Duration (s) | OPS |
| :--- | :--- | :--- | :--- | :--- |
| **A** | 50% Read, 50% Update | 1000 | 13.68 | 73.1 |
| **B** | 95% Read, 5% Update | 1000 | 5.58 | 179.2 |
| **C** | 100% Read | 1000 | 5.06 | 197.6 |
| **D** | 95% Read, 5% Insert (Latest) | 1000 | 4.74 | 211.0 |
| **E** | 95% Scan, 5% Insert | 1000/1000 | 5.39 | 185.5 |
| **F** | 50% Read, 50% RMW | 1000 | 15.84 | 63.1 |

*Note: OPS (Operations Per Second) based on total execution time including any overhead.*

## Workload Details

### Write-Heavy Workloads (A, B, D, F)
- **Observation**:
    - High contention on small datasets (1000 records) with Update/Insert operations leads to frequent "Version mismatch" errors with Optimistic Concurrency Control.
    - Pessimistic Locking (via `_busy_timeout`) solves the "Version mismatch" errors by waiting for locks.
    - However, this introduces significant latency (timeouts) and reduces throughput (OPS) drastically under high contention (e.g., Workload D dropped to ~8 OPS with 4 threads).
    - **Conclusion**: ConcreteSQL's current locking implementation ensures correctness but requires careful tuning of timeouts and concurrency levels for write-heavy workloads. For high-throughput writes, larger datasets or sharding (to reduce collision probability) is recommended.

## 4. Workload Details

### Workload A (Update Heavy)
- **Mix**: 50% Read, 50% Update
- **Goal**: Test intensive write contention.
- **Results (Multi-threaded with Pessimistic Locking)**:
    - **Threads**: 4
    - **Outcome**: Successful execution without "Version mismatch", but with high latency due to lock waits. Proper serialization confirmed.

### Workload B (Read Mostly)
- **Mix**: 95% Read, 5% Update
- **Goal**: Test read performance with light write pressure.
- **Results**:
    - **Threads**: 4
    - **Outcome**: Balanced performance. Reads remain fast, while occasional updates wait for locks.

### Workload C (Read Only)
- **Mix**: 100% Read
- **Goal**: Test pure read scalability.
- **Results**:
    - **Threads**: 10
    - **Outcome**: Excellent scalability (~694 OPS vs 197 OPS single-threaded). No contention.

### Workload D (Read Latest)
- **Mix**: 95% Read, 5% Insert
- **Goal**: Test insertion patterns.
- **Results**:
    - **Threads**: 4
    - **Outcome**: Functional with pessimistic locking, though inserts on a small dataset/file cause serialization bottlenecks (context deadline exceeded logs observed).

### Workload E (Scan)
- **Mix**: 95% Scan, 5% Insert
- **Goal**: Test range query performance.

### Workload F (Read-Modify-Write)
- **Distribution**: Zipfian
- **Mix**: 50/50 Read/Read-Modify-Write
- **Performance**: 63.1 OPS
- **Analysis**: Lowest throughput due to the atomicity requirement of Read-Modify-Write, which likely involves transaction retries or stricter locking.

## Setup
- **Client**: `go-ycsb` (custom driver)
- **Database**: ConcreteSQL (FDB backend)
- **Record Count**: 1000
- **Operation Count**: 1000 per workload
- **Environment**: Local Workspace

## Multi-threaded Benchmark Results (Workload C - Read Only)

To evaluate the scalability of ConcreteSQL, we ran Workload C (100% Read) with **10 concurrent threads**.

| Workload | Threads | Operations | Time (s) | OPS | Avg Latency (us) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Workload C (Read Only) | 10 | 1000 | 1.44 | **694.4** | ~14,000 |

### Observations

- **Read Scalability:** Running with 10 threads significantly improved throughput compared to the single-threaded run (~180 OPS -> ~694 OPS), demonstrating that ConcreteSQL scales well for read-heavy workloads.
- **Write Contention:** For write-heavy workloads (A, F) on this small dataset (1,000 records), we observed significant contention (Optimistic Concurrency Control conflicts leading to "Version mismatch") when running with high concurrency (10 threads). This is expected for high-concurrency updates on a small keyspace without application-level retries. Partitioning data or using larger datasets would mitigate this.

