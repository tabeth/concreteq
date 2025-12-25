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

### Workload A (Update Heavy)
- **Distribution**: Zipfian
- **Mix**: 50/50 Read/Update
- **Performance**: 73.1 OPS
- **Analysis**: Update operations involve locking and potentially more complex write paths, resulting in lower throughput compared to read-heavy workloads.

### Workload B (Read Mostly)
- **Distribution**: Zipfian
- **Mix**: 95/5 Read/Update
- **Performance**: 179.2 OPS
- **Analysis**: Significantly higher throughput due to the dominance of read operations which are faster.

### Workload C (Read Only)
- **Distribution**: Zipfian
- **Mix**: 100% Read
- **Performance**: 197.6 OPS
- **Analysis**: Highest throughput as expected for a pure read workload.

### Workload D (Read Latest)
- **Distribution**: Latest
- **Mix**: 95/5 Read/Insert
- **Performance**: 211.0 OPS
- **Analysis**: Performs very well, benefiting from locality or efficient insert handling.

### Workload E (Short Ranges)
- **Distribution**: Zipfian
- **Mix**: 95/5 Scan/Insert
- **Performance**: 185.5 OPS
- **Analysis**: Scans are relatively efficient in ConcreteSQL given the result. The scan length distribution was uniform (up to 100 records).

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

