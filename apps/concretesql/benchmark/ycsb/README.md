# ConcreteSQL YCSB Benchmark

This directory contains the YCSB benchmark setup for ConcreteSQL.

## Components

- **`main.go`**: Custom YCSB entry point that registers the ConcreteSQL driver.
- **`db.go`**: The ConcreteSQL driver implementation for YCSB.
- **`workloads/`**: YCSB workload configuration files (A-F).
- **`run_all.sh`**: Helper script to build and run all workloads.
- **`BENCHMARK.md`**: Recorded benchmark results.

## Running Benchmarks

To run all workloads:

```bash
./run_all.sh
```

To run a specific workload with custom thread count (e.g., 10 threads):

```bash
./ycsb-concrete run concretesql -P workloads/workloada -p threadcount=10
```

## Local `go-ycsb` Library

The project uses a local copy of the `go-ycsb` library located in `../../../../libs/go-ycsb`.

**Rationale:**
The upstream `go-ycsb` (v1.0.1) generic `Core` workload implementation does not instrument `Read`, `Insert`, `Update`, and `Scan` operations with the `measurement` package. As a result, running the benchmark produced standard output but zero performance metrics (OPS, latency). 

We patched the local copy of `libs/go-ycsb/pkg/workload/core.go` to explicitly call `measurement.Measure` for these operations. This local copy is referenced via a `replace` directive in `go.work`.
