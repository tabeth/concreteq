#!/bin/bash
set -e

# Build
echo "Building benchmark..."
go build -o ycsb-concrete .

# DSN
DSN="file:/tmp/bench_optimized.db?vfs=concretesql"

# Workload A - Load (Single thread to populate)
echo "Loading data (Workload A) - Single Thread..."
rm -f load_a_threaded.output
./ycsb-concrete load concretesql -P workloads/workloada -p threadcount=1 -p dsn="${DSN}" > load_a_threaded.output

# Workload C - Read Only (10 Threads - High Concurrency)
echo "Running Workload C (Read Only) - 10 Threads..."
rm -f run_c_threaded.output
./ycsb-concrete run concretesql -P workloads/workloadc -p threadcount=10 -p dsn="${DSN}" > run_c_threaded.output

# Workload B - Read Mostly (95/5) - 4 Threads
echo "Running Workload B (Read Mostly) - 4 Threads..."
rm -f run_b_threaded.output
./ycsb-concrete run concretesql -P workloads/workloadb -p threadcount=4 -p dsn="${DSN}" > run_b_threaded.output

# Workload D - Read/Insert (95/5) - 4 Threads
echo "Running Workload D (Read Latest) - 4 Threads..."
rm -f run_d_threaded.output
./ycsb-concrete run concretesql -P workloads/workloadd -p threadcount=4 -p dsn="${DSN}" > run_d_threaded.output

# Workload A - Update Heavy (50/50) - 2 Threads (High Contention)
echo "Running Workload A (Update Heavy) - 2 Threads..."
rm -f run_a_threaded.output
./ycsb-concrete run concretesql -P workloads/workloada -p threadcount=2 -p dsn="${DSN}" > run_a_threaded.output

# Workload F - RMW (50/50) - 2 Threads
echo "Running Workload F (RMW) - 2 Threads..."
rm -f run_f_threaded.output
./ycsb-concrete run concretesql -P workloads/workloadf -p threadcount=2 -p dsn="${DSN}" > run_f_threaded.output

# Workload E - Scan (95/5) - 4 Threads
echo "Running Workload E (Scan) - 4 Threads..."
rm -f run_e_threaded.output
./ycsb-concrete run concretesql -P workloads/workloade -p threadcount=4 -p dsn="${DSN}" > run_e_threaded.output

echo "Done. Results in *_threaded.output"
