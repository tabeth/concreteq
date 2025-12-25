#!/bin/bash
set -e

# Build
echo "Building benchmark..."
go build -o ycsb-concrete .

# Setup DB path (uses FDB VFS)
DB="file:/tmp/bench.db?vfs=concretesql"

# Common properties
PROPS="-p dsn=$DB -p recordcount=1000 -p operationcount=1000 -p threadcount=1"

# Load (Workload A Load)
echo "Loading data (Workload A)..."
./ycsb-concrete load concretesql -P workloads/workloada $PROPS > load_a.output

# Run Workload A
echo "Running Workload A..."
./ycsb-concrete run concretesql -P workloads/workloada $PROPS > run_a.output

# Run Workload B
echo "Running Workload B..."
./ycsb-concrete run concretesql -P workloads/workloadb $PROPS > run_b.output

# Run Workload C
echo "Running Workload C..."
./ycsb-concrete run concretesql -P workloads/workloadc $PROPS > run_c.output

# Run Workload D
echo "Running Workload D..."
./ycsb-concrete run concretesql -P workloads/workloadd $PROPS > run_d.output

# Run Workload E
echo "Running Workload E..."
./ycsb-concrete run concretesql -P workloads/workloade $PROPS > run_e.output

# Run Workload F
echo "Running Workload F..."
./ycsb-concrete run concretesql -P workloads/workloadf $PROPS > run_f.output

echo "Done. Results in *.output"
