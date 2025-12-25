#!/bin/bash

# Build the benchmark tool
go build -o ycsb-concrete main.go db.go

# Directory for results
mkdir -p results

# Workload directory
WORKLOADS="../../../concretesql/benchmark/ycsb/workloads"

# Parameters
RECORDCOUNT=1000
OPERATIONCOUNT=1000
THREADCOUNT=1

echo "Starting YCSB benchmark for ConcreteDB..."

# Load Data (Workload A loads data)
echo "Loading data (Workload A)..."
./ycsb-concrete load concretedb -P $WORKLOADS/workloada -p recordcount=$RECORDCOUNT -p threadcount=$THREADCOUNT > results/load_a.output

# Run Workload A
echo "Running Workload A..."
./ycsb-concrete run concretedb -P $WORKLOADS/workloada -p recordcount=$RECORDCOUNT -p operationcount=$OPERATIONCOUNT -p threadcount=$THREADCOUNT > results/run_a.output

# Run Workload B
echo "Running Workload B..."
./ycsb-concrete run concretedb -P $WORKLOADS/workloadb -p recordcount=$RECORDCOUNT -p operationcount=$OPERATIONCOUNT -p threadcount=$THREADCOUNT > results/run_b.output

# Run Workload C
echo "Running Workload C..."
./ycsb-concrete run concretedb -P $WORKLOADS/workloadc -p recordcount=$RECORDCOUNT -p operationcount=$OPERATIONCOUNT -p threadcount=$THREADCOUNT > results/run_c.output

# Run Workload D
echo "Running Workload D..."
./ycsb-concrete run concretedb -P $WORKLOADS/workloadd -p recordcount=$RECORDCOUNT -p operationcount=$OPERATIONCOUNT -p threadcount=$THREADCOUNT > results/run_d.output

# Run Workload E (Scan)
echo "Running Workload E..."
./ycsb-concrete run concretedb -P $WORKLOADS/workloade -p recordcount=$RECORDCOUNT -p operationcount=$OPERATIONCOUNT -p threadcount=$THREADCOUNT > results/run_e.output

# Run Workload F
echo "Running Workload F..."
./ycsb-concrete run concretedb -P $WORKLOADS/workloadf -p recordcount=$RECORDCOUNT -p operationcount=$OPERATIONCOUNT -p threadcount=$THREADCOUNT > results/run_f.output

echo "Benchmark complete. Results in results/"
