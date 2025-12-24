# ConcreteDB

ConcreteDB is a DynamoDB-compatible database implemented on top of FoundationDB. It allows you to run DynamoDB workloads locally or in your private cloud with the ACID guarantees and performance of FoundationDB.

## Features

### Core DynamoDB API Support
- **Key-Value Operations**: `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`.
- **Query & Scan**: Full support for `Query` (partition/sort key) and `Scan`, including Filter Expressions and Pagination.
- **Batch Operations**: `BatchGetItem` and `BatchWriteItem`.
- **Transactions**: ACID-compliance with `TransactGetItems` and `TransactWriteItems`.
- **Conditional Writes**: Full support for `ConditionExpression` (CAS).
- **Secondary Indexes**:
  - Global Secondary Indexes (GSI)
  - Local Secondary Indexes (LSI)
- **Table Management**: `CreateTable`, `DeleteTable`, `ListTables`, `DescribeTable`.

### Architecture
- **Storage Engine**: FoundationDB (FDB).
  - Data is stored in FDB directories (`/tables/{tableName}`).
  - Indexes use dedicated subspaces (`/tables/{tableName}/index/{indexName}`).
- **Atomicity**: leveraging FDB transactions (`transact`) for strong consistency.

## Getting Started

### Prerequisites
- Go 1.25+
- FoundationDB Server running locally.

### Installation
```bash
go mod tidy
go build -o concretedb .
```

### Running the Server
```bash
./concretedb
```
The server listens on port **8000** by default.

### Development
Running tests:
```bash
go test -v ./...
```

## Limitations
- **DynamoDB Streams**: Not yet implemented.
- **TTL**: Not yet implemented.
- **Throughput**: No provisioned throughput limits (On-Demand only).
- **Transaction Limit**: Max 25 items per transaction (matches DynamoDB limit).
