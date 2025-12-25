# ConcreteDB Backups and Restore

ConcreteDB implements an On-Demand Backup and Restore system designed to achieve functional parity with Amazon DynamoDB's backup features. This document outlines the architecture, implementation details, and data model used to achieve consistent, point-in-time backups on top of FoundationDB.

## 1. Feature Overview

The system supports the following core operations:
- **CreateBackup**: Initiates an asynchronous backup of a table.
- **RestoreTableFromBackup**: Restores a backup to a *new* table asynchronously.
- **ListBackups**: Lists available backups.
- **DescribeBackup**: Retrieves metadata for a specific backup.
- **DeleteBackup**: Deletes a backup and its underlying data.

## 2. Architecture

### Snapshot Isolation
To ensure **Snapshot Consistency** (data validity at the exact moment of request), ConcreteDB leverages FoundationDB's transactional capabilities.

1.  **Read Version**: When `CreateBackup` is called, the system acquires a specific FDB **Read Version** (snapshot version).
2.  **Consistency**: The asynchronous backup process uses this *same* Read Version for all subsequent read operations. This guarantees that any writes (PutItem, UpdateItem, DeleteItem) occurring *after* the backup request initiation are **not** included in the backup, providing a consistent point-in-time snapshot.

### Asynchronous Processing
Both Backup and Restore operations are potentially long-running. To prevent blocking the HTTP API:
- The initial request creates a metadata record with status `CREATING`.
- A background goroutine is spawned to perform the data copy.
- Upon completion, the metadata status is updated to `AVAILABLE` (for backups) or the table status to `ACTIVE` (for restores).

## 3. Data Model

Backup data is stored separately from active table data within FoundationDB to maximize isolation and management.

### Directory Structure
ConcreteDB uses the FDB Directory Layer to organize data:

- **Active Tables**: `tables/{TableName}`
- **Backup Metadata**: `backups/metadata`
- **Backup Data**: `backups/data/{BackupArn}`

### Storage Format
- **Metadata**: Stored as JSON-serialized `BackupDetails` structs in the `backups/metadata` subspace. Keyed by `BackupArn`.
- **Table Data**:
    - The source table's items are copied verbatim (key and value bytes) to the backup data directory.
    - **`_metadata` Key**: A special key `_metadata` is written to the backup subspace containing the JSON-serialized definition of the source table (KeySchema, AttributeDefinitions). This allows the table to be recreated correctly during restore.

## 4. Usage

The API is compatible with the DynamoDB JSON API.

**Example: Create Backup**
```json
// POST /
{
    "Target": "DynamoDB_20120810.CreateBackup",
    "TableName": "MyTable",
    "BackupName": "MyBackup"
}
```

**Example: Restore Table**
```json
// POST /
{
    "Target": "DynamoDB_20120810.RestoreTableFromBackup",
    "TargetTableName": "MyRestoredTable",
    "BackupArn": "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable/backup/..."
}
```

## 5. Limitations

- **Cross-Region Restore**: Currently simulated. Since FDB is treated as a single cluster in this implementation, "Cross-Region" restores would effectively be restores to different directory prefixes if multiple "regions" were emulated within the same FDB cluster.
- **Point-In-Time Recovery (PITR)**: This implementation covers *On-Demand* backups. Continuous backups (PITR) would require a different architecture (e.g., FDB Mutation Logs).
