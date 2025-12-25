package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_BackupRestore(t *testing.T) {
	tableName := "backup-test-table"
	restoreTableName := "backup-restore-table"
	s := setupTestStore(t, tableName)
	ctx := context.Background()

	// Helper
	createTable := func(t *testing.T, s *FoundationDBStore, name string) {
		err := s.CreateTable(ctx, &models.Table{
			TableName:            name,
			KeySchema:            []models.KeySchemaElement{{AttributeName: "PK", KeyType: "HASH"}},
			AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "PK", AttributeType: "S"}},
			Status:               models.StatusActive,
		})
		if err != nil {
			t.Fatalf("Failed to create table %s: %v", name, err)
		}
	}

	// 1. Create Source Table
	createTable(t, s, tableName)

	// 2. Put Item
	pkVal := "item1"
	_, err := s.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"PK":  {S: &pkVal},
		"Val": {S: stringPtr("data")},
	}, "", nil, nil, "")
	assert.NoError(t, err)

	// 2a. Verify Source Item
	sourceItem, _ := s.GetItem(ctx, tableName, map[string]models.AttributeValue{
		"PK": {S: &pkVal},
	}, "", nil, true)
	if sourceItem == nil || sourceItem["Val"].S == nil {
		t.Fatalf("Source item missing Val: %v", sourceItem)
	}

	// 3. Create Backup
	backupReq := &models.CreateBackupRequest{
		TableName:  tableName,
		BackupName: "my-backup-1",
	}
	backupDesc, err := s.CreateBackup(ctx, backupReq)
	assert.NoError(t, err)
	assert.Equal(t, "CREATING", backupDesc.BackupStatus)
	backupArn := backupDesc.BackupArn

	// 4. Poll for Backup Completion (AVAILABLE)
	assert.Eventually(t, func() bool {
		desc, err := s.DescribeBackup(ctx, backupArn)
		if err != nil {
			return false
		}
		return desc.BackupDetails.BackupStatus == "AVAILABLE"
	}, 5*time.Second, 100*time.Millisecond, "Backup should become AVAILABLE")

	// 5. Restore Table
	restoreReq := &models.RestoreTableFromBackupRequest{
		TargetTableName: restoreTableName,
		BackupArn:       backupArn,
	}
	restoreDesc, err := s.RestoreTableFromBackup(ctx, restoreReq)
	assert.NoError(t, err)
	assert.Equal(t, "CREATING", restoreDesc.TableStatus)

	// 6. Poll for Restore Completion (ACTIVE)
	assert.Eventually(t, func() bool {
		table, err := s.GetTable(ctx, restoreTableName)
		if err != nil {
			return false
		}
		return table.Status == models.StatusActive
	}, 5*time.Second, 100*time.Millisecond, "Restore table should become ACTIVE")

	// 7. Verify Data
	item, err := s.GetItem(ctx, restoreTableName, map[string]models.AttributeValue{
		"PK": {S: &pkVal},
	}, "", nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, item)

	if val, ok := item["Val"]; ok {
		if val.S != nil {
			assert.Equal(t, "data", *val.S)
		} else {
			t.Errorf("Item Val.S is nil. Item: %+v", item)
		}
	} else {
		t.Errorf("Item Val key missing. Item: %+v", item)
	}
}

func TestFoundationDBStore_ListBackups(t *testing.T) {
	tableName := "backup-list-test"
	s := setupTestStore(t, tableName)
	ctx := context.Background()

	createTable := func(t *testing.T, s *FoundationDBStore, name string) {
		s.CreateTable(ctx, &models.Table{
			TableName:            name,
			KeySchema:            []models.KeySchemaElement{{AttributeName: "PK", KeyType: "HASH"}},
			AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "PK", AttributeType: "S"}},
			Status:               models.StatusActive,
		})
	}
	createTable(t, s, tableName)

	// Create 3 Backups
	// The CreateBackup includes async work but ListBackups should see them immediately (CREATING status)
	arns := []string{}
	for i := 0; i < 3; i++ {
		req := &models.CreateBackupRequest{
			TableName:  tableName,
			BackupName: fmt.Sprintf("backup-%d", i),
		}
		desc, err := s.CreateBackup(ctx, req)
		assert.NoError(t, err)
		arns = append(arns, desc.BackupArn)
	}

	// List All
	listReq := &models.ListBackupsRequest{
		TableName: tableName,
	}
	summaries, _, err := s.ListBackups(ctx, listReq)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(summaries), 3)

	// Verify filtering by name/arn implicitly tested by looking for our backups
	found := 0
	for _, summ := range summaries {
		for _, arn := range arns {
			if summ.BackupArn == arn {
				found++
			}
		}
	}
	assert.Equal(t, 3, found)

	// Pagination Test
	listReq.Limit = 1
	pagedSummaries, lastKey, err := s.ListBackups(ctx, listReq)
	assert.NoError(t, err)
	assert.Len(t, pagedSummaries, 1)
	assert.NotEmpty(t, lastKey)

	listReq.Limit = 0
	listReq.ExclusiveStartBackupArn = lastKey
	restSummaries, _, err := s.ListBackups(ctx, listReq)
	assert.NoError(t, err)
	// Should contain the other 2 (or more if parallel tests run)
	// At least 2 more
	assert.GreaterOrEqual(t, len(restSummaries), 2)
}

func TestFoundationDBStore_DeleteBackup(t *testing.T) {
	tableName := "backup-del-test"
	s := setupTestStore(t, tableName)
	ctx := context.Background()

	createTable := func(t *testing.T, s *FoundationDBStore, name string) {
		s.CreateTable(ctx, &models.Table{
			TableName:            name,
			KeySchema:            []models.KeySchemaElement{{AttributeName: "PK", KeyType: "HASH"}},
			AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "PK", AttributeType: "S"}},
			Status:               models.StatusActive,
		})
	}
	createTable(t, s, tableName)

	req := &models.CreateBackupRequest{
		TableName:  tableName,
		BackupName: "backup-del",
	}
	desc, _ := s.CreateBackup(ctx, req)
	backupArn := desc.BackupArn

	// Poll until available (optional but cleaner)
	assert.Eventually(t, func() bool {
		d, _ := s.DescribeBackup(ctx, backupArn)
		return d.BackupDetails.BackupStatus == "AVAILABLE"
	}, 5*time.Second, 100*time.Millisecond)

	// Delete
	delDesc, err := s.DeleteBackup(ctx, backupArn)
	assert.NoError(t, err)
	assert.Equal(t, "DELETED", delDesc.BackupDetails.BackupStatus)

	// Verify gone from List? Or status DELETED?
	// GetBackup should fail or return DELETED?
	// AWS: DeleteBackup deletes it. DescribeBackup might throw Not Found.
	// Our implementation:
	// tr.Clear(metaDir.Pack(tuple.Tuple{backupArn}))
	// So DescribeBackup should return "Backup not found" (ResourceNotFoundException)

	_, err = s.DescribeBackup(ctx, backupArn)
	assert.ErrorContains(t, err, "Backup not found")
}

func TestFoundationDBStore_Backup_Errors(t *testing.T) {
	s := setupTestStore(t, "err")
	ctx := context.Background()

	createTable := func(t *testing.T, s *FoundationDBStore, name string) {
		s.CreateTable(ctx, &models.Table{
			TableName:            name,
			KeySchema:            []models.KeySchemaElement{{AttributeName: "PK", KeyType: "HASH"}},
			AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "PK", AttributeType: "S"}},
			Status:               models.StatusActive,
		})
	}

	// CreateBackup - Table not found
	_, err := s.CreateBackup(ctx, &models.CreateBackupRequest{TableName: "non-existent"})
	assert.ErrorContains(t, err, "Table not found")

	// Restore - Table already exists
	createTable(t, s, "exist-restore")
	_, err = s.RestoreTableFromBackup(ctx, &models.RestoreTableFromBackupRequest{
		TargetTableName: "exist-restore",
		BackupArn:       "some-arn",
	})
	assert.ErrorContains(t, err, "Table already exists")

	// Restore - Backup not found
	_, err = s.RestoreTableFromBackup(ctx, &models.RestoreTableFromBackupRequest{
		TargetTableName: "new-restore",
		BackupArn:       "fake-arn",
	})
	assert.ErrorContains(t, err, "Backup not found")
}
