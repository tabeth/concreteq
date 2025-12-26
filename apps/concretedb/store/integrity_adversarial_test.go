package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestIntegrity_NumericSorting(t *testing.T) {
	tableName := "test-numeric-sorting"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "sk", AttributeType: "N"},
		},
	}
	assert.NoError(t, store.CreateTable(ctx, table))

	pk := "user1"
	// Insert out of order
	nums := []string{"10", "2", "1", "100", "20"}
	for _, n := range nums {
		item := map[string]models.AttributeValue{
			"pk": {S: &pk},
			"sk": {N: strPtr(n)},
		}
		_, err := store.PutItem(ctx, tableName, item, "", nil, nil, "")
		assert.NoError(t, err)
	}

	// Query and verify order
	items, _, err := store.Query(ctx, tableName, "", "pk = :p", "", "", nil, map[string]models.AttributeValue{":p": {S: &pk}}, 0, nil, false)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(items))

	expectedOrder := []string{"1", "2", "10", "20", "100"}
	for i, item := range items {
		assert.Equal(t, expectedOrder[i], *item["sk"].N)
	}
}

func TestIntegrity_SizeLimits(t *testing.T) {
	tableName := "test-size-limits"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}
	assert.NoError(t, store.CreateTable(ctx, table))

	// 1. Just under FDB limit (~90KB)
	largeVal := strings.Repeat("A", 90*1024)
	item1 := map[string]models.AttributeValue{
		"id":   {S: strPtr("1")},
		"data": {S: &largeVal},
	}
	_, err := store.PutItem(ctx, tableName, item1, "", nil, nil, "")
	assert.NoError(t, err)

	// 2. Over FDB limit (>100KB)
	overFdbVal := strings.Repeat("B", 110*1024)
	item2 := map[string]models.AttributeValue{
		"id":   {S: strPtr("2")},
		"data": {S: &overFdbVal},
	}
	_, err = store.PutItem(ctx, tableName, item2, "", nil, nil, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "FoundationDB value limit")

	// 3. Over DynamoDB limit (400KB) - though our FDB check will catch it first
	// To test specifically the 400KB check, we'd need many small attributes or a system that supports large FDB values.
	// But since 100KB < 400KB, the FDB check is the effective one.
}

func TestIntegrity_TransactWriteIdempotency(t *testing.T) {
	tableName := "test-transact-idempotency"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	assert.NoError(t, store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}))

	token := "unique-token-123"
	items := []models.TransactWriteItem{
		{
			Put: &models.PutItemRequest{
				TableName: tableName,
				Item:      map[string]models.AttributeValue{"id": {S: strPtr("1")}, "val": {S: strPtr("a")}},
			},
		},
	}

	// First call
	err := store.TransactWriteItems(ctx, items, token)
	assert.NoError(t, err)

	// Verify write
	item, _ := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"id": {S: strPtr("1")}}, "", nil, true)
	assert.Equal(t, "a", *item["val"].S)

	// Second call with same token but DIFFERENT data (to prove it's idempotent and doesn't re-run)
	items2 := []models.TransactWriteItem{
		{
			Put: &models.PutItemRequest{
				TableName: tableName,
				Item:      map[string]models.AttributeValue{"id": {S: strPtr("1")}, "val": {S: strPtr("b")}},
			},
		},
	}
	err = store.TransactWriteItems(ctx, items2, token)
	assert.NoError(t, err)

	// Verify data remains "a"
	item, _ = store.GetItem(ctx, tableName, map[string]models.AttributeValue{"id": {S: strPtr("1")}}, "", nil, true)
	assert.Equal(t, "a", *item["val"].S)
}

func TestIntegrity_PITR_LargeHistoryRestore(t *testing.T) {
	tableName := "test-pitr-large"
	targetName := "test-pitr-large-restored"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	assert.NoError(t, store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}))

	// Enable PITR
	_, err := store.UpdateContinuousBackups(ctx, &models.UpdateContinuousBackupsRequest{
		TableName:                        tableName,
		PointInTimeRecoverySpecification: models.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: true},
	})
	assert.NoError(t, err)

	// Insert many versions of the same item to exceed pitrBatchSize (default 1000)
	// We'll temporarily lower pitrBatchSize for the test if possible,
	// or just write 1100 records.

	// Since pitrBatchSize is a package variable, we can't easily change it from here without exporting it.
	// Let's check foundationdb_pitr.go to see if it's exported. It was var pitrBatchSize = 1000.
	// I'll write 1100 records.

	id := "item1"
	for i := 0; i < 1100; i++ {
		item := map[string]models.AttributeValue{
			"id":  {S: &id},
			"val": {N: strPtr(fmt.Sprintf("%d", i))},
		}
		_, err := store.PutItem(ctx, tableName, item, "", nil, nil, "")
		if err != nil {
			t.Fatalf("Failed at %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond) // Ensure timestamps move forward
	restoreTime := float64(time.Now().UnixNano()) / 1e9

	// Restore
	_, err = store.RestoreTableToPointInTime(ctx, &models.RestoreTableToPointInTimeRequest{
		SourceTableName: tableName,
		TargetTableName: targetName,
		RestoreDateTime: restoreTime,
	})
	assert.NoError(t, err)

	// Verify restored item
	item, err := store.GetItem(ctx, targetName, map[string]models.AttributeValue{"id": {S: &id}}, "", nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, item)
	assert.Equal(t, "1099", *item["val"].N)
}
