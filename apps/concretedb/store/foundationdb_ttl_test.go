package store

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

// TestTTL_BackgroundCleanup verifies that the TTL worker correctly identifies and deletes expired items.
func TestTTL_BackgroundCleanup(t *testing.T) {
	tableName := "test-ttl-cleanup"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	err := store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
		TimeToLiveDescription: &models.TimeToLiveDescription{
			TimeToLiveStatus: "ENABLED",
			AttributeName:    "ttl",
		},
	})
	assert.NoError(t, err)

	now := time.Now().Unix()
	past := now - 100
	future := now + 1000

	items := []struct {
		pk  string
		ttl int64
	}{
		{"expired-1", past},
		{"expired-2", past - 10},
		{"valid-1", future},
		{"valid-no-ttl", 0}, // No TTL attribute
	}

	// 1. Insert Items
	for _, item := range items {
		av := map[string]models.AttributeValue{
			"pk": {S: strPtr(item.pk)},
		}
		if item.ttl != 0 {
			ts := fmt.Sprintf("%d", item.ttl)
			av["ttl"] = models.AttributeValue{N: &ts}
		}
		_, err := store.PutItem(ctx, tableName, av, "", nil, nil, "")
		assert.NoError(t, err)
	}

	// 2. Verify all exist
	scanRes, _, err := store.Scan(ctx, tableName, "", "", nil, nil, 0, nil, true)
	assert.NoError(t, err)
	assert.Len(t, scanRes, 4)

	// store is already *FoundationDBStore, so we can call runTTLPass directly
	store.runTTLPass(ctx)

	// 4. Verify Expired Items Deleted
	scanRes, _, err = store.Scan(ctx, tableName, "", "", nil, nil, 0, nil, true)
	assert.NoError(t, err)

	// Expect 2 items remaining (valid-1, valid-no-ttl)
	assert.Len(t, scanRes, 2)

	for _, item := range scanRes {
		pk := *item["pk"].S
		if pk == "expired-1" || pk == "expired-2" {
			t.Errorf("Item %s should have been deleted", pk)
		}
	}
}

func TestTTL_UpdateRespectsIndex(t *testing.T) {
	tableName := "test-ttl-update"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	err := store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		TimeToLiveDescription: &models.TimeToLiveDescription{
			TimeToLiveStatus: "ENABLED",
			AttributeName:    "ttl",
		},
	})
	assert.NoError(t, err)

	pk := "item-1"

	// 1. Insert with FUTURE TTL
	now := time.Now().Unix()
	future := fmt.Sprintf("%d", now+1000)
	_, err = store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"pk":  {S: &pk},
		"ttl": {N: &future},
	}, "", nil, nil, "")
	assert.NoError(t, err)

	// 2. Update to PAST TTL
	past := fmt.Sprintf("%d", now-100)
	_, err = store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"pk":  {S: &pk},
		"ttl": {N: &past},
	}, "", nil, nil, "")
	assert.NoError(t, err)

	// 3. Run Cleanup
	store.runTTLPass(ctx)

	// 4. Verify Deleted
	item, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, true)
	assert.NoError(t, err)
	assert.Nil(t, item)
}

func TestTTL_DisablePreventsCleanup(t *testing.T) {
	// ... logic to verify if disabled, runTTLPass skips it ...
	tableName := "test-ttl-disabled"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	err := store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		TimeToLiveDescription: &models.TimeToLiveDescription{
			TimeToLiveStatus: "DISABLED", // Start disabled
			AttributeName:    "ttl",
		},
	})
	assert.NoError(t, err)

	pk := "expired-but-safe"
	past := fmt.Sprintf("%d", time.Now().Unix()-100)
	_, err = store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"pk":  {S: &pk},
		"ttl": {N: &past},
	}, "", nil, nil, "")
	assert.NoError(t, err)

	store.runTTLPass(ctx)

	// Verify NOT Deleted
	item, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, true)
	assert.NoError(t, err)
	assert.NotNil(t, item)
}

func TestTTL_NumericKeys_And_Batching(t *testing.T) {
	tableName := "test-ttl-numeric-batch"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	err := store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			// Define as N
			{AttributeName: "id", AttributeType: "N"},
		},
		TimeToLiveDescription: &models.TimeToLiveDescription{
			TimeToLiveStatus: "ENABLED",
			AttributeName:    "ttl",
		},
	})
	assert.NoError(t, err)

	now := time.Now().Unix()
	past := now - 100

	// Insert 150 items (batch size is 100)
	for i := 0; i < 150; i++ {
		id := fmt.Sprintf("%d", i)
		ts := fmt.Sprintf("%d", past)
		_, err := store.PutItem(ctx, tableName, map[string]models.AttributeValue{
			"id":  {N: &id},
			"ttl": {N: &ts},
		}, "", nil, nil, "")
		assert.NoError(t, err)
	}

	// Verify count
	scanRes, _, err := store.Scan(ctx, tableName, "", "", nil, nil, 0, nil, true)
	assert.NoError(t, err)
	assert.Len(t, scanRes, 150)

	// Run Pass
	store.runTTLPass(ctx)

	// Verify All Deleted
	scanRes, _, err = store.Scan(ctx, tableName, "", "", nil, nil, 0, nil, true)
	assert.NoError(t, err)
	assert.Len(t, scanRes, 0)
}

func TestTTL_BinaryKeys(t *testing.T) {
	tableName := "test-ttl-binary"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	err := store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "id", AttributeType: "B"},
		},
		TimeToLiveDescription: &models.TimeToLiveDescription{
			TimeToLiveStatus: "ENABLED",
			AttributeName:    "ttl",
		},
	})
	assert.NoError(t, err)

	now := time.Now().Unix()
	past := fmt.Sprintf("%d", now-100)

	idBytes := []byte{0x01, 0x02, 0x03}
	idStr := base64.StdEncoding.EncodeToString(idBytes)

	_, err = store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"id":  {B: &idStr},
		"ttl": {N: &past},
	}, "", nil, nil, "")
	assert.NoError(t, err)

	store.runTTLPass(ctx)

	// Verify Deleted
	scanRes, _, err := store.Scan(ctx, tableName, "", "", nil, nil, 0, nil, true)
	assert.NoError(t, err)
	assert.Len(t, scanRes, 0)
}
