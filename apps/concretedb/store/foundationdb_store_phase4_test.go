package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_Pagination(t *testing.T) {
	store := setupTestStore(t, "test-pagination")
	tableName := "test-pagination-" + time.Now().Format("20060102150405")
	ctx := context.Background()

	// 1. Create Table
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "sk", KeyType: "RANGE"}},
	})

	// 2. Populate Data (10 items)
	for i := 1; i <= 10; i++ {
		pk := "A"
		sk := fmt.Sprintf("sort-%02d", i)
		store.PutItem(ctx, tableName, map[string]models.AttributeValue{
			"pk":  {S: &pk},
			"sk":  {S: &sk},
			"val": {S: &sk},
		}, "", nil, nil, "NONE")
	}

	// 3. Query Pagination (Limit 2)
	// Query all 10 items in pages of 2
	var allItems []map[string]models.AttributeValue
	var lastKey map[string]models.AttributeValue
	pages := 0

	for {
		items, lek, err := store.Query(ctx, tableName, "", "pk = :v", "", "", nil, map[string]models.AttributeValue{":v": {S: stringPtr("A")}}, 2, lastKey, true)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		allItems = append(allItems, items...)
		pages++
		lastKey = lek
		if len(lek) == 0 {
			break
		}
	}

	if len(allItems) != 10 {
		t.Errorf("Expected 10 items, got %d", len(allItems))
	}
	if pages < 5 {
		t.Errorf("Expected at least 5 pages, got %d", pages)
	}

	// 4. Scan Pagination (Limit 3)
	allItems = nil
	lastKey = nil
	pages = 0
	for {
		items, lek, err := store.Scan(ctx, tableName, "", "", nil, nil, 3, lastKey, true)
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		allItems = append(allItems, items...)
		pages++
		lastKey = lek
		if len(lek) == 0 {
			break
		}
	}
	if len(allItems) != 10 {
		t.Errorf("Expected 10 items, got %d", len(allItems))
	}
	if pages < 4 {
		t.Errorf("Expected at least 4 pages, got %d", pages)
	}
}

func TestFoundationDBStore_DeleteItem_Condition(t *testing.T) {
	store := setupTestStore(t, "test-del-cond")
	tableName := "test-del-cond-" + time.Now().Format("20060102150405")
	ctx := context.Background()

	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	})

	pk := "item1"
	val := "v1"
	store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"pk":  {S: &pk},
		"val": {S: &val},
	}, "", nil, nil, "NONE")

	// 1. Condition Failed (Expect val = v2)
	old, err := store.DeleteItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, "val = :v", nil, map[string]models.AttributeValue{":v": {S: stringPtr("v2")}}, "ALL_OLD")
	if err == nil {
		t.Error("Expected error, got nil")
	}
	// Verify item still exists
	item, _ := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, true)
	if item == nil {
		t.Error("Item deleted despite failed condition")
	}

	// 2. Condition Success (Expect val = v1)
	old, err = store.DeleteItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, "val = :v", nil, map[string]models.AttributeValue{":v": {S: stringPtr("v1")}}, "ALL_OLD")
	if err != nil {
		t.Errorf("DeleteItem failed: %v", err)
	}
	if old == nil || *old["val"].S != "v1" {
		t.Errorf("Expected OLD item v1, got %v", old)
	}

	// Verify item deleted
	item, _ = store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, true)
	if item != nil {
		t.Error("Item should be deleted")
	}
}
