package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_ReturnValues(t *testing.T) {
	tableName := fmt.Sprintf("test-return-values-%d", time.Now().UnixNano())
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	pk := "item1"
	item := map[string]models.AttributeValue{
		"pk":  {S: &pk},
		"val": {S: strPtr("old")},
	}

	// 2. PutItem without ReturnValues
	attrs, err := store.PutItem(ctx, tableName, item, "")
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}
	if attrs != nil {
		t.Errorf("Expected nil attributes when ReturnValues is empty, got %v", attrs)
	}

	// 3. PutItem with ALL_OLD
	newItem := map[string]models.AttributeValue{
		"pk":  {S: &pk},
		"val": {S: strPtr("new")},
	}
	attrs, err = store.PutItem(ctx, tableName, newItem, "ALL_OLD")
	if err != nil {
		t.Fatalf("PutItem with ALL_OLD failed: %v", err)
	}
	if attrs == nil {
		t.Fatal("Expected old attributes, got nil")
	}
	if *attrs["val"].S != "old" {
		t.Errorf("Expected old value 'old', got '%s'", *attrs["val"].S)
	}

	// 4. DeleteItem with ALL_OLD
	attrs, err = store.DeleteItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, "ALL_OLD")
	if err != nil {
		t.Fatalf("DeleteItem with ALL_OLD failed: %v", err)
	}
	if attrs == nil {
		t.Fatal("Expected old attributes, got nil")
	}
	if *attrs["val"].S != "new" {
		t.Errorf("Expected old value 'new', got '%s'", *attrs["val"].S)
	}

	// 5. DeleteItem without ReturnValues
	// Add item back first
	store.PutItem(ctx, tableName, newItem, "")
	attrs, err = store.DeleteItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, "")
	if err != nil {
		t.Fatalf("DeleteItem failed: %v", err)
	}
	if attrs != nil {
		t.Errorf("Expected nil attributes when ReturnValues is empty, got %v", attrs)
	}
}

func TestFoundationDBStore_ConsistentRead(t *testing.T) {
	tableName := fmt.Sprintf("test-consistent-read-%d", time.Now().UnixNano())
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	pk := "item1"
	item := map[string]models.AttributeValue{
		"pk": {S: &pk},
	}
	store.PutItem(ctx, tableName, item, "")

	// 2. GetItem with ConsistentRead=true
	attrs, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, true)
	if err != nil {
		t.Fatalf("GetItem with ConsistentRead=true failed: %v", err)
	}
	if attrs == nil {
		t.Fatal("Expected item, got nil")
	}

	// 3. Scan with ConsistentRead=true
	items, _, err := store.Scan(ctx, tableName, "", "", nil, nil, 0, nil, true)
	if err != nil {
		t.Fatalf("Scan with ConsistentRead=true failed: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("Expected 1 item, got %d", len(items))
	}

	// 4. Query with ConsistentRead=true
	items, _, err = store.Query(ctx, tableName, "", "pk = :p", "", "", nil, map[string]models.AttributeValue{":p": {S: &pk}}, 0, nil, true)
	if err != nil {
		t.Fatalf("Query with ConsistentRead=true failed: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("Expected 1 item, got %d", len(items))
	}
}
