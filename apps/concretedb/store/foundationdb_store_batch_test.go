package store

import (
	"context"
	"testing"

	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_BatchWriteItem(t *testing.T) {
	tableName := "test-batch-write"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table
	table := &models.Table{
		TableName: tableName,
		Status:    models.StatusActive,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 2. BatchWriteItem (Put 2 items, Delete 0)
	putReqs := []models.WriteRequest{
		{
			PutRequest: &models.PutRequest{
				Item: map[string]models.AttributeValue{"id": {S: stringPtr("1")}, "val": {S: stringPtr("one")}},
			},
		},
		{
			PutRequest: &models.PutRequest{
				Item: map[string]models.AttributeValue{"id": {S: stringPtr("2")}, "val": {S: stringPtr("two")}},
			},
		},
	}

	input := map[string][]models.WriteRequest{
		tableName: putReqs,
	}

	unprocessed, err := store.BatchWriteItem(ctx, input)
	if err != nil {
		t.Fatalf("BatchWriteItem failed: %v", err)
	}
	if len(unprocessed) > 0 {
		t.Errorf("Expected 0 unprocessed items, got %d", len(unprocessed))
	}

	// 3. Verify Puts
	item1, _ := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"id": {S: stringPtr("1")}}, true)
	if item1 == nil {
		t.Error("Item 1 not found")
	}
	item2, _ := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"id": {S: stringPtr("2")}}, true)
	if item2 == nil {
		t.Error("Item 2 not found")
	}

	// 4. BatchWriteItem (Delete 1 item)
	delReqs := []models.WriteRequest{
		{
			DeleteRequest: &models.DeleteRequest{
				Key: map[string]models.AttributeValue{"id": {S: stringPtr("1")}},
			},
		},
	}

	inputDel := map[string][]models.WriteRequest{
		tableName: delReqs,
	}

	unprocessedDel, err := store.BatchWriteItem(ctx, inputDel)
	if err != nil {
		t.Fatalf("BatchWriteItem Delete failed: %v", err)
	}
	if len(unprocessedDel) > 0 {
		t.Errorf("Expected 0 unprocessed items, got %d", len(unprocessedDel))
	}

	// 5. Verify Delete
	item1Del, _ := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"id": {S: stringPtr("1")}}, true)
	if item1Del != nil {
		t.Error("Item 1 should be deleted")
	}
}

func TestFoundationDBStore_BatchGetItem(t *testing.T) {
	tableName1 := "test-batch-get-1"
	tableName2 := "test-batch-get-2"
	store := setupTestStore(t, tableName1)
	setupTestStore(t, tableName2)
	ctx := context.Background()

	// 1. Create Tables
	t1 := &models.Table{TableName: tableName1, Status: models.StatusActive, KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}
	t2 := &models.Table{TableName: tableName2, Status: models.StatusActive, KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}
	store.CreateTable(ctx, t1)
	store.CreateTable(ctx, t2)

	// 2. Put Items
	store.PutItem(ctx, tableName1, map[string]models.AttributeValue{"pk": {S: stringPtr("A")}, "data": {S: stringPtr("valA")}}, "", nil, nil, "")
	store.PutItem(ctx, tableName1, map[string]models.AttributeValue{"pk": {S: stringPtr("B")}, "data": {S: stringPtr("valB")}}, "", nil, nil, "")
	store.PutItem(ctx, tableName2, map[string]models.AttributeValue{"pk": {S: stringPtr("C")}, "data": {S: stringPtr("valC")}}, "", nil, nil, "")

	// 3. BatchGetItem
	input := map[string]models.KeysAndAttributes{
		tableName1: {
			Keys: []map[string]models.AttributeValue{
				{"pk": {S: stringPtr("A")}},
				{"pk": {S: stringPtr("B")}},
				{"pk": {S: stringPtr("Z")}}, // Missing
			},
		},
		tableName2: {
			Keys: []map[string]models.AttributeValue{
				{"pk": {S: stringPtr("C")}},
			},
		},
	}

	responses, unprocessed, err := store.BatchGetItem(ctx, input)
	if err != nil {
		t.Fatalf("BatchGetItem failed: %v", err)
	}
	if len(unprocessed) > 0 {
		t.Errorf("Expected 0 unprocessed keys, got %d", len(unprocessed))
	}

	// 4. Verify Responses
	if len(responses[tableName1]) != 2 {
		t.Errorf("Expected 2 items from table 1, got %d", len(responses[tableName1]))
	}
	if len(responses[tableName2]) != 1 {
		t.Errorf("Expected 1 item from table 2, got %d", len(responses[tableName2]))
	}

	// Verify content
	foundA := false
	for _, item := range responses[tableName1] {
		if *item["pk"].S == "A" && *item["data"].S == "valA" {
			foundA = true
		}
	}
	if !foundA {
		t.Error("Item A not found or incorrect")
	}
}
