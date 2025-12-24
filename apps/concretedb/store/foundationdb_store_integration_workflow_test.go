package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_Integration_Workflow(t *testing.T) {
	store := setupTestStore(t, "test-workflow")
	tableName := "UserActions-" + time.Now().Format("20060102150405")
	ctx := context.Background()

	// 1. Create Table
	err := store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "UserID", KeyType: "HASH"}, {AttributeName: "ActionID", KeyType: "RANGE"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "UserID", AttributeType: "S"},
			{AttributeName: "ActionID", AttributeType: "S"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 2. BatchWriteItem: Insert initial data
	puts := []models.WriteRequest{}
	for i := 1; i <= 5; i++ {
		uid := "user1"
		aid := fmt.Sprintf("action-%d", i)
		ts := "2023-01-01"
		puts = append(puts, models.WriteRequest{
			PutRequest: &models.PutRequest{
				Item: map[string]models.AttributeValue{
					"UserID":    {S: &uid},
					"ActionID":  {S: &aid},
					"Timestamp": {S: &ts},
					"Status":    {S: stringPtr("PENDING")},
				},
			},
		})
	}
	// Add another user
	uid2 := "user2"
	aid2 := "action-1"
	puts = append(puts, models.WriteRequest{PutRequest: &models.PutRequest{Item: map[string]models.AttributeValue{"UserID": {S: &uid2}, "ActionID": {S: &aid2}}}})

	unprocessed, err := store.BatchWriteItem(ctx, map[string][]models.WriteRequest{tableName: puts})
	if err != nil {
		t.Fatalf("BatchWriteItem failed: %v", err)
	}
	if len(unprocessed) > 0 {
		t.Errorf("Expected 0 unprocessed, got %d", len(unprocessed))
	}

	// 3. TransactWriteItems: Mixed operations
	// - Put new item for user1
	// - Update existing item for user1 (action-1) to COMPLETED
	// - Check user2 item exists
	newAid := "action-new"
	updateAid := "action-1"
	checkAid := "action-1"

	err = store.TransactWriteItems(ctx, []models.TransactWriteItem{
		{Put: &models.PutItemRequest{TableName: tableName, Item: map[string]models.AttributeValue{"UserID": {S: stringPtr("user1")}, "ActionID": {S: &newAid}, "Status": {S: stringPtr("NEW")}}}},
		{Update: &models.UpdateItemRequest{
			TableName:                 tableName,
			Key:                       map[string]models.AttributeValue{"UserID": {S: stringPtr("user1")}, "ActionID": {S: &updateAid}},
			UpdateExpression:          "SET Status = :s",
			ExpressionAttributeValues: map[string]models.AttributeValue{":s": {S: stringPtr("COMPLETED")}},
		}},
		{ConditionCheck: &models.ConditionCheck{
			TableName:           tableName,
			Key:                 map[string]models.AttributeValue{"UserID": {S: stringPtr("user2")}, "ActionID": {S: &checkAid}},
			ConditionExpression: "attribute_exists(UserID)",
		}},
	}, "")
	if err != nil {
		t.Fatalf("TransactWriteItems failed: %v", err)
	}

	// 4. Query: Fetch user1 actions
	// Expect 5 initial + 1 new = 6 items.
	items, _, err := store.Query(ctx, tableName, "", "UserID = :u", "", "", nil, map[string]models.AttributeValue{":u": {S: stringPtr("user1")}}, 10, nil, true)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(items) != 6 {
		t.Errorf("Expected 6 items for user1, got %d", len(items))
	}

	// Verify Update
	foundCompleted := false
	for _, item := range items {
		if *item["ActionID"].S == "action-1" && *item["Status"].S == "COMPLETED" {
			foundCompleted = true
			break
		}
	}
	if !foundCompleted {
		t.Error("Did not find updated item status COMPLETED")
	}

	// 5. DeleteItem (Conditional)
	// Delete user2 item only if exists
	delAid := "action-1"
	_, err = store.DeleteItem(ctx, tableName, map[string]models.AttributeValue{"UserID": {S: stringPtr("user2")}, "ActionID": {S: &delAid}}, "attribute_exists(UserID)", nil, nil, "NONE")
	if err != nil {
		t.Fatalf("DeleteItem failed: %v", err)
	}

	// 6. Scan: Verify final count
	// user1: 6 items. user2: 0 items. Total 6.
	scanItems, _, err := store.Scan(ctx, tableName, "", "", nil, nil, 100, nil, true)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(scanItems) != 6 {
		t.Errorf("Expected total 6 items, got %d", len(scanItems))
	}
}
