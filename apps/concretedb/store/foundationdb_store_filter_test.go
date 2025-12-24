package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_Scan_Filter(t *testing.T) {
	// Unique table
	tableName := fmt.Sprintf("test-scan-filter-%d", time.Now().UnixNano())
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table (Simple PK)
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
		},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 2. Put Items
	// Item 1: id=1, type=user, age=20
	// Item 2: id=2, type=user, age=30
	// Item 3: id=3, type=admin, age=35
	items := []map[string]models.AttributeValue{
		{
			"id":   {S: strPtr("1")},
			"type": {S: strPtr("user")},
			"age":  {N: strPtr("20")},
		},
		{
			"id":   {S: strPtr("2")},
			"type": {S: strPtr("user")},
			"age":  {N: strPtr("30")},
		},
		{
			"id":   {S: strPtr("3")},
			"type": {S: strPtr("admin")},
			"age":  {N: strPtr("35")},
		},
	}
	for _, it := range items {
		if _, err := store.PutItem(ctx, tableName, it, ""); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// 3. Tests
	tests := []struct {
		name          string
		filter        string
		vals          map[string]models.AttributeValue
		expectedCount int
	}{
		{
			name:          "FilterByTypeUser",
			filter:        "type = :t",
			vals:          map[string]models.AttributeValue{":t": {S: strPtr("user")}},
			expectedCount: 2, // 1 and 2
		},
		{
			name:          "FilterByAgeGT25",
			filter:        "age > :a",
			vals:          map[string]models.AttributeValue{":a": {N: strPtr("25")}},
			expectedCount: 2, // 2 and 3
		},
		{
			name:          "FilterByAgeAndType", // Evaluator supports basic AND by splitting
			filter:        "type = :t AND age > :a",
			vals:          map[string]models.AttributeValue{":t": {S: strPtr("user")}, ":a": {N: strPtr("25")}},
			expectedCount: 1, // Only id=2 is user AND > 25
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := store.Scan(ctx, tableName, tt.filter, "", nil, tt.vals, 0, nil, false)
			if err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if len(result) != tt.expectedCount {
				t.Errorf("Expected %d items, got %d", tt.expectedCount, len(result))
			}
		})
	}
}

func TestFoundationDBStore_Query_Filter(t *testing.T) {
	// Query restricts by PK, then Filters results
	tableName := fmt.Sprintf("test-query-filter-%d", time.Now().UnixNano())
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table (PK+SK)
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "sk", AttributeType: "S"},
		},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 2. Data
	// pk=A, sk=1, status=active
	// pk=A, sk=2, status=active
	// pk=A, sk=3, status=archived
	// pk=B, sk=1, status=active (Should not be returned by Query PK=A)
	items := []map[string]models.AttributeValue{
		{"pk": {S: strPtr("A")}, "sk": {S: strPtr("1")}, "status": {S: strPtr("active")}},
		{"pk": {S: strPtr("A")}, "sk": {S: strPtr("2")}, "status": {S: strPtr("active")}},
		{"pk": {S: strPtr("A")}, "sk": {S: strPtr("3")}, "status": {S: strPtr("archived")}},
		{"pk": {S: strPtr("B")}, "sk": {S: strPtr("1")}, "status": {S: strPtr("active")}},
	}
	for _, it := range items {
		if _, err := store.PutItem(ctx, tableName, it, ""); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// 3. Tests
	tests := []struct {
		name          string
		keyCond       string
		filter        string
		vals          map[string]models.AttributeValue
		expectedCount int
	}{
		{
			name:          "QueryPKOnly_FilterActive",
			keyCond:       "pk = :p",
			filter:        "status = :s",
			vals:          map[string]models.AttributeValue{":p": {S: strPtr("A")}, ":s": {S: strPtr("active")}},
			expectedCount: 2, // 1 and 2
		},
		{
			name:          "QueryPKAndSK_FilterStatus",
			keyCond:       "pk = :p AND sk > :sk",
			filter:        "status = :s",
			vals:          map[string]models.AttributeValue{":p": {S: strPtr("A")}, ":sk": {S: strPtr("1")}, ":s": {S: strPtr("active")}},
			expectedCount: 1, // Only sk=2 is > 1 AND active. (sk=3 is > 1 but archived).
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := store.Query(ctx, tableName, "", tt.keyCond, tt.filter, "", nil, tt.vals, 0, nil, false)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(result) != tt.expectedCount {
				t.Errorf("Expected %d items, got %d", tt.expectedCount, len(result))
			}
		})
	}
}

// Reuse strPtr helper if in same package? Yes.
// If it collides with another file in same package, it's fine as long as not duplicated in same file or if it's test package.
// If both are `package store`, they share scope.
// Scan test file uses `strPtr` from `query_test`?
// If `foundationdb_store_query_test.go` and `foundationdb_store_filter_test.go` are both in `store`, they share functions.
// I should NOT redefine `strPtr` if it's already there.
// I will check if `query_test.go` exported it? No, it was `func strPtr` (unexported).
// Unexported functions are visible within the same package.
// So I should NOT redefine it if I want to avoid "redeclared" error.
// I'll assume it is visible. I will NOT include `strPtr` definition in this file.
