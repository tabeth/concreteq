package store

import (
	"context"
	"fmt"
	"testing"
	"time" // Added import for time

	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_Query_SortKeyConditions(t *testing.T) {
	tableName := fmt.Sprintf("test-query-sk-%d", time.Now().UnixNano()) // Changed getTimestamp to time.Now().UnixNano()
	store := setupTestStore(t, tableName)                               // Modified setupTestStore call
	ctx := context.Background()

	// 1. Create Table with SK
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

	// 2. Put Items
	// PK="A", SK="1", "2", "3", "10", "prefix#1", "prefix#2"
	// Note: We use string comparison for SK. "1" < "10" < "2"
	items := []struct {
		pk, sk string
	}{
		{"A", "1"},
		{"A", "2"},
		{"A", "3"},
		{"A", "10"},
		{"A", "prefix#1"},
		{"A", "prefix#2"},
		{"B", "1"},
	}

	for _, it := range items {
		pk, sk := it.pk, it.sk
		_, err := store.PutItem(ctx, tableName, map[string]models.AttributeValue{
			"pk": {S: &pk},
			"sk": {S: &sk},
		}, "")
		if err != nil {
			t.Fatalf("PutItem failed for %s/%s: %v", pk, sk, err)
		}
	}

	tests := []struct {
		name          string
		cond          string
		vals          map[string]models.AttributeValue
		expectedCount int
		expectedSKs   []string // order matters
	}{
		{
			name: "Equals",
			cond: "pk = :p AND sk = :s",
			vals: map[string]models.AttributeValue{
				":p": {S: strPtr("A")},
				":s": {S: strPtr("2")},
			},
			expectedCount: 1,
			expectedSKs:   []string{"2"},
		},
		{
			name: "GreaterThan",
			cond: "pk = :p AND sk > :s",
			vals: map[string]models.AttributeValue{
				":p": {S: strPtr("A")},
				":s": {S: strPtr("2")}, // "3", "prefix#1", "prefix#2" are > "2". "10" is < "2" (lex)
			},
			expectedCount: 3,
			expectedSKs:   []string{"3", "prefix#1", "prefix#2"},
		},
		{
			name: "LessThan",
			cond: "pk = :p AND sk < :s",
			vals: map[string]models.AttributeValue{
				":p": {S: strPtr("A")},
				":s": {S: strPtr("2")}, // "1", "10" are < "2"
			},
			expectedCount: 2,
			expectedSKs:   []string{"1", "10"},
		},
		{
			name: "Between",
			cond: "pk = :p AND sk BETWEEN :v1 AND :v2",
			vals: map[string]models.AttributeValue{
				":p":  {S: strPtr("A")},
				":v1": {S: strPtr("1")},
				":v2": {S: strPtr("2")}, // "1", "10", "2" ? "10" is between "1" and "2"? Yes. "1" <= "10" <= "2" is TRUE.
			},
			expectedCount: 3,
			expectedSKs:   []string{"1", "10", "2"},
		},
		{
			name: "BeginsWith",
			cond: "pk = :p AND begins_with(sk, :v)",
			vals: map[string]models.AttributeValue{
				":p": {S: strPtr("A")},
				":v": {S: strPtr("prefix")},
			},
			expectedCount: 2,
			expectedSKs:   []string{"prefix#1", "prefix#2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Query
			items, _, err := store.Query(ctx, tableName, "", tt.cond, "", "", nil, tt.vals, 0, nil, false)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(items) != tt.expectedCount {
				// Gather actual SKs for debug
				var actual []string
				for _, it := range items {
					if it["sk"].S != nil {
						actual = append(actual, *it["sk"].S)
					}
				}
				t.Errorf("expected %d items, got %d. Actual: %v", tt.expectedCount, len(items), actual)
			}

			// Verify content/order if needed
			if len(items) == tt.expectedCount {
				for i, sk := range tt.expectedSKs {
					if *items[i]["sk"].S != sk {
						t.Errorf("index %d: expected sk %s, got %s", i, sk, *items[i]["sk"].S)
					}
				}
			}
		})
	}

}

func strPtr(s string) *string { return &s }
