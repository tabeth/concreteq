package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_Projection(t *testing.T) {
	tableName := fmt.Sprintf("test-projection-%d", time.Now().UnixNano())
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

	// 2. Data
	// pk=1, name=Alice, age=30, role=admin
	item := map[string]models.AttributeValue{
		"pk":   {S: strPtr("1")},
		"name": {S: strPtr("Alice")},
		"age":  {N: strPtr("30")},
		"role": {S: strPtr("admin")},
	}
	if _, err := store.PutItem(ctx, tableName, item, "", nil, nil, ""); err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	tests := []struct {
		name          string
		proj          string
		names         map[string]string
		operation     string // "SCAN" or "QUERY"
		expectedAttrs []string
		missingAttrs  []string
	}{
		{
			name:          "Scan_ProjectName",
			proj:          "name",
			operation:     "SCAN",
			expectedAttrs: []string{"name"},
			missingAttrs:  []string{"pk", "age", "role"},
		},
		{
			name:          "Scan_ProjectNameAndAge",
			proj:          "name, age",
			operation:     "SCAN",
			expectedAttrs: []string{"name", "age"},
			missingAttrs:  []string{"pk", "role"},
		},
		{
			name:          "Query_ProjectRole_WithAlias",
			proj:          "#r",
			names:         map[string]string{"#r": "role"},
			operation:     "QUERY",
			expectedAttrs: []string{"role"},
			missingAttrs:  []string{"pk", "name", "age"},
		},
		{
			name:          "Scan_ProjectNonExistent",
			proj:          "foo",
			operation:     "SCAN",
			expectedAttrs: []string{}, // No attrs should match
			missingAttrs:  []string{"pk", "name", "age", "role", "foo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result []map[string]models.AttributeValue
			var err error

			if tt.operation == "SCAN" {
				result, _, err = store.Scan(ctx, tableName, "", tt.proj, tt.names, nil, 0, nil, false)
			} else {
				// Query PK=1
				result, _, err = store.Query(ctx, tableName, "", "pk = :pk", "", tt.proj, tt.names, map[string]models.AttributeValue{":pk": {S: strPtr("1")}}, 0, nil, false)
			}

			if err != nil {
				t.Fatalf("Operation failed: %v", err)
			}
			if len(result) != 1 {
				t.Fatalf("Expected 1 item, got %d", len(result))
			}

			item := result[0]
			for _, attr := range tt.expectedAttrs {
				if _, ok := item[attr]; !ok {
					t.Errorf("Expected attribute %s missing", attr)
				}
			}
			for _, attr := range tt.missingAttrs {
				if _, ok := item[attr]; ok {
					t.Errorf("Attribute %s should NOT be present", attr)
				}
			}
		})
	}
}
