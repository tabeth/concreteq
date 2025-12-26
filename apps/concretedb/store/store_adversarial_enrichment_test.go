package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestAdversarial_AttributeValueExtremes(t *testing.T) {
	tableName := "test-adversarial-attrs"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	assert.NoError(t, store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	}))

	tests := []struct {
		name     string
		item     map[string]models.AttributeValue
		expected bool // true for success, false for error
	}{
		{
			name: "Empty String Value",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("empty-s")},
				"data": {S: strPtr("")},
			},
			expected: true,
		},
		{
			name: "Unicode/Emoji Value",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("unicode")},
				"data": {S: strPtr("😎 漢 👨‍👩‍👧‍👦")},
			},
			expected: true,
		},
		{
			name: "Numeric Negative Boundary",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("neg-n")},
				"data": {N: strPtr("-1")},
			},
			expected: true,
		},
		{
			name: "Numeric Large Integer",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("large-n")},
				"data": {N: strPtr("9223372036854775807")},
			},
			expected: true,
		},
		{
			name: "Numeric NaN (Should Fail)",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("nan-n")},
				"data": {N: strPtr("NaN")},
			},
			expected: false,
		},
		{
			name: "Numeric Infinity (Should Fail)",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("inf-n")},
				"data": {N: strPtr("Infinity")},
			},
			expected: false,
		},
		{
			name: "Empty Binary Blob",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("empty-b")},
				"data": {B: strPtr("")},
			},
			expected: true,
		},
		{
			name: "Empty Set (Should Fail)",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("empty-ss")},
				"data": {SS: []string{}},
			},
			expected: false,
		},
		{
			name: "Empty Map",
			item: map[string]models.AttributeValue{
				"pk":   {S: strPtr("empty-m")},
				"data": {M: map[string]models.AttributeValue{}},
			},
			expected: true,
		},
		{
			name: "Nested Map",
			item: map[string]models.AttributeValue{
				"pk": {S: strPtr("nested")},
				"data": {M: map[string]models.AttributeValue{
					"level1": {M: map[string]models.AttributeValue{
						"level2": {S: strPtr("bottom")},
					}},
				}},
			},
			expected: true,
		},
		{
			name: "List with Null",
			item: map[string]models.AttributeValue{
				"pk": {S: strPtr("list-null")},
				"data": {L: []models.AttributeValue{
					{NULL: boolPtr(true)},
				}},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.PutItem(ctx, tableName, tt.item, "", nil, nil, "")
			if tt.expected {
				assert.NoError(t, err)
				// Verify round-trip
				retrieved, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": tt.item["pk"]}, "", nil, true)
				assert.NoError(t, err)
				assert.NotNil(t, retrieved)
				// Small check for values
				for k, v := range tt.item {
					ret := retrieved[k]
					// Normalize empty map/slice to nil for comparison if necessary,
					// but let's see if our validation change helped.
					// Actually, empty map/slice in Go json unmarshal usually results in nil if omitempty was used.
					if k == "data" && tt.name == "Empty Map" && ret.M == nil {
						continue // Accept nil for empty map
					}
					assert.Equal(t, v, ret, "Mismatch in attribute %s", k)
				}
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdversarial_ExpressionParsing(t *testing.T) {
	tableName := "test-adversarial-expr"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	assert.NoError(t, store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	}))

	pk := "item1"
	store.PutItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}, "age": {N: strPtr("25")}}, "", nil, nil, "")

	tests := []struct {
		name     string
		expr     string
		expected bool
	}{
		{"Invalid Syntax", "INVALID EXPRESSION !!!", false},
		{"Missing Placeholder Name", "#missing = :v", false},
		{"Missing Placeholder Value", "age = :missing", false},
		{"Valid Complexish", "age = :v", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.UpdateItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: &pk}}, "SET "+tt.expr, "", nil, map[string]models.AttributeValue{":v": {N: strPtr("30")}}, "")
			if tt.expected {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestAdversarial_PaginationLimits(t *testing.T) {
	tableName := "test-adversarial-pagination"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	assert.NoError(t, store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	}))

	// Negative limit
	_, _, err := store.ListTables(ctx, -1, "")
	assert.NoError(t, err, "FDB layer usually handles negative limits as 0 or default, but should not crash")

	// Extremely large limit
	_, _, err = store.ListTables(ctx, 1000000, "")
	assert.NoError(t, err)
}

func TestAdversarial_RecursionDepth(t *testing.T) {
	tableName := "test-adversarial-recursion"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	assert.NoError(t, store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	}))

	// Create a deeply nested map
	deepItem := map[string]models.AttributeValue{
		"pk": {S: strPtr("deep")},
	}
	current := deepItem
	for i := 0; i < 50; i++ {
		next := map[string]models.AttributeValue{}
		current["m"] = models.AttributeValue{M: next}
		current = next
	}

	// This should fail due to recursion depth limit (32).
	_, err := store.PutItem(ctx, tableName, deepItem, "", nil, nil, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "depth exceeds limit")
}
