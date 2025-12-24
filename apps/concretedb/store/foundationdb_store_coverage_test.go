package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_UpdateItem_Comprehensive(t *testing.T) {
	tableName := "test-update-comprehensive"
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
	key := map[string]models.AttributeValue{"pk": {S: &pk}}

	// 2. Update non-existent item (should create)
	attrs, err := store.UpdateItem(ctx, tableName, key, "SET age = :a", nil, map[string]models.AttributeValue{":a": {N: strPtr("25")}}, "ALL_NEW")
	if err != nil {
		t.Fatalf("UpdateItem failed: %v", err)
	}
	if attrs["age"].N == nil || *attrs["age"].N != "25" {
		t.Errorf("expected age 25, got %v", attrs["age"])
	}

	// 3. Update existing item (ALL_OLD)
	attrs, err = store.UpdateItem(ctx, tableName, key, "SET age = :a", nil, map[string]models.AttributeValue{":a": {N: strPtr("30")}}, "ALL_OLD")
	if err != nil {
		t.Fatalf("UpdateItem ALL_OLD failed: %v", err)
	}
	if attrs["age"].N == nil || *attrs["age"].N != "25" {
		t.Errorf("expected old age 25, got %v", attrs["age"])
	}

	// 4. Update existing item (multiple assignments)
	attrs, err = store.UpdateItem(ctx, tableName, key, "SET age = :a, #n = :n", map[string]string{"#n": "name"}, map[string]models.AttributeValue{":a": {N: strPtr("35")}, ":n": {S: strPtr("John")}}, "ALL_NEW")
	if err != nil {
		t.Fatalf("UpdateItem multiple failed: %v", err)
	}
	if *attrs["age"].N != "35" || *attrs["name"].S != "John" {
		t.Errorf("expected new age 35 and name John, got %v", attrs)
	}

	// 5. Update without expression (NONE)
	attrs, err = store.UpdateItem(ctx, tableName, key, "", nil, nil, "")
	if err != nil {
		t.Fatalf("UpdateItem NONE failed: %v", err)
	}
	if attrs != nil {
		t.Errorf("expected nil attributes, got %v", attrs)
	}

	// 6. Table not found
	_, err = store.UpdateItem(ctx, "non-existent", key, "SET age = :a", nil, map[string]models.AttributeValue{":a": {N: strPtr("25")}}, "")
	if err != ErrTableNotFound {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}

	// 7. Unsupported expression (not SET)
	_, err = store.UpdateItem(ctx, tableName, key, "REMOVE age", nil, nil, "")
	if err == nil || !strings.Contains(err.Error(), "only SET expressions supported") {
		t.Error("expected error for unsupported expression")
	}

	// 8. Invalid assignment (missing =)
	_, err = store.UpdateItem(ctx, tableName, key, "SET age :a", nil, map[string]models.AttributeValue{":a": {N: strPtr("25")}}, "")
	if err == nil || !strings.Contains(err.Error(), "invalid assignment") {
		t.Error("expected error for invalid assignment")
	}

	// 9. Missing placeholder in Names
	_, err = store.UpdateItem(ctx, tableName, key, "SET #missing = :a", nil, map[string]models.AttributeValue{":a": {N: strPtr("25")}}, "")
	if err == nil || !strings.Contains(err.Error(), "missing expression attribute name") {
		t.Error("expected error for missing name placeholder")
	}

	// 10. Missing placeholder in Values
	_, err = store.UpdateItem(ctx, tableName, key, "SET age = :missing", nil, nil, "")
	if err == nil || !strings.Contains(err.Error(), "missing expression attribute value") {
		t.Error("expected error for missing value placeholder")
	}

	// 11. Literal value without colon (unsupported by our simple parser)
	_, err = store.UpdateItem(ctx, tableName, key, "SET age = 25", nil, nil, "")
	if err == nil || !strings.Contains(err.Error(), "only literal values with ':' prefix supported") {
		t.Error("expected error for literal without colon")
	}
}

func TestFoundationDBStore_ItemOperations_NotFound(t *testing.T) {
	tableName := "test-not-found-comprehensive"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	key := map[string]models.AttributeValue{"pk": {S: strPtr("1")}}

	// 1. PutItem
	_, err := store.PutItem(ctx, "non-existent", map[string]models.AttributeValue{"pk": {S: strPtr("1")}}, "")
	if err != ErrTableNotFound {
		t.Errorf("PutItem: expected ErrTableNotFound, got %v", err)
	}

	// 2. GetItem
	_, err = store.GetItem(ctx, "non-existent", key, false)
	if err != ErrTableNotFound {
		t.Errorf("GetItem: expected ErrTableNotFound, got %v", err)
	}

	// 3. DeleteItem
	_, err = store.DeleteItem(ctx, "non-existent", key, "")
	if err != ErrTableNotFound {
		t.Errorf("DeleteItem: expected ErrTableNotFound, got %v", err)
	}

	// 4. Scan
	_, _, err = store.Scan(ctx, "non-existent", "", "", nil, nil, 0, nil, false)
	if err != ErrTableNotFound {
		t.Errorf("Scan: expected ErrTableNotFound, got %v", err)
	}

	// 5. Query
	_, _, err = store.Query(ctx, "non-existent", "", "pk = :k", "", "", nil, map[string]models.AttributeValue{":k": {S: strPtr("1")}}, 0, nil, false)
	if err != ErrTableNotFound {
		t.Errorf("Query: expected ErrTableNotFound, got %v", err)
	}
}

func TestFoundationDBStore_Scan_Comprehensive(t *testing.T) {
	tableName := "test-scan-comprehensive"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 2. Insert Items
	for i := 0; i < 5; i++ {
		pk := fmt.Sprintf("pk%d", i)
		item := map[string]models.AttributeValue{"pk": {S: &pk}, "val": {N: strPtr(fmt.Sprintf("%d", i))}}
		if _, err := store.PutItem(ctx, tableName, item, ""); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// 3. Scan with Limit
	items, lek, err := store.Scan(ctx, tableName, "", "", nil, nil, 2, nil, false)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
	if lek == nil {
		t.Fatal("expected LastEvaluatedKey, got nil")
	}

	// 4. Scan with ExclusiveStartKey
	items2, _, err := store.Scan(ctx, tableName, "", "", nil, nil, 10, lek, false)
	if err != nil {
		t.Fatalf("Scan with ESK failed: %v", err)
	}
	if len(items2) != 3 {
		t.Errorf("expected 3 remaining items, got %d", len(items2))
	}

	// 5. Scan with invalid ESK (missing partition key)
	_, _, err = store.Scan(ctx, tableName, "", "", nil, nil, 10, map[string]models.AttributeValue{"missing": {S: strPtr("val")}}, false)
	if err == nil {
		t.Error("expected error for invalid ESK in Scan")
	}
}

func TestFoundationDBStore_Query_Comprehensive(t *testing.T) {
	tableName := "test-query-comprehensive"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table with SK
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 2. Insert Items
	pk := "user1"
	for i := 0; i < 5; i++ {
		sk := fmt.Sprintf("sort%d", i)
		item := map[string]models.AttributeValue{"pk": {S: &pk}, "sk": {S: &sk}, "val": {N: strPtr(fmt.Sprintf("%d", i))}}
		if _, err := store.PutItem(ctx, tableName, item, ""); err != nil {
			t.Fatalf("PutItem failed: %v", err)
		}
	}

	// 3. Query with Limit
	items, lek, err := store.Query(ctx, tableName, "", "pk = :pk", "", "", nil, map[string]models.AttributeValue{":pk": {S: &pk}}, 2, nil, false)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
	if lek == nil {
		t.Fatal("expected LastEvaluatedKey, got nil")
	}

	// 4. Query with ExclusiveStartKey
	items2, _, err := store.Query(ctx, tableName, "", "pk = :pk", "", "", nil, map[string]models.AttributeValue{":pk": {S: &pk}}, 10, lek, false)
	if err != nil {
		t.Fatalf("Query with ESK failed: %v", err)
	}
	if len(items2) != 3 {
		t.Errorf("expected 3 remaining items, got %d", len(items2))
	}

	// 5. Query Error: No HASH key in table (manual corruption)
	_, err = store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		metaKey := tableDir.Pack(tuple.Tuple{"metadata"})
		badMeta := &models.Table{TableName: tableName, KeySchema: []models.KeySchemaElement{}} // No HASH key
		bytes, _ := json.Marshal(badMeta)
		tr.Set(metaKey, bytes)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("setup corruption failed: %v", err)
	}

	_, _, err = store.Query(ctx, tableName, "", "pk = :pk", "", "", nil, map[string]models.AttributeValue{":pk": {S: &pk}}, 10, nil, false)
	if err == nil || !strings.Contains(err.Error(), "no HASH key") {
		t.Errorf("expected error for no HASH key, got %v", err)
	}

	// 6. Query with missing placeholder
	_, _, err = store.Query(ctx, "non-existent", "", "pk = :missing", "", "", nil, nil, 10, nil, false)
	if err == nil {
		t.Error("expected error for missing placeholder in Query")
	}

	// 7. Query with invalid ESK
	_, _, err = store.Query(ctx, "list-tables-comprehensive", "", "pk = :pk", "", "", nil, map[string]models.AttributeValue{":pk": {S: strPtr("val")}}, 10, map[string]models.AttributeValue{"pk": {BOOL: boolPtr(true)}}, false)
	if err == nil {
		t.Error("expected error for invalid ESK in Query")
	}
}

func TestFoundationDBStore_ListTables_Comprehensive(t *testing.T) {
	store := setupTestStore(t, "list-tables-comprehensive")
	ctx := context.Background()

	// Clear all tables for total isolation
	_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		_, err := store.dir.Remove(tr, []string{"tables"})
		return nil, err
	})
	if err != nil {
		t.Fatalf("Clear tables failed: %v", err)
	}

	// 1. Create multiple tables
	for i := 0; i < 5; i++ {
		tableName := fmt.Sprintf("list-table-%d", i)
		table := &models.Table{TableName: tableName, KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}
		if err := store.CreateTable(ctx, table); err != nil {
			t.Fatalf("CreateTable failed: %v", err)
		}
	}

	// 2. List with Limit
	tables, last, err := store.ListTables(ctx, 2, "")
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}
	if last == "" {
		t.Error("expected last evaluated table name")
	}

	// 3. List with ExclusiveStartTableName
	tables2, last2, err := store.ListTables(ctx, 10, last)
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}
	if len(tables2) != 3 {
		t.Errorf("expected 3 remaining tables, got %d", len(tables2))
	}
	if last2 != "" {
		t.Errorf("expected empty last evaluated name, got %s", last2)
	}

	// 4. List with limit 0 (hits default size branch)
	tables3, _, err := store.ListTables(ctx, 0, "")
	if err != nil {
		t.Fatalf("ListTables(0) failed: %v", err)
	}
	if len(tables3) != 5 {
		t.Errorf("expected 5 tables, got %d", len(tables3))
	}
}

func TestFoundationDBStore_PutItem_ReturnValues(t *testing.T) {
	tableName := "test-put-return-values"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	pk := "item1"
	item := map[string]models.AttributeValue{"pk": {S: &pk}, "val": {S: strPtr("old")}}

	// 2. Put initial item
	_, err := store.PutItem(ctx, tableName, item, "")
	if err != nil {
		t.Fatalf("Initial PutItem failed: %v", err)
	}

	// 3. Put replacement with ALL_OLD
	newItem := map[string]models.AttributeValue{"pk": {S: &pk}, "val": {S: strPtr("new")}}
	old, err := store.PutItem(ctx, tableName, newItem, "ALL_OLD")
	if err != nil {
		t.Fatalf("PutItem ALL_OLD failed: %v", err)
	}
	if old == nil || *old["val"].S != "old" {
		t.Errorf("expected old value 'old', got %v", old)
	}
}

func TestFoundationDBStore_CorruptedData_Comprehenisve(t *testing.T) {
	tableName := "test-corrupted-comprehensive"
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
	key := map[string]models.AttributeValue{"pk": {S: &pk}}

	// 2. Write corrupted item data
	_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		itemKey := tableDir.Pack(tuple.Tuple{"data", pk})
		tr.Set(itemKey, []byte("{invalid-json"))
		return nil, nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// 3. GetItem should fail (covers getItemInternal unmarshal error)
	_, err = store.GetItem(ctx, tableName, key, false)
	if err == nil {
		t.Error("expected error from GetItem on corrupted data")
	}

	// 4. UpdateItem should fail during read
	_, err = store.UpdateItem(ctx, tableName, key, "SET age = :a", nil, map[string]models.AttributeValue{":a": {N: strPtr("25")}}, "")
	if err == nil {
		t.Error("expected error from UpdateItem on corrupted data")
	}

	// 5. Scan should fail during unmarshal
	_, _, err = store.Scan(ctx, tableName, "", "", nil, nil, 10, nil, false)
	if err == nil || !strings.Contains(err.Error(), "failed to unmarshal item") {
		t.Errorf("expected unmarshal error from Scan, got %v", err)
	}

	// 6. DeleteItem with ALL_OLD should fail (covers getItemInternal unmarshal error)
	_, err = store.DeleteItem(ctx, tableName, key, "ALL_OLD")
	if err == nil {
		t.Error("expected error from DeleteItem(ALL_OLD) on corrupted data")
	}

	// 7. Corrupt Table Metadata (covers getTableInternal unmarshal error)
	_, err = store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		metaKey := tableDir.Pack(tuple.Tuple{"metadata"})
		tr.Set(metaKey, []byte("{invalid-json"))
		return nil, nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	_, err = store.GetItem(ctx, tableName, key, false)
	if err == nil {
		t.Error("expected error from GetItem on corrupted metadata")
	}

	// 8. DeleteTable on corrupted metadata
	_, err = store.DeleteTable(ctx, tableName)
	if err == nil {
		t.Error("expected error from DeleteTable on corrupted metadata")
	}

	// 9. PutItem on corrupted metadata
	_, err = store.PutItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: strPtr("1")}}, "")
	if err == nil {
		t.Error("expected error from PutItem on corrupted metadata")
	}
}

func TestFoundationDBStore_DeleteTable_ExistingMetadata_Missing(t *testing.T) {
	tableName := "test-delete-missing-meta"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table
	table := &models.Table{TableName: tableName}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 2. Manually delete metadata but leave directory
	_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		metaKey := tableDir.Pack(tuple.Tuple{"metadata"})
		tr.Clear(metaKey)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("manual clear failed: %v", err)
	}

	// 3. DeleteTable should return ErrTableNotFound (existingVal == nil branch)
	_, err = store.DeleteTable(ctx, tableName)
	if err != ErrTableNotFound {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}
}

func TestFoundationDBStore_KeyValidation(t *testing.T) {
	tableName := "test-key-validation"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table with HASH and RANGE
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
	}
	if err := store.CreateTable(ctx, table); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 2. Missing HASH key
	_, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"sk": {S: strPtr("1")}}, false)
	if err == nil {
		t.Error("expected error for missing HASH key")
	}

	// 3. Missing RANGE key
	_, err = store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: strPtr("1")}}, false)
	if err == nil {
		t.Error("expected error for missing RANGE key")
	}

	// 4. Unsupported key type (BOOL)
	_, err = store.GetItem(ctx, tableName, map[string]models.AttributeValue{
		"pk": {BOOL: boolPtr(true)},
		"sk": {S: strPtr("1")},
	}, false)
	if err == nil {
		t.Error("expected error for unsupported key type")
	}
}

func TestFoundationDBStore_Query_EdgeCases(t *testing.T) {
	ctx := context.Background()
	tableName := fmt.Sprintf("test-query-edges-%d", time.Now().UnixNano())
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "sk", AttributeType: "S"},
		},
	})

	// 1. Placeholder fallback (only one value provided, use as PK even if name doesn't match)
	items, _, err := store.Query(ctx, tableName, "", "arbitrary = :v", "", "", nil, map[string]models.AttributeValue{
		":v": {S: strPtr("val1")},
	}, 0, nil, false)
	if err != nil {
		t.Fatalf("Query fallback failed: %v", err)
	}
	if len(items) != 0 {
		t.Error("expected 0 items for non-existent pk")
	}

	// 2. begins_with with 0xFF byte
	ff := "abc" + string([]byte{0xFF})
	store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"pk": {S: strPtr("pk1")},
		"sk": {S: strPtr(ff + "suffix")},
	}, "")

	items, _, err = store.Query(ctx, tableName, "", "pk = :p AND begins_with(sk, :s)", "", "", nil, map[string]models.AttributeValue{
		":p": {S: strPtr("pk1")},
		":s": {S: strPtr(ff)},
	}, 0, nil, false)
	if err != nil {
		t.Fatalf("begins_with ff failed: %v", err)
	}
	if len(items) != 1 {
		t.Error("expected 1 item for begins_with 0xFF")
	}

	// 3. Limit and LastEvaluatedKey
	pkLimit := "pk_limit"
	for i := 0; i < 5; i++ {
		store.PutItem(ctx, tableName, map[string]models.AttributeValue{
			"pk": {S: &pkLimit},
			"sk": {S: strPtr(fmt.Sprintf("sk%d", i))},
		}, "")
	}

	items, lek, err := store.Query(ctx, tableName, "", "pk = :p", "", "", nil, map[string]models.AttributeValue{
		":p": {S: &pkLimit},
	}, 2, nil, false)
	if err != nil {
		t.Fatalf("Query limit failed: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
	if lek == nil || *lek["sk"].S != "sk1" {
		t.Errorf("expected LEK sk1, got %v", lek)
	}
}

func TestFoundationDBStore_UpdateItem_ALL_OLD_NonExistent(t *testing.T) {
	ctx := context.Background()
	tableName := "test-update-all-old-missing"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	})

	// Update non-existent item with ALL_OLD
	old, err := store.UpdateItem(ctx, tableName, map[string]models.AttributeValue{
		"id": {S: strPtr("999")},
	}, "SET attr = :v", nil, map[string]models.AttributeValue{
		":v": {S: strPtr("val")},
	}, "ALL_OLD")

	if err != nil {
		t.Fatalf("UpdateItem failed: %v", err)
	}
	// DynamoDB behavior: If item didn't exist, ALL_OLD returns nothing (nil).
	if old != nil && len(old) > 0 {
		t.Errorf("expected nil/empty for ALL_OLD on non-existent item, got %v", old)
	}
}

func TestFoundationDBStore_DeleteItem_ALL_OLD(t *testing.T) {
	ctx := context.Background()
	tableName := "test-delete-all-old"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	})

	store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"id":   {S: strPtr("123")},
		"data": {S: strPtr("val")},
	}, "")

	old, err := store.DeleteItem(ctx, tableName, map[string]models.AttributeValue{
		"id": {S: strPtr("123")},
	}, "ALL_OLD")

	if err != nil {
		t.Fatalf("DeleteItem failed: %v", err)
	}
	if *old["data"].S != "val" {
		t.Errorf("expected val, got %s", *old["data"].S)
	}
}

func TestFoundationDBStore_Scan_FilterError(t *testing.T) {
	ctx := context.Background()
	tableName := "test-scan-filter-err"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	})
	store.PutItem(ctx, tableName, map[string]models.AttributeValue{"id": {S: strPtr("1")}}, "")

	_, _, err := store.Scan(ctx, tableName, "id INVALID :v", "", nil, map[string]models.AttributeValue{":v": {S: strPtr("1")}}, 0, nil, false)
	if err == nil {
		t.Error("expected error for invalid filter")
	}
}

func TestFoundationDBStore_Query_ResolvedPK_Fail(t *testing.T) {
	ctx := context.Background()
	tableName := "test-query-pk-fail"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	})

	// Multiple values, cannot resolve PK automatically if not named
	_, _, err := store.Query(ctx, tableName, "", "something", "", "", nil, map[string]models.AttributeValue{
		":v1": {S: strPtr("1")},
		":v2": {S: strPtr("2")},
	}, 0, nil, false)
	if err == nil {
		t.Error("expected error for ambiguous PK")
	}
}

func boolPtr(b bool) *bool { return &b }
func TestFoundationDBStore_Internal_EdgeCases(t *testing.T) {
	ctx := context.Background()
	tableName := "test-internal-edges"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	})

	// 1. Directory exists but metadata is nil (hits getTableInternal line 753)
	store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		tr.Clear(tableDir.Pack(tuple.Tuple{"metadata"}))
		return nil, nil
	})
	// GetItem calls getTableInternal
	_, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: strPtr("1")}}, false)
	if err == nil {
		t.Error("expected error for missing metadata")
	}

	// 2. Item key exists but value is empty (hits getItemInternal line 773)
	// Re-create table properly
	store.DeleteTable(ctx, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	})
	store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		itemKey := tableDir.Pack(tuple.Tuple{"data", "item_empty"})
		tr.Set(itemKey, []byte{}) // empty value
		return nil, nil
	})
	item, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: strPtr("item_empty")}}, false)
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	if item != nil {
		t.Error("expected nil item for empty value")
	}
}

func TestFoundationDBStore_Query_AllOperators(t *testing.T) {
	ctx := context.Background()
	tableName := "test-query-all-ops"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
	})

	p := "pk1"
	for i := 1; i <= 5; i++ {
		sk := fmt.Sprintf("sk%d", i)
		store.PutItem(ctx, tableName, map[string]models.AttributeValue{
			"pk": {S: &p},
			"sk": {S: &sk},
		}, "")
	}

	tests := []struct {
		expr      string
		vals      map[string]models.AttributeValue
		wantCount int
	}{
		{"pk = :p AND sk = :s", map[string]models.AttributeValue{":p": {S: &p}, ":s": {S: strPtr("sk3")}}, 1},
		{"pk = :p AND sk < :s", map[string]models.AttributeValue{":p": {S: &p}, ":s": {S: strPtr("sk3")}}, 2},
		{"pk = :p AND sk <= :s", map[string]models.AttributeValue{":p": {S: &p}, ":s": {S: strPtr("sk3")}}, 3},
		{"pk = :p AND sk > :s", map[string]models.AttributeValue{":p": {S: &p}, ":s": {S: strPtr("sk3")}}, 2},
		{"pk = :p AND sk >= :s", map[string]models.AttributeValue{":p": {S: &p}, ":s": {S: strPtr("sk3")}}, 3},
		{"pk = :p AND sk BETWEEN :v1 AND :v2", map[string]models.AttributeValue{":p": {S: &p}, ":v1": {S: strPtr("sk2")}, ":v2": {S: strPtr("sk4")}}, 3},
	}

	for _, tt := range tests {
		items, _, err := store.Query(ctx, tableName, "", tt.expr, "", "", nil, tt.vals, 0, nil, false)
		if err != nil {
			t.Errorf("Query(%s) failed: %v", tt.expr, err)
			continue
		}
		if len(items) != tt.wantCount {
			t.Errorf("Query(%s) got %d, want %d", tt.expr, len(items), tt.wantCount)
		}
	}
}

func TestFoundationDBStore_FinalEdgeCases(t *testing.T) {
	ctx := context.Background()
	tableName := "test-final-edges"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	})

	// 1. UpdateItem with bad key (hits line 862)
	_, err := store.UpdateItem(ctx, tableName, map[string]models.AttributeValue{"bad": {S: strPtr("1")}}, "", nil, nil, "")
	if err == nil {
		t.Error("expected error for bad key in UpdateItem")
	}

	// 2. DeleteItem with bad key (hits line 702)
	_, err = store.DeleteItem(ctx, tableName, map[string]models.AttributeValue{"bad": {S: strPtr("1")}}, "")
	if err == nil {
		t.Error("expected error for bad key in DeleteItem")
	}

	// 3. Scan with bad ESK (hits line 105)
	_, _, err = store.Scan(ctx, tableName, "", "", nil, nil, 0, map[string]models.AttributeValue{"bad": {S: strPtr("1")}}, false)
	if err == nil {
		t.Error("expected error for bad ESK in Scan")
	}

	// 4. Query with bad ESK (hits line 359)
	_, _, err = store.Query(ctx, tableName, "", "pk = :p", "", "", nil, map[string]models.AttributeValue{":p": {S: strPtr("1")}}, 0, map[string]models.AttributeValue{"bad": {S: strPtr("1")}}, false)
	if err == nil {
		t.Error("expected error for bad ESK in Query")
	}
}

func TestFoundationDBStore_CorruptedMetadata_Extra(t *testing.T) {
	ctx := context.Background()
	tableName := "test-corrupted-meta-extra"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	})

	// Corrupt metadata by writing invalid JSON
	store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		tr.Set(tableDir.Pack(tuple.Tuple{"metadata"}), []byte("{invalid-json"))
		return nil, nil
	})

	_, err := store.GetTable(ctx, tableName)
	if err == nil {
		t.Error("expected error for corrupted table metadata")
	}
}

func TestFoundationDBStore_CorruptedItem_Extra(t *testing.T) {
	ctx := context.Background()
	tableName := "test-corrupted-item-extra"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	})

	// Corrupt item by writing invalid JSON
	store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		// HASH key "123" maps to a simple tuple
		itemKey := tableDir.Pack(tuple.Tuple{"data", "123"})
		tr.Set(itemKey, []byte("{invalid-json"))
		return nil, nil
	})

	_, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"id": {S: strPtr("123")}}, false)
	if err == nil {
		t.Error("expected error for corrupted item data")
	}
}

func TestFoundationDBStore_TableNotFound_Internal(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t, "unused")
	tableName := "missing-table-forever"

	// test CRUD ops with missing table
	_, err := store.DeleteItem(ctx, tableName, nil, "")
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}

	_, err = store.UpdateItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: strPtr("1")}}, "", nil, nil, "")
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}

	_, _, err = store.Scan(ctx, tableName, "", "", nil, nil, 0, nil, false)
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}
}

func TestFoundationDBStore_KeyEdges(t *testing.T) {
	ctx := context.Background()
	tableName := "test-key-edges"
	store := setupTestStore(t, tableName)
	table := &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	}
	store.CreateTable(ctx, table)

	// 1. Missing key attribute
	_, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{"wrong": {S: strPtr("1")}}, false)
	if err == nil {
		t.Error("expected error for missing key attribute")
	}

	// 2. Unsupported key type
	_, err = store.GetItem(ctx, tableName, map[string]models.AttributeValue{"pk": {BOOL: boolPtr(true)}}, false)
	if err == nil {
		t.Error("expected error for unsupported key type (BOOL)")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

func TestFoundationDBStore_ListTables_LimitZero(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t, "unused")
	// Should default to 100, no error
	_, _, err := store.ListTables(ctx, 0, "")
	if err != nil {
		t.Errorf("expected no error for limit 0, got %v", err)
	}
}

func TestFoundationDBStore_CRUD_TableNotFound(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t, "unused")
	tableName := "i-do-not-exist"

	// PutItem
	_, err := store.PutItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: strPtr("1")}}, "NONE")
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}

	// GetTable
	respT, err := store.GetTable(ctx, tableName)
	if err != nil {
		t.Errorf("expected no error for GetTable on missing table, got %v", err)
	}
	if respT != nil {
		t.Errorf("expected nil table for missing table")
	}

	// DeleteTable
	_, err = store.DeleteTable(ctx, tableName)
	if !errors.Is(err, ErrTableNotFound) {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}
}

func TestFoundationDBStore_Pagination_ListTables(t *testing.T) {
	ctx := context.Background()
	store := setupTestStore(t, "unused")
	store.CreateTable(ctx, &models.Table{TableName: "a"})
	store.CreateTable(ctx, &models.Table{TableName: "b"})

	names, next, err := store.ListTables(ctx, 1, "")
	if err != nil || len(names) != 1 || next == "" {
		t.Errorf("Pagination failed: names=%v, next=%s, err=%v", names, next, err)
	}

	names2, _, err := store.ListTables(ctx, 1, next)
	if err != nil || len(names2) != 1 {
		t.Errorf("Pagination page 2 failed: names=%v, err=%v", names2, err)
	}
}

func TestFoundationDBStore_RangeKey_GetItem(t *testing.T) {
	ctx := context.Background()
	tableName := "test-range-get"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
	})

	item := map[string]models.AttributeValue{
		"pk": {S: strPtr("p1")},
		"sk": {N: strPtr("10")},
		"v":  {S: strPtr("val")},
	}
	store.PutItem(ctx, tableName, item, "NONE")

	// Success with both keys
	got, err := store.GetItem(ctx, tableName, map[string]models.AttributeValue{
		"pk": {S: strPtr("p1")},
		"sk": {N: strPtr("10")},
	}, false)
	if err != nil || got == nil {
		t.Errorf("Range Get failed: %v", err)
	}
}

func TestFoundationDBStore_Query_SK_Edges(t *testing.T) {
	ctx := context.Background()
	tableName := "test-query-sk-edges"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
	})

	// 1. begins_with on numeric SK (unsupported)
	vals := map[string]models.AttributeValue{
		":p": {S: strPtr("p1")},
		":s": {N: strPtr("1")},
	}
	_, _, err := store.Query(ctx, tableName, "", "pk = :p AND begins_with(sk, :s)", "", "", nil, vals, 0, nil, false)
	if err == nil {
		t.Error("expected error for begins_with on numeric SK")
	}

	// 2. unsupported operator
	_, _, err = store.Query(ctx, tableName, "", "pk = :p AND sk IN :s", "", "", nil, vals, 0, nil, false)
	if err == nil {
		t.Error("expected error for unsupported SK operator")
	}
}

func TestFoundationDBStore_Cycle_PutItem(t *testing.T) {
	ctx := context.Background()
	tableName := "test-cycle"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	})

	// Create cyclic item
	// We need to be careful, as Go maps are references.
	// But AttributeValue struct is value type, except slices/maps inside.
	// We need to construct a cycle.
	// Recursive map
	// Recursive map
	// We can't put m directly into AttributeValue value because it copies?
	// No, M is map[string]AttributeValue.
	// If we do:
	// item := make(map[string]models.AttributeValue)
	// sub := models.AttributeValue{M: item}
	// item["self"] = sub
	// json.Marshal(item) -> error

	item := make(map[string]models.AttributeValue)
	item["pk"] = models.AttributeValue{S: strPtr("1")}
	sub := models.AttributeValue{M: item}
	item["self"] = sub

	_, err := store.PutItem(ctx, tableName, item, "NONE")
	if err == nil {
		t.Error("expected error for cyclic item")
	}
}

func TestFoundationDBStore_CorruptedItem_Operations(t *testing.T) {
	ctx := context.Background()
	tableName := "test-corrupted-ops"
	store := setupTestStore(t, tableName)
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
	})

	// Corrupt item
	store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tableDir, _ := store.dir.Open(tr, []string{"tables", tableName}, nil)
		itemKey := tableDir.Pack(tuple.Tuple{"data", "1"})
		tr.Set(itemKey, []byte("{invalid-json"))
		return nil, nil
	})

	// 1. UpdateItem should fail to unmarshal existing item
	_, err := store.UpdateItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: strPtr("1")}}, "SET a = :v", nil, map[string]models.AttributeValue{":v": {S: strPtr("x")}}, "")
	if err == nil {
		t.Error("expected error for UpdateItem on corrupted data")
	}

	// 2. DeleteItem with ALL_OLD should fail to unmarshal
	_, err = store.DeleteItem(ctx, tableName, map[string]models.AttributeValue{"pk": {S: strPtr("1")}}, "ALL_OLD")
	if err == nil {
		t.Error("expected error for DeleteItem on corrupted data")
	}
}
