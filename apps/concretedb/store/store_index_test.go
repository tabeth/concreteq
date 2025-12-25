package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
)

// Helper copied from fdb_mocks_test.go if needed, but since it's in same package tests, it should be available.
// If stringSliceToTuple is not exported, we might need to redefine it or assume it's available.
// It was defined as `func stringSliceToTuple` (unexported) in `fdb_mocks_test.go`.
// Tests in same package share unexported symbols.

func TestIndexes_Types_Logic_Isolated(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	table := &models.Table{
		TableName: "T_Idx",
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
			{IndexName: "Ig_Num", KeySchema: []models.KeySchemaElement{{AttributeName: "num", KeyType: "HASH"}}, Projection: models.Projection{ProjectionType: "ALL"}},
			{IndexName: "Ig_Bin", KeySchema: []models.KeySchemaElement{{AttributeName: "bin", KeyType: "HASH"}}, Projection: models.Projection{ProjectionType: "ALL"}},
		},
	}
	// Verify PutItem calls toFDBElement for N and B
	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}
		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					// Metadata
					if strings.Contains(string(k.FDBKey()), "metadata") {
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					}
					// Old item nil (return nil for Get -> Get)
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				},
			},
			SetFunc:                  func(k fdb.KeyConvertible, v []byte) {},
			ClearFunc:                func(k fdb.KeyConvertible) {},
			ClearRangeFunc:           func(r fdb.ExactRange) {},
			SetVersionstampedKeyFunc: func(k fdb.KeyConvertible, v []byte) {},
		}
		return f(mockTR)
	}

	item := map[string]models.AttributeValue{
		"pk":  {S: stringPtr("p1")},
		"num": {N: stringPtr("123")},
		"bin": {B: stringPtr("base64data")},
	}
	if _, err := store.PutItem(ctx, "T_Idx", item, "", nil, nil, "NONE"); err != nil {
		t.Fatalf("PutItem with N/B indexes failed: %v", err)
	}

}

func TestIndexes_Error_Logic_Isolated(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	table := &models.Table{
		TableName: "T_Err",
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
			{IndexName: "Ig_Bad", KeySchema: []models.KeySchemaElement{{AttributeName: "bad", KeyType: "HASH"}}, Projection: models.Projection{ProjectionType: "KEYS_ONLY"}},
		},
	}

	// 1. Unsupported index key type
	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}
		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					if strings.Contains(string(k.FDBKey()), "metadata") {
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					}
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				},
			},
			SetFunc:   func(k fdb.KeyConvertible, v []byte) {},
			ClearFunc: func(k fdb.KeyConvertible) {},
		}
		return f(mockTR)
	}

	itemBad := map[string]models.AttributeValue{
		"pk":  {S: stringPtr("p1")},
		"bad": {BOOL: boolPtr(true)}, // BOOL not supported for index key
	}
	if _, err := store.PutItem(ctx, "T_Err", itemBad, "", nil, nil, "NONE"); err == nil {
		t.Error("PutItem should fail on unsupported index key type")
	}

	// 2. Index Directory Open Error
	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			// Fail on index open
			// path usually ["tables", tableName, "index", indexName]
			if len(path) > 2 && path[2] == "index" {
				return nil, errors.New("index dir open error")
			}
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}
		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					if strings.Contains(string(k.FDBKey()), "metadata") {
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					}
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				},
			},
			SetFunc:   func(k fdb.KeyConvertible, v []byte) {},
			ClearFunc: func(k fdb.KeyConvertible) {},
		}
		return f(mockTR)
	}
	itemOk := map[string]models.AttributeValue{
		"pk":  {S: stringPtr("p1")},
		"bad": {S: stringPtr("ok")},
	}
	if _, err := store.PutItem(ctx, "T_Err", itemOk, "", nil, nil, "NONE"); err == nil {
		t.Error("PutItem should fail on index directory error")
	}
}

func TestIndexes_LSI_Logic_Isolated(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	table := &models.Table{
		TableName: "T_Lsi",
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "sk", KeyType: "RANGE"}},
		LocalSecondaryIndexes: []models.LocalSecondaryIndex{
			{IndexName: "Il_S", KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}, {AttributeName: "lsk", KeyType: "RANGE"}}, Projection: models.Projection{ProjectionType: "ALL"}},
		},
	}

	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}
		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					if strings.Contains(string(k.FDBKey()), "metadata") {
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					}
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				},
			},
			SetFunc:   func(k fdb.KeyConvertible, v []byte) {},
			ClearFunc: func(k fdb.KeyConvertible) {},
		}
		return f(mockTR)
	}

	item := map[string]models.AttributeValue{
		"pk":  {S: stringPtr("p1")},
		"sk":  {S: stringPtr("s1")},
		"lsk": {S: stringPtr("l1")},
	}
	// This covers LSI loop in putIndexEntries
	if _, err := store.PutItem(ctx, "T_Lsi", item, "", nil, nil, "NONE"); err != nil {
		t.Fatalf("PutItem with LSI failed: %v", err)
	}

	// This covers LSI loop in deleteIndexEntries (via PutItem with same PK/SK replacing old)
	// Mock non-nil old item to trigger deletion
	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					if strings.Contains(string(k.FDBKey()), "metadata") {
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					}
					// Return "old" item for data key
					if strings.Contains(string(k.FDBKey()), "data") {
						b, _ := json.Marshal(item)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					}
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				},
			},
			SetFunc:   func(k fdb.KeyConvertible, v []byte) {},
			ClearFunc: func(k fdb.KeyConvertible) {},
		}
		return f(mockTR)
	}
	if _, err := store.PutItem(ctx, "T_Lsi", item, "", nil, nil, "NONE"); err != nil {
		t.Fatalf("PutItem replacing LSI item failed: %v", err)
	}
}
