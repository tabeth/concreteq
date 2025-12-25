package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
	"github.com/tabeth/kiroku-core/libs/fdb/directory"
)

// reused from store_logic_test.go: setupMockStore, MockFDBDatabase, MockDirectoryProvider, etc.

func TestListStreams_Pagination_Logic(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	// Setup: 5 tables, 3 with streams
	tables := []*models.Table{
		{TableName: "T1", StreamSpecification: &models.StreamSpecification{StreamEnabled: true}, LatestStreamArn: "arn:1", LatestStreamLabel: "l1"},
		{TableName: "T2", StreamSpecification: &models.StreamSpecification{StreamEnabled: false}},
		{TableName: "T3", StreamSpecification: &models.StreamSpecification{StreamEnabled: true}, LatestStreamArn: "arn:3", LatestStreamLabel: "l3"},
		{TableName: "T4", StreamSpecification: &models.StreamSpecification{StreamEnabled: false}},
		{TableName: "T5", StreamSpecification: &models.StreamSpecification{StreamEnabled: true}, LatestStreamArn: "arn:5", LatestStreamLabel: "l5"},
	}

	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		dir.ListFunc = func(tr fdbadapter.FDBReadTransaction, path []string, opts directory.ListOptions) ([]string, error) {
			// Mock ListTables behavior: Return all table names first, filtered by opts.After
			allNames := []string{"T1", "T2", "T3", "T4", "T5"}
			var res []string
			count := 0
			start := false
			if opts.After == "" {
				start = true
			}
			for _, n := range allNames {
				if !start {
					if n == opts.After {
						start = true
					}
					continue
				}
				if count < opts.Limit {
					res = append(res, n)
					count++
				}
			}
			return res, nil
		}

		mockRTR := &MockFDBReadTransaction{
			GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
				// We need to return metadata for tables (called inside GetTable -> getTableInternal)
				// fdbadapter wrapper usually packs keys. We need to decode or guess which table.
				// But here we can't easily parse the key without the directory layer being real.
				// However, `GetTable` calls `dir.Open` then `Pack("metadata")`.
				// In our mock usage in manual scan, we can assume the mockDIR returns a path we can recognize OR
				// we just need to know valid calls happen.
				// Simpler: The `ListStreams` implementation calls `ListTables` then `GetTable` for each.
				// `GetTable` uses `db.ReadTransact`.
				// Wait! ListStreams calls ListTables which uses db.ReadTransact.
				// THEN it iterates and calls GetTable which ALSO uses db.ReadTransact.
				// But FDB doesn't support nested transactions easily in this mock unless we handle it.
				// `ListStreams` implementation:
				// It calls `ListTables` (1 txn).
				// Then loop: `GetTable` (1 txn).
				return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
			},
		}
		return f(mockRTR)
	}

	// We need to override db.ReadTransactFunc to handle both ListTables and subsequent GetTable calls.
	// Since they are separate transactions in `ListStreams` (it calls ListTables then loop GetTable),
	// we can switch behavior based on what's being asked.
	// But `ReadTransactFunc` is global.
	// Let's make a smart mock that can handle both.

	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		// We can't easily detect checking specific "tables" vs "metadata" key inside the function easily
		// without deeper mocks.
		// But we know ListTables calls dir.List.
		// GetTable calls dir.Open and then Get.

		dir.ListFunc = func(tr fdbadapter.FDBReadTransaction, path []string, opts directory.ListOptions) ([]string, error) {
			allNames := []string{"T1", "T2", "T3", "T4", "T5"}
			var res []string
			count := 0
			start := false
			if opts.After == "" {
				start = true
			}
			for _, n := range allNames {
				if !start {
					if n == opts.After {
						start = true
					}
					continue
				}
				if count < opts.Limit {
					res = append(res, n)
					count++
				}
			}
			return res, nil
		}

		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			// path ["tables", "T1"] etc.
			// Return a mock subspace that holds the name
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}

		mockRTR := &MockFDBReadTransaction{
			GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
				// k is from tableDir.Pack("metadata")
				// We can try to extract the table name from the key if possible,
				// or just use a shared state if we are sequential.
				// In `GetTable`, it opens dir then gets metadata.
				// The key bytes will contain the tuple path.
				// We can try to match it.
				keyBytes := k.FDBKey()
				// T1
				if contains(keyBytes, "T1") {
					b, _ := json.Marshal(tables[0])
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
				}
				if contains(keyBytes, "T2") {
					b, _ := json.Marshal(tables[1])
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
				}
				if contains(keyBytes, "T3") {
					b, _ := json.Marshal(tables[2])
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
				}
				if contains(keyBytes, "T4") {
					b, _ := json.Marshal(tables[3])
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
				}
				if contains(keyBytes, "T5") {
					b, _ := json.Marshal(tables[4])
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
				}

				return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
			},
		}

		dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) {
			return true, nil
		}

		return f(mockRTR)
	}

	// 1. Limit 2 (expect T1, T3)
	streams, lastArn, err := store.ListStreams(ctx, "", 2, "")
	if err != nil {
		t.Fatalf("ListStreams failed: %v", err)
	}
	if len(streams) != 2 {
		t.Errorf("Expected 2 streams, got %d", len(streams))
	}
	if streams[0].TableName != "T1" || streams[1].TableName != "T3" {
		t.Errorf("Unexpected streams: %v", streams)
	}
	// Last evaluated table name should be used to derive ARN, or just return it?
	// ListStreams returns "LastEvaluatedStreamArn".
	// Logic: if we hit limit, we return the ARN of the last stream?
	// OR does logic continue scanning until it finds `limit` streams?
	// The logic I read in `foundationdb_store.go` was:
	// "Scan tables until we find 'limit' streams or hit end."
	// "names, last, err := s.ListTables(ctx, chunkSize, currentStart)"
	// It loops.
	// So if we request limit=2.
	// 1. ListTables(limit=2) -> T1, T2.
	// T1 has stream -> add. T2 no -> skip.
	// summaries=[T1]. len < 2. Loop.
	// Start=T2. ListTables(limit=2, after=T2) -> T3, T4.
	// T3 has stream -> add. T4 no -> skip.
	// summaries=[T1, T3]. len == 2. Break.
	// Return T1, T3. LastArn = T3's ARN.

	if lastArn != "arn:3" {
		t.Errorf("Expected lastArn arn:3, got %s", lastArn)
	}
}

func contains(b []byte, sub string) bool {
	// Simple helper since we can't easily unpack tuple in this mock without real FDB library logic sometimes
	// But `stringSliceToTuple` packs strings as-is usually.
	for i := 0; i < len(b)-len(sub); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			if b[i+j] != sub[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func TestConditionCheck_EdgeCases(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()
	table := &models.Table{TableName: "T1", KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}

	// Setup: Existing item {pk: v, a: 1}
	existingItem, _ := json.Marshal(map[string]models.AttributeValue{
		"pk": {S: stringPtr("v")},
		"a":  {N: stringPtr("1")},
	})

	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{}, nil
		}
		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					if contains(k.FDBKey(), "metadata") {
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					}
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return existingItem, nil }}
				},
			},
			SetFunc: func(k fdb.KeyConvertible, v []byte) {},
		}
		return f(mockTR)
	}

	// 1. attribute_exists(missing) -> Fail
	_, err := store.PutItem(ctx, "T1", map[string]models.AttributeValue{"pk": {S: stringPtr("v")}}, "attribute_exists(b)", nil, nil, "")
	if err == nil {
		t.Error("Expected error for attribute_exists(missing)")
	}

	// 2. attribute_not_exists(present) -> Fail
	_, err = store.PutItem(ctx, "T1", map[string]models.AttributeValue{"pk": {S: stringPtr("v")}}, "attribute_not_exists(a)", nil, nil, "")
	if err == nil {
		t.Error("Expected error for attribute_not_exists(present)")
	}

	// 3. invalid syntax -> Fail
	_, err = store.PutItem(ctx, "T1", map[string]models.AttributeValue{"pk": {S: stringPtr("v")}}, "INVALID SYNTAX", nil, nil, "")
	if err == nil {
		t.Error("Expected error for invalid syntax")
	}
}

func TestGetRecords_Iterator_Logic(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	// mock table with stream
	table := &models.Table{TableName: "T1", StreamSpecification: &models.StreamSpecification{StreamEnabled: true}}

	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{}, nil
		}
		mockRTR := &MockFDBReadTransaction{
			GetFunc: mockGetForTable(table),
			GetRangeFunc: func(r fdb.Range, opts fdb.RangeOptions) fdbadapter.FDBRangeResult {
				return &MockFDBResult{it: &MockFDBRangeIterator{AdvanceFunc: func() bool { return false }}}
			},
		}
		return f(mockRTR)
	}

	// 1. Invalid Iterator (Base64 but garbage)
	if _, _, err := store.GetRecords(ctx, "invalid-base64", 10); err == nil {
		t.Error("Expected error for invalid base64 iterator")
	}

	// 2. Valid Iterator JSON but invalid JSON structure
	// (Needs to act like Valid Base64 but decodes to garbage JSON)
	garbageJSON := "not-json"
	// The implementation tries to unmarshal JSON. If it's valid base64 but not JSON...
	// base64("not-json") -> "bm90LWpzb24="
	if _, _, err := store.GetRecords(ctx, "bm90LWpzb24=", 10); err == nil {
		t.Error("Expected error for non-json iterator content")
	}
	_ = garbageJSON // Verify intent or remove if unnecessary
}

func TestGlobalTables_Errors(t *testing.T) {
	store, db, _ := setupMockStore()
	ctx := context.Background()

	// 1. Empty Table Name
	if _, err := store.CreateGlobalTable(ctx, &models.CreateGlobalTableRequest{GlobalTableName: ""}); err == nil {
		t.Error("Expected error for empty global table name")
	}

	// 2. Transaction error in Create
	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		return nil, errors.New("fdb error")
	}
	if _, err := store.CreateGlobalTable(ctx, &models.CreateGlobalTableRequest{GlobalTableName: "G1", ReplicationGroup: []models.Replica{{RegionName: "us"}}}); err == nil {
		t.Error("Expected error when FDB fails")
	}
}

func TestListStreams_Error_Paths(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	// 1. ListTables fails
	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		dir.ListFunc = func(tr fdbadapter.FDBReadTransaction, path []string, opts directory.ListOptions) ([]string, error) {
			return nil, errors.New("list error")
		}
		return f(&MockFDBReadTransaction{})
	}
	if _, _, err := store.ListStreams(ctx, "", 10, ""); err == nil {
		t.Error("Expected error when ListTables fails")
	}

	// 2. GetTable fails during scan
	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		t.Log("ReadTransact called")
		dir.ListFunc = func(tr fdbadapter.FDBReadTransaction, path []string, opts directory.ListOptions) ([]string, error) {
			t.Log("ListFunc called")
			return []string{"T1"}, nil
		}
		// Force OpenFunc to fail. GetTable calls Exists(ok) -> Open(fail)
		dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) {
			t.Log("ExistsFunc called")
			return true, nil
		}
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			t.Log("OpenFunc called")
			return nil, errors.New("open error")
		}
		val, err := f(&MockFDBReadTransaction{})
		t.Logf("ReadTransact returning val=%v err=%v", val, err)
		return val, err
	}
	if streams, _, err := store.ListStreams(ctx, "", 10, ""); err != nil {
		t.Errorf("ListStreams returned error when GetTable failed: %v", err)
	} else if len(streams) != 0 {
		t.Errorf("Expected 0 streams when GetTable fails, got %d", len(streams))
	}
}

func TestConditionCheck_Internal_Errors(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()
	table := &models.Table{TableName: "T1", KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}

	// 1. buildKeyTuple error (ValidationException propagated)
	// Missing PK
	err := store.TransactWriteItems(ctx, []models.TransactWriteItem{{
		ConditionCheck: &models.ConditionCheck{
			TableName:           "T1",
			Key:                 map[string]models.AttributeValue{"other": {S: stringPtr("v")}}, // Missing pk
			ConditionExpression: "attribute_exists(pk)",
		},
	}}, "")
	if err == nil {
		t.Error("Expected error for missing key in ConditionCheck")
	}

	// 2. dir.Open error
	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return nil, errors.New("open error")
		}
		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{GetFunc: mockGetForTable(table)},
		}
		return f(mockTR)
	}
	err = store.TransactWriteItems(ctx, []models.TransactWriteItem{{
		ConditionCheck: &models.ConditionCheck{
			TableName:           "T1",
			Key:                 map[string]models.AttributeValue{"pk": {S: stringPtr("v")}},
			ConditionExpression: "attribute_exists(pk)",
		},
	}}, "")
	if err == nil {
		t.Error("Expected error when dir.Open fails")
	}

	// 3. tr.Get error
	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{}, nil
		}
		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					if contains(k.FDBKey(), "metadata") {
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					}
					// Fail on data get
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, errors.New("get error") }}
				},
			},
		}
		return f(mockTR)
	}
	err = store.TransactWriteItems(ctx, []models.TransactWriteItem{{
		ConditionCheck: &models.ConditionCheck{
			TableName:           "T1",
			Key:                 map[string]models.AttributeValue{"pk": {S: stringPtr("v")}},
			ConditionExpression: "attribute_exists(pk)",
		},
	}}, "")
	if err == nil {
		t.Error("Expected error when tr.Get fails")
	}
}
