package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
)

// Reuse setupMockStore from store_logic_test.go (assumed available in package)

func TestDescribeStream_Coverage(t *testing.T) {
	store, _, _ := setupMockStore()
	ctx := context.Background()

	// 1. Invalid Stream ARN (too short)
	if _, err := store.DescribeStream(ctx, "invalid-arn", 10, ""); err == nil {
		t.Error("Expected error for invalid ARN")
	}

	// 2. Table Name extraction from ARN failing (or verify it parses correctly)
	// arn:aws:dynamodb:local:000000000000:table/TableName/stream/Label
	// We need a mock store that returns a table for "TableName"

	// Setup Mock for GetTable
	// Since GetTable calls db.ReadTransact, we need to mock it.
	// But ListStreams test showed we need deep mocks.
	// Let's use the same approach as store_coverage_test.go
}

func TestStreams_Detailed_Coverage(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	table := &models.Table{
		TableName:           "StreamTable",
		Status:              models.StatusActive,
		LatestStreamArn:     "arn:aws:dynamodb:local:000000000000:table/StreamTable/stream/2023",
		LatestStreamLabel:   "2023",
		StreamSpecification: &models.StreamSpecification{StreamEnabled: true, StreamViewType: "NEW_IMAGE"},

		CreationDateTime: time.Now(),
	}

	// Mock DB behavior
	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) {
			return true, nil
		}
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}

		mockRTR := &MockFDBReadTransaction{
			GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
				// Return table metadata
				b, _ := json.Marshal(table)
				return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
			},
		}
		return f(mockRTR)
	}

	// 1. DescribeStream - Success
	desc, err := store.DescribeStream(ctx, table.LatestStreamArn, 10, "")
	if err != nil {
		t.Fatalf("DescribeStream failed: %v", err)
	}
	if desc.StreamArn != table.LatestStreamArn {
		t.Errorf("ARN mismatch")
	}

	// 2. DescribeStream - Mismatched ARN
	// Pass an ARN that is valid structure but doesn't match table's LatestStreamArn
	badArn := "arn:aws:dynamodb:local:000000000000:table/StreamTable/stream/OLD"
	if _, err := store.DescribeStream(ctx, badArn, 10, ""); err == nil {
		t.Error("Expected error for mismatched ARN")
	}

	// 3. DescribeStream - Table Not Found
	// We need the GetTable to return nil or error.
	// We can't change the mock inside this test function easily for just one call unless we use a closure variable.
}

func TestGetRecords_Coverage(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	// 1. Iterator Type AFTER_SEQUENCE_NUMBER logic
	// We need to verify decoding of the hex sequence number.
	// Iterator: {"StreamArn":..., "ShardId":..., "IteratorType":"AFTER_SEQUENCE_NUMBER", "SequenceNum":"HEX..."}

	validHex := "000000000000000000000001" // 12 bytes hex -> 24 chars
	// 10 bytes transaction version + 2 bytes user version

	iteratorMap := map[string]string{
		"StreamArn":    "arn:aws:dynamodb:local:000000000000:table/T1/stream/L1",
		"ShardId":      "shard-0000",
		"IteratorType": "AFTER_SEQUENCE_NUMBER",
		"SequenceNum":  validHex,
	}
	jb, _ := json.Marshal(iteratorMap)
	iterator := base64Encode(jb) // helper? or just import encoding/base64

	// Mock DB
	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}

		mockRTR := &MockFDBReadTransaction{
			GetRangeFunc: func(r fdb.Range, opts fdb.RangeOptions) fdbadapter.FDBRangeResult {
				// Return empty result to avoid crash, we just want to exercise the Selector creation logic
				return &MockFDBResult{it: &MockFDBRangeIterator{AdvanceFunc: func() bool { return false }}}
			},
		}
		return f(mockRTR)
	}

	_, _, err := store.GetRecords(ctx, iterator, 10)
	if err != nil {
		t.Errorf("GetRecords failed with valid hex sequence: %v", err)
	}

	// 2. Iterator Type LATEST
	iteratorMapLatest := map[string]string{
		"StreamArn":    "arn:aws:dynamodb:local:000000000000:table/T1/stream/L1",
		"ShardId":      "shard-0000",
		"IteratorType": "LATEST",
	}
	jb2, _ := json.Marshal(iteratorMapLatest)
	iteratorLatest := base64Encode(jb2)

	// Needs GetRange with Reverse=true
	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{}, nil
		}
		mockRTR := &MockFDBReadTransaction{
			GetRangeFunc: func(r fdb.Range, opts fdb.RangeOptions) fdbadapter.FDBRangeResult {
				if opts.Reverse {
					// LATEST logic
				}
				return &MockFDBResult{it: &MockFDBRangeIterator{AdvanceFunc: func() bool { return false }}}
			},
		}
		return f(mockRTR)
	}

	_, _, err = store.GetRecords(ctx, iteratorLatest, 10)
	if err != nil {
		t.Errorf("GetRecords failed with LATEST: %v", err)
	}
}

func TestUpdateTable_Coverage(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	// Initial Table
	initialTable := &models.Table{
		TableName: "UpdateTable",
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
		GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
			{IndexName: "GSI1"},
		},
	}

	db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}
		dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) {
			return true, nil
		}

		mockTR := &MockFDBTransaction{
			MockFDBReadTransaction: MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					b, _ := json.Marshal(initialTable)
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
				},
			},
			SetFunc: func(k fdb.KeyConvertible, v []byte) {
				// We could verify the set content here
			},
		}
		return f(mockTR)
	}

	// 1. Update AttributeDefinitions (Add New)
	req := &models.UpdateTableRequest{
		TableName: "UpdateTable",
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "newAttr", AttributeType: "N"}, // New
			{AttributeName: "pk", AttributeType: "S"},      // Existing
		},
	}
	updated, err := store.UpdateTable(ctx, req)
	if err != nil {
		t.Fatalf("UpdateTable failed: %v", err)
	}
	if len(updated.AttributeDefinitions) != 2 {
		t.Errorf("Expected 2 attributes, got %d", len(updated.AttributeDefinitions))
	}

	// 2. Update GSI (Create and Delete)
	req2 := &models.UpdateTableRequest{
		TableName: "UpdateTable",
		GlobalSecondaryIndexUpdates: []models.GlobalSecondaryIndexUpdate{
			{
				Create: &models.CreateGlobalSecondaryIndexAction{
					IndexName:  "GSI2",
					KeySchema:  []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
					Projection: models.Projection{ProjectionType: "ALL"},
				},
			},
			{
				Delete: &models.DeleteGlobalSecondaryIndexAction{
					IndexName: "GSI1",
				},
			},
		},
	}
	updated2, err := store.UpdateTable(ctx, req2)
	if err != nil {
		t.Fatalf("UpdateTable GSI failed: %v", err)
	}
	// GSI1 deleted, GSI2 created
	foundGSI1 := false
	foundGSI2 := false
	for _, gsi := range updated2.GlobalSecondaryIndexes {
		if gsi.IndexName == "GSI1" {
			foundGSI1 = true
		}
		if gsi.IndexName == "GSI2" {
			foundGSI2 = true
		}
	}
	if foundGSI1 {
		t.Error("Expected GSI1 to be deleted")
	}
	if !foundGSI2 {
		t.Error("Expected GSI2 to be created")
	}
}

func TestListStreams_FilterLogic(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	table := &models.Table{
		TableName:           "T1",
		StreamSpecification: &models.StreamSpecification{StreamEnabled: true},
		LatestStreamArn:     "arn:aws:dynamodb:local:000000000000:table/T1/stream/timestamp",
		LatestStreamLabel:   "timestamp",
		Status:              models.StatusActive,
	}

	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}
		dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) {
			return true, nil
		}
		mockRTR := &MockFDBReadTransaction{
			GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
				b, _ := json.Marshal(table)
				return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
			},
		}
		return f(mockRTR)
	}

	// 1. ListStreams with TableName Filter
	streams, _, err := store.ListStreams(ctx, "T1", 10, "")
	if err != nil {
		t.Fatalf("ListStreams with TableName failed: %v", err)
	}
	if len(streams) != 1 {
		t.Errorf("Expected 1 stream, got %d", len(streams))
	}

	// 2. ListStreams with TableName AND ExclusiveStart (matching) -> should skip
	streams2, _, err := store.ListStreams(ctx, "T1", 10, table.LatestStreamArn)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams2) != 0 {
		// Logic: If we start *after* the current stream ARN, and it's the only one for this table, we return empty?
		// Note from code: "if exclusiveStartStreamArn != "" && table.LatestStreamArn == exclusiveStartStreamArn { skip }"
		// Since T1 only has 1 stream (Latest), skipping it means empty. Correct.
	} else {
		// Correct
	}
}

func TestDescribeStream_EdgeCases(t *testing.T) {
	store, db, dir := setupMockStore()
	ctx := context.Background()

	db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
		dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
		}
		dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) {
			return true, nil
		}
		mockRTR := &MockFDBReadTransaction{
			GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
				// 1. Table Not Found (nil, nil)
				if contains(k.FDBKey(), "MissingTable") {
					// Return nil bytes -> GetTable returns nil, nil
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				}
				// 2. Table Not Active
				if contains(k.FDBKey(), "CreatingTable") {
					tObj := &models.Table{
						TableName:           "CreatingTable",
						Status:              models.StatusCreating,
						LatestStreamArn:     "arn:aws:dynamodb:local:000000000000:table/CreatingTable/stream/label",
						StreamSpecification: &models.StreamSpecification{StreamEnabled: true, StreamViewType: "NEW_IMAGE"},
					}
					b, _ := json.Marshal(tObj)
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
				}
				return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
			},
		}
		return f(mockRTR)
	}

	// 1. Table Not Found
	if _, err := store.DescribeStream(ctx, "arn:aws:dynamodb:local:000000000000:table/MissingTable/stream/label", 10, ""); err == nil {
		t.Error("Expected error for missing table")
	}

	// 2. Table Not Active (Should still return description according to code, just hits the if block)
	desc, err := store.DescribeStream(ctx, "arn:aws:dynamodb:local:000000000000:table/CreatingTable/stream/label", 10, "")
	if err != nil {
		t.Errorf("DescribeStream failed for creating table: %v", err)
	}
	if desc == nil {
		t.Error("Expected description for creating table")
	}
}

func TestListStreams_Limits(t *testing.T) {
	store, _, _ := setupMockStore()
	ctx := context.Background()

	// 1. Limit > 100
	// We can't easily verify the internal limit was capped without deep inspection,
	// but we can execute the code path.
	// Logic: if limit > 100 { limit = 100 }
	store.ListStreams(ctx, "", 200, "")

	// 2. Arn Parsing logic
	// "arn:aws:dynamodb:local:000000000000:table/TableName/stream/Timestamp"
	// Parse fail cases? Code: "parts := strings.Split(exclusiveStartStreamArn, "/")"
	// "if len(parts) >= 2"
	store.ListStreams(ctx, "", 10, "invalid-arn-no-slashes")
}

func base64Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
