package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
	"github.com/tabeth/kiroku-core/libs/fdb/directory"
)

func TestMock_AbsoluteFinal_Final(t *testing.T) {
	ctx := context.Background()
	db := &MockFDBDatabase{}
	dir := &MockDirectoryProvider{}
	store := &FoundationDBStore{db: db, dir: dir}

	t.Run("BatchWriteItem_Empty", func(t *testing.T) {
		resp, err := store.BatchWriteItem(ctx, nil)
		assert.NoError(t, err)
		assert.Empty(t, resp)
	})

	t.Run("UpdateItem_EmptyAssignment", func(t *testing.T) {
		// SET a=:v, , b=:v (to hit the continue branch)
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						sKey := string(k.FDBKey())
						if strings.Contains(sKey, "metadata") {
							table := &models.Table{TableName: "T1", KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}
							b, _ := json.Marshal(table)
							return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
						}
						// Item data
						item := map[string]models.AttributeValue{"pk": {S: stringPtr("p1")}}
						b, _ := json.Marshal(item)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					},
				},
			}
			return f(mockTR)
		}
		pk := "p1"
		_, err := store.UpdateItem(ctx, "T1", map[string]models.AttributeValue{"pk": {S: &pk}}, "SET a=:v, , b=:v", "", nil, map[string]models.AttributeValue{":v": {S: &pk}}, "")
		assert.NoError(t, err)
	})

	t.Run("DeleteTable_MetadataError", func(t *testing.T) {
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, errors.New("metadata error") }}
					},
				},
			}
			return f(mockTR)
		}
		_, err := store.DeleteTable(ctx, "T1")
		assert.Error(t, err)
	})

	t.Run("Backup_Status_Success_Paths", func(t *testing.T) {
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.CreateOrOpenFunc = func(tr fdbadapter.FDBTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						details := models.BackupDetails{BackupArn: "arn1", BackupStatus: "AVAILABLE"}
						b, _ := json.Marshal(details)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					},
				},
			}
			return f(mockTR)
		}
		// Success paths
		store.updateBackupStatus("arn1", "AVAILABLE")
		store.updateBackupCompletion("arn1", "AVAILABLE", 123)
	})

	t.Run("GlobalTables_More_Coverage", func(t *testing.T) {
		// 1. ListGlobalTables with skip everything
		db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
			dir.ListFunc = func(tr fdbadapter.FDBReadTransaction, path []string, opts directory.ListOptions) ([]string, error) {
				return []string{"GT1", "GT2"}, nil
			}
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					gt := &models.GlobalTableDescription{
						GlobalTableName:  "GT1",
						ReplicationGroup: []models.ReplicaDescription{{RegionName: "us-east-1"}},
					}
					b, _ := json.Marshal(gt)
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
				},
			}
			return f(mockTR)
		}
		_, _, err := store.ListGlobalTables(ctx, 10, "GT2") // Skip both
		assert.NoError(t, err)

		// 2. UpdateGlobalTable with existing replica
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						gt := &models.GlobalTable{
							GlobalTableName:  "GT1",
							ReplicationGroup: []models.Replica{{RegionName: "us-east-1"}},
						}
						b, _ := json.Marshal(gt)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					},
				},
			}
			return f(mockTR)
		}
		_, err = store.UpdateGlobalTable(ctx, &models.UpdateGlobalTableRequest{
			GlobalTableName: "GT1",
			ReplicaUpdates: []models.ReplicaUpdate{
				{Create: &models.CreateReplicaAction{RegionName: "us-east-1"}},
			},
		})
		assert.NoError(t, err)
	})
}
