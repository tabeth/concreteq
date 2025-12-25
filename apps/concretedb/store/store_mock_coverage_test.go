package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
)

func TestMock_Coverage_Gaps(t *testing.T) {
	ctx := context.Background()

	t.Run("RestoreTableFromBackup_MissingMetadata_Extra", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		// Mock GetTable (exists? check)
		db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
			mockTR := &MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				},
			}
			return f(mockTR)
		}

		openLogic := func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			m := &MockFDBDirectorySubspace{}
			m.PackFunc = func(t tuple.Tuple) fdb.Key {
				return fdb.Key("DIR:" + strings.Join(path, "|") + ":TUP:" + string(t.Pack()))
			}
			return m, nil
		}
		dir.OpenFunc = openLogic
		dir.CreateOrOpenFunc = func(tr fdbadapter.FDBTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
			return openLogic(tr, path, layer)
		}
		dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }

		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						sKey := string(k.FDBKey())
						if strings.Contains(sKey, "DIR:backups|metadata") && strings.Contains(sKey, "arn1") {
							dt := models.BackupDetails{
								BackupArn:    "arn1",
								BackupStatus: "AVAILABLE",
							}
							b, _ := json.Marshal(dt)
							return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
						}
						// Metadata check in backups|data|arn1
						if strings.Contains(sKey, "backups|data") && strings.Contains(sKey, "_metadata") {
							return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
						}
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
					},
				},
			}
			return f(mockTR)
		}

		_, err := store.RestoreTableFromBackup(ctx, &models.RestoreTableFromBackupRequest{
			TargetTableName: "TargetTable",
			BackupArn:       "arn1",
		})
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "Corrupted backup: missing metadata")
		}
	})

	t.Run("DescribeGlobalTable_CorruptMetadata", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return []byte("corrupt-json"), nil }}
				},
			}
			return f(mockTR)
		}

		_, err := store.DescribeGlobalTable(ctx, "GT1")
		assert.Error(t, err)
	})

	t.Run("Scan_Error_Paths", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					// table not found
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				},
			}
			return f(mockTR)
		}

		_, _, err := store.Scan(ctx, "T1", "", "", nil, nil, 10, nil, false)
		assert.Error(t, err)
	})

	t.Run("DeleteItem_Missing_Item_Coverage", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		table := &models.Table{TableName: "T1", KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}
		tableJSON, _ := json.Marshal(table)

		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) {
				if len(path) >= 2 && path[0] == "tables" && path[1] == "T1" {
					return true, nil
				}
				return false, nil
			}
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				m := &MockFDBDirectorySubspace{}
				m.PackFunc = func(t tuple.Tuple) fdb.Key {
					return fdb.Key("DIR:" + strings.Join(path, "|") + ":TUP:" + string(t.Pack()))
				}
				return m, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						sKey := string(k.FDBKey())
						if strings.Contains(sKey, "TUP:") && !strings.Contains(sKey, "metadata") {
							// Item data - not found
							return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
						}
						// Metadata
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return tableJSON, nil }}
					},
				},
			}
			return f(mockTR)
		}

		pk := "p1"
		_, err := store.DeleteItem(ctx, "T1", map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, nil, "")
		assert.NoError(t, err)
	})

	t.Run("UpdateGlobalTable_Error_Paths", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			return nil, errors.New("simulated error")
		}

		_, err := store.UpdateGlobalTable(ctx, &models.UpdateGlobalTableRequest{GlobalTableName: "G1"})
		assert.Error(t, err)
	})

	t.Run("ListGlobalTables_Empty", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBReadTransaction{
				GetRangeFunc: func(r fdb.Range, opts fdb.RangeOptions) fdbadapter.FDBRangeResult {
					return &MockFDBRangeResult{}
				},
			}
			return f(mockTR)
		}

		_, _, err := store.ListGlobalTables(ctx, 10, "")
		assert.NoError(t, err)
	})

	t.Run("Aggressive_Error_Paths_Extra", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		// 1. deleteIndexEntries error path
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				if strings.Contains(strings.Join(path, "/"), "index/GSI1") {
					return nil, errors.New("simulated dir open error")
				}
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						table := &models.Table{
							TableName: "T1",
							KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
							GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
								{IndexName: "GSI1", KeySchema: []models.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}}},
							},
						}
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					},
				},
			}
			return f(mockTR)
		}
		pk := "p1"
		g := "g1"
		_, err := store.DeleteItem(ctx, "T1", map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, map[string]models.AttributeValue{"pk": {S: &pk}, "g": {S: &g}}, "")
		assert.Error(t, err)

		// 2. DescribeGlobalTable FDB error path
		db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, errors.New("fdb error") }}
				},
			}
			return f(mockTR)
		}
		_, err = store.DescribeGlobalTable(ctx, "GT1")
		assert.Error(t, err)

		// 3. updateBackupStatus error path
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.CreateOrOpenFunc = func(tr fdbadapter.FDBTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return nil, errors.New("dir error")
			}
			return f(&MockFDBTransaction{})
		}
		store.updateBackupStatus("arn1", "AVAILABLE") // should just return
	})

	t.Run("EvenMore_Aggressive_Error_Paths_Extra", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		// 1. CreateBackup getBackupMetadataDir error
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.CreateOrOpenFunc = func(tr fdbadapter.FDBTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				if strings.Contains(strings.Join(path, "/"), "backups/metadata") {
					return nil, errors.New("metadata dir error")
				}
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						table := &models.Table{TableName: "T1", KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					},
				},
			}
			return f(mockTR)
		}
		_, err := store.CreateBackup(ctx, &models.CreateBackupRequest{TableName: "T1", BackupName: "B1"})
		assert.Error(t, err)

		// 2. getItemInternal metadata Get error
		db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, errors.New("metadata get error") }}
				},
			}
			return f(mockTR)
		}
		pk := "p1"
		_, err = store.GetItem(ctx, "T1", map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, false)
		assert.Error(t, err)
	})

	t.Run("FinalPush_Error_Paths_Extra", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		// 1. DescribeGlobalTable data == nil (Not Found after directory exists)
		db.ReadTransactFunc = func(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBReadTransaction{
				GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
					return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
				},
			}
			return f(mockTR)
		}
		_, err := store.DescribeGlobalTable(ctx, "GT1")
		assert.Error(t, err)

		// 2. UpdateGlobalTable not found
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return false, nil }
			return f(&MockFDBTransaction{})
		}
		_, err = store.UpdateGlobalTable(ctx, &models.UpdateGlobalTableRequest{GlobalTableName: "GT1"})
		assert.Error(t, err)

		// 3. UpdateGlobalTable metadata Get error
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, errors.New("fdb error") }}
					},
				},
			}
			return f(mockTR)
		}
		_, err = store.UpdateGlobalTable(ctx, &models.UpdateGlobalTableRequest{GlobalTableName: "GT1"})
		assert.Error(t, err)
	})

	t.Run("AbsoluteFinal_Error_Paths_Extra", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		// 1. deleteIndexEntries LSI error path
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				if strings.Contains(strings.Join(path, "/"), "index/LSI1") {
					return nil, errors.New("simulated LSI dir open error")
				}
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						table := &models.Table{
							TableName: "T1",
							KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
							LocalSecondaryIndexes: []models.LocalSecondaryIndex{
								{IndexName: "LSI1", KeySchema: []models.KeySchemaElement{
									{AttributeName: "pk", KeyType: "HASH"},
									{AttributeName: "sk", KeyType: "RANGE"},
								}},
							},
						}
						b, _ := json.Marshal(table)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					},
				},
			}
			return f(mockTR)
		}
		pk := "p1"
		sk := "s1"
		_, err := store.DeleteItem(ctx, "T1", map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, map[string]models.AttributeValue{"pk": {S: &pk}, "sk": {S: &sk}}, "")
		assert.Error(t, err)

		// 2. RestoreTableToPointInTime source table not found internal
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			return nil, errors.New("transact error")
		}
		_, err = store.RestoreTableToPointInTime(ctx, &models.RestoreTableToPointInTimeRequest{
			SourceTableName: "S1",
			TargetTableName: "T1",
			RestoreDateTime: float64(time.Now().Unix()),
		})
		assert.Error(t, err)

		// 3. backup completion error path
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.CreateOrOpenFunc = func(tr fdbadapter.FDBTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return nil, errors.New("dir error")
			}
			return f(&MockFDBTransaction{})
		}
		store.updateBackupCompletion("arn1", "AVAILABLE", 1)
	})

	t.Run("ShardIterator_And_Gaps_Extra", func(t *testing.T) {
		db := &MockFDBDatabase{}
		dir := &MockDirectoryProvider{}
		store := &FoundationDBStore{db: db, dir: dir}

		// 1. GetShardIterator success
		it, err := store.GetShardIterator(ctx, "arn:aws:dynamodb:us-east-1:123456789012:table/T1/stream/2023-01-01T00:00:00.000", "shard1", "TRIM_HORIZON", "")
		assert.NoError(t, err)
		assert.NotEmpty(t, it)

		// 2. GetRecords invalid iterator
		_, _, err = store.GetRecords(ctx, "invalid-base64", 10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid ShardIterator")

		_, _, err = store.GetRecords(ctx, "bm90LWpzb24=", 10) // "not-json" base64
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid ShardIterator JSON")

		// 3. writeHistoryRecord corrupt config
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return []byte("corrupt-json"), nil }}
					},
				},
			}
			return f(mockTR)
		}
		table := &models.Table{TableName: "T1", KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}
		item := map[string]models.AttributeValue{"pk": {S: stringPtr("p1")}}
		err = store.writeHistoryRecord(&MockFDBTransaction{}, table, item, false)
		assert.NoError(t, err) // Should skip corrupt config

		// 4. DeleteTable inconsistent metadata
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.ExistsFunc = func(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) { return true, nil }
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return nil, nil }}
					},
				},
			}
			return f(mockTR)
		}
		_, err = store.DeleteTable(ctx, "T1")
		assert.Error(t, err)
		assert.Equal(t, ErrTableNotFound, err)

		// 5. writeHistoryRecord DISABLED status
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.OpenFunc = func(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						config := models.PointInTimeRecoveryDescription{PointInTimeRecoveryStatus: "DISABLED"}
						b, _ := json.Marshal(config)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					},
				},
			}
			return f(mockTR)
		}
		err = store.writeHistoryRecord(&MockFDBTransaction{}, table, item, false)
		assert.NoError(t, err)

		// 6. DeleteBackup directory remove error
		db.TransactFunc = func(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
			dir.CreateOrOpenFunc = func(tr fdbadapter.FDBTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
				return &MockFDBDirectorySubspace{}, nil
			}
			dir.RemoveFunc = func(tr fdbadapter.FDBTransaction, path []string) (bool, error) {
				if strings.Contains(strings.Join(path, "/"), "backups/data") {
					return false, errors.New("remove error")
				}
				return true, nil
			}
			mockTR := &MockFDBTransaction{
				MockFDBReadTransaction: MockFDBReadTransaction{
					GetFunc: func(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
						det := &models.BackupDetails{BackupArn: "arn1"}
						b, _ := json.Marshal(det)
						return &MockFDBFutureByteSlice{GetFunc: func() ([]byte, error) { return b, nil }}
					},
				},
			}
			return f(mockTR)
		}
		_, err = store.DeleteBackup(ctx, "arn1")
		assert.Error(t, err)

		// 7. toFDBElement unsupported type
		_, err = toFDBElement(models.AttributeValue{BOOL: boolPtr(true)})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported index key type")

		// 8. buildIndexKeyTuple missing table key
		table2 := &models.Table{
			TableName: "T2",
			KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		}
		item2 := map[string]models.AttributeValue{"not-pk": {S: stringPtr("v")}}
		_, err = store.buildIndexKeyTuple(table2, []models.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}}, item2)
		assert.Error(t, err)
		assert.Equal(t, ErrSkipIndex, err)

		// 9. cloneItem nil
		assert.Nil(t, cloneItem(nil))
	})
}
