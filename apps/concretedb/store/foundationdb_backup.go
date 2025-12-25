package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
)

// Helpers
func serialize(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func deserialize(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// CreateBackup creates a backup of the specified table.
func (s *FoundationDBStore) CreateBackup(ctx context.Context, request *models.CreateBackupRequest) (*models.BackupDetails, error) {
	// 1. Basic Validation
	table, err := s.GetTable(ctx, request.TableName)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, models.New("ResourceNotFoundException", "Table not found")
	}

	backupId := uuid.New().String()
	backupArn := fmt.Sprintf("arn:aws:dynamodb:us-east-1:123456789012:table/%s/backup/%s", request.TableName, backupId)
	now := float64(time.Now().Unix())

	details := &models.BackupDetails{
		BackupArn:              backupArn,
		BackupName:             request.BackupName,
		BackupStatus:           "CREATING",
		BackupType:             "USER",
		BackupCreationDateTime: now,
		BackupSizeBytes:        0,
	}

	// 2. Store Metadata (Sync)
	_, err = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		backupDir, err := s.getBackupMetadataDir(tr)
		if err != nil {
			return nil, err
		}

		data, err := serialize(details)
		if err != nil {
			return nil, err
		}

		tr.Set(backupDir.Pack(tuple.Tuple{backupArn}), data)
		return nil, nil
	})
	if err != nil {
		return nil, err
	}

	// 3. Async Backup Process
	go s.performBackupAsyncCorrect(request.TableName, backupArn, table)

	return details, nil
}

// Fixed Async Backup
func (s *FoundationDBStore) performBackupAsyncCorrect(tableName string, backupArn string, sourceTable *models.Table) {
	res, err := s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		tableDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}

		backupDataDir, err := s.getBackupDataDir(tr, backupArn)
		if err != nil {
			return nil, err
		}

		metaDataBytes, err := serialize(sourceTable)
		if err != nil {
			return nil, err
		}
		tr.Set(backupDataDir.Pack(tuple.Tuple{"_metadata"}), metaDataBytes)

		totalBytes := int64(len(metaDataBytes))

		// Fix Range Calculation: FDBRangeKeys returns (begin, end) for the directory subspace
		// But tableDir is likely /root/content/tables/NAME
		// We want to scan everything under that.
		beginKey, endKey := tableDir.FDBRangeKeys()
		r := fdb.KeyRange{Begin: beginKey, End: endKey}

		iter := tr.GetRange(r, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()

		count := 0
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			srcPrefix := tableDir.Bytes()
			if len(kv.Key) < len(srcPrefix) {
				continue
			}
			suffix := kv.Key[len(srcPrefix):]

			destKey := append(backupDataDir.Bytes(), suffix...)
			tr.Set(fdb.Key(destKey), kv.Value)

			totalBytes += int64(len(kv.Value) + len(destKey))
			count++
		}
		fmt.Printf("Backup Total Bytes: %d, Count: %d\n", totalBytes, count)
		return totalBytes, nil
	})

	if err != nil {
		fmt.Printf("Backup failed: %v\n", err)
		s.updateBackupStatus(backupArn, "DELETED")
		return
	}

	totalBytes := res.(int64)
	s.updateBackupCompletion(backupArn, "AVAILABLE", totalBytes)
}

func (s *FoundationDBStore) updateBackupStatus(backupArn string, status string) {
	_, _ = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		backupDir, err := s.getBackupMetadataDir(tr)
		if err != nil {
			return nil, err
		}

		key := backupDir.Pack(tuple.Tuple{backupArn})
		valBytes, err := tr.Get(key).Get()
		if err != nil || valBytes == nil {
			return nil, err
		}

		var details models.BackupDetails
		if err := deserialize(valBytes, &details); err != nil {
			return nil, err
		}

		details.BackupStatus = status
		newData, _ := serialize(details)
		tr.Set(key, newData)
		return nil, nil
	})
}

func (s *FoundationDBStore) updateBackupCompletion(backupArn string, status string, sizeBytes int64) {
	_, _ = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		backupDir, err := s.getBackupMetadataDir(tr)
		if err != nil {
			return nil, err
		}

		key := backupDir.Pack(tuple.Tuple{backupArn})
		valBytes, err := tr.Get(key).Get()
		if err != nil || valBytes == nil {
			return nil, err
		}

		var details models.BackupDetails
		if err := deserialize(valBytes, &details); err != nil {
			return nil, err
		}

		details.BackupStatus = status
		details.BackupSizeBytes = sizeBytes
		newData, _ := serialize(details)
		tr.Set(key, newData)
		return nil, nil
	})
}

// RestoreTableFromBackup creates a new table from a backup.
func (s *FoundationDBStore) RestoreTableFromBackup(ctx context.Context, request *models.RestoreTableFromBackupRequest) (*models.TableDescription, error) {
	exists, err := s.GetTable(ctx, request.TargetTableName)
	if err != nil {
		return nil, err
	}
	if exists != nil {
		return nil, models.New("ResourceInUseException", "Table already exists")
	}

	var backupDetails models.BackupDetails
	var msg string

	_, err = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		backupMetaDir, err := s.getBackupMetadataDir(tr)
		if err != nil {
			return nil, err
		}
		key := backupMetaDir.Pack(tuple.Tuple{request.BackupArn})
		valBytes, err := tr.Get(key).Get()
		if err != nil || valBytes == nil {
			msg = "Backup not found"
			return nil, nil
		}
		return nil, deserialize(valBytes, &backupDetails)
	})

	if msg != "" {
		return nil, models.New("ResourceNotFoundException", msg)
	}
	if err != nil {
		return nil, err
	}

	if backupDetails.BackupStatus != "AVAILABLE" {
		return nil, models.New("BackupInUseException", "Backup is not available")
	}

	var sourceTable models.Table
	_, err = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		backupDataDir, err := s.getBackupDataDir(tr, request.BackupArn)
		if err != nil {
			return nil, err
		}

		valBytes, err := tr.Get(backupDataDir.Pack(tuple.Tuple{"_metadata"})).Get()
		if err != nil || valBytes == nil {
			msg = "Corrupted backup: missing metadata"
			return nil, nil
		}
		return nil, deserialize(valBytes, &sourceTable)
	})
	if msg != "" {
		return nil, models.New("InternalServerError", msg)
	}
	if err != nil {
		return nil, err
	}

	newTable := sourceTable
	newTable.TableName = request.TargetTableName
	newTable.CreationDateTime = time.Now()
	newTable.Status = models.StatusCreating

	err = s.CreateTable(ctx, &newTable)
	if err != nil {
		return nil, err
	}

	go s.performRestoreAsync(request.TargetTableName, request.BackupArn)

	desc := &models.TableDescription{
		TableName:             newTable.TableName,
		TableStatus:           "CREATING",
		AttributeDefinitions:  newTable.AttributeDefinitions,
		KeySchema:             newTable.KeySchema,
		ProvisionedThroughput: newTable.ProvisionedThroughput,
		TableSizeBytes:        0,
		ItemCount:             0,
		CreationDateTime:      float64(newTable.CreationDateTime.Unix()),
	}
	return desc, nil
}

func (s *FoundationDBStore) performRestoreAsync(tableName string, backupArn string) {
	_, err := s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		backupDataDir, err := s.getBackupDataDir(tr, backupArn)
		if err != nil {
			return nil, err
		}

		targetDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}

		beginKey, endKey := backupDataDir.FDBRangeKeys()
		r := fdb.KeyRange{Begin: beginKey, End: endKey}
		iter := tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()

		count := 0
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			backupPrefix := backupDataDir.Bytes()
			if len(kv.Key) < len(backupPrefix) {
				continue
			}
			suffix := kv.Key[len(backupPrefix):]

			t, err := tuple.Unpack(suffix)
			if err == nil && len(t) > 0 {
				if str, ok := t[0].(string); ok {
					if str == "_metadata" || str == "metadata" {
						continue
					}
				}
			}

			destKey := append(targetDir.Bytes(), suffix...)
			tr.Set(fdb.Key(destKey), kv.Value)
			count++
		}
		return nil, nil
	})

	if err != nil {
		fmt.Printf("Restore failed: %v\n", err)
		return
	}

	// Update to Active
	s.setTableStatus(tableName, models.StatusActive)
}

func (s *FoundationDBStore) setTableStatus(tableName string, status models.TableStatus) {
	_, _ = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		dir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}

		metaKey := dir.Pack(tuple.Tuple{"metadata"})
		valBytes, err := tr.Get(metaKey).Get()
		if err != nil || valBytes == nil {
			return nil, nil
		}

		var table models.Table
		deserialize(valBytes, &table)
		table.Status = status

		d, _ := serialize(table)
		tr.Set(metaKey, d)
		return nil, nil
	})
}

// Helpers
func (s *FoundationDBStore) getBackupMetadataDir(tr fdbadapter.FDBTransaction) (fdbadapter.FDBDirectorySubspace, error) {
	return s.dir.CreateOrOpen(tr, []string{"backups", "metadata"}, nil)
}

func (s *FoundationDBStore) getBackupDataDir(tr fdbadapter.FDBTransaction, backupArn string) (fdbadapter.FDBDirectorySubspace, error) {
	return s.dir.CreateOrOpen(tr, []string{"backups", "data", backupArn}, nil)
}

func (s *FoundationDBStore) ListBackups(ctx context.Context, request *models.ListBackupsRequest) ([]models.BackupSummary, string, error) {
	summaries := []models.BackupSummary{}
	var lastKey string

	_, err := s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		backupMetaDir, err := s.getBackupMetadataDir(tr)
		if err != nil {
			return nil, err
		}

		beginKey, endKey := backupMetaDir.FDBRangeKeys()
		r := fdb.KeyRange{Begin: beginKey, End: endKey}

		if request.ExclusiveStartBackupArn != "" {
			startKey := backupMetaDir.Pack(tuple.Tuple{request.ExclusiveStartBackupArn})
			r.Begin = fdb.Key(append(startKey, 0x00))
		}

		// Iterate
		iter := tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()

		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			var details models.BackupDetails
			if err := deserialize(kv.Value, &details); err != nil {
				continue
			}

			// Filter by TableName if requested
			if request.TableName != "" {
				expectedSegment := fmt.Sprintf("table/%s/backup/", request.TableName)
				if !strings.Contains(details.BackupArn, expectedSegment) {
					continue
				}
			}

			// Time Range Filtering (if implemented, but ignoring for now as per test)

			summaries = append(summaries, models.BackupSummary{
				BackupArn:              details.BackupArn,
				BackupName:             details.BackupName,
				BackupStatus:           details.BackupStatus,
				BackupType:             details.BackupType,
				BackupCreationDateTime: details.BackupCreationDateTime,
				BackupSizeBytes:        details.BackupSizeBytes,
				// TableName: ... we could extract it back
			})

			lastKey = details.BackupArn

			if request.Limit > 0 && len(summaries) >= request.Limit {
				break
			}
		}

		return nil, nil
	})

	if err != nil {
		return nil, "", err
	}

	var lastEvaluatedBackupArn string
	if request.Limit > 0 && len(summaries) >= request.Limit {
		lastEvaluatedBackupArn = lastKey
	}

	return summaries, lastEvaluatedBackupArn, nil
}

func (s *FoundationDBStore) DeleteBackup(ctx context.Context, backupArn string) (*models.BackupDescription, error) {
	desc, err := s.DescribeBackup(ctx, backupArn)
	if err != nil {
		return nil, err
	}

	_, err = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		metaDir, err := s.getBackupMetadataDir(tr)
		if err != nil {
			return nil, err
		}
		tr.Clear(metaDir.Pack(tuple.Tuple{backupArn}))

		s.dir.Remove(tr, []string{"backups", "data", backupArn})
		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	desc.BackupDetails.BackupStatus = "DELETED"
	return desc, nil
}

func (s *FoundationDBStore) DescribeBackup(ctx context.Context, backupArn string) (*models.BackupDescription, error) {
	var details models.BackupDetails
	var found bool

	_, err := s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		metaDir, err := s.getBackupMetadataDir(tr)
		if err != nil {
			return nil, err
		}
		valBytes, err := tr.Get(metaDir.Pack(tuple.Tuple{backupArn})).Get()
		if err != nil || valBytes == nil {
			return nil, nil
		}
		found = true
		return nil, deserialize(valBytes, &details)
	})

	if err != nil {
		return nil, err
	}
	if !found {
		return nil, models.New("ResourceNotFoundException", "Backup not found")
	}

	return &models.BackupDescription{
		BackupDetails: details,
	}, nil
}
