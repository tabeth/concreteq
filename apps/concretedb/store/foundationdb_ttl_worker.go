package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
)

const ttlWorkerInterval = 1 * time.Minute
const ttlBatchSize = 100 // items per transaction

func (s *FoundationDBStore) startTTLWorker(ctx context.Context) {
	fmt.Println("TTL Background Worker Started")
	ticker := time.NewTicker(ttlWorkerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("TTL Background Worker Stopped")
			s.workerWg.Done()
			return
		case <-ticker.C:
			s.runTTLPass(ctx)
		}
	}
}

func (s *FoundationDBStore) runTTLPass(ctx context.Context) {
	// List all tables
	// Find tables with TTL enabled.
	tables, _, err := s.ListTables(ctx, 1000, "")
	if err != nil {
		log.Printf("TTL Worker: Failed to list tables: %v", err)
		return
	}

	now := float64(time.Now().Unix())

	for _, tableName := range tables {
		if ctx.Err() != nil {
			return
		}

		// Check if TTL enabled
		desc, err := s.DescribeTimeToLive(ctx, tableName)
		if err != nil {
			continue
		}
		if desc.TimeToLiveStatus != "ENABLED" {
			continue
		}

		// Run cleanup for this table
		if err := s.cleanupExpiredItems(ctx, tableName, now); err != nil {
			log.Printf("TTL Worker: Cleanup failed for table %s: %v", tableName, err)
		}
	}
}

func (s *FoundationDBStore) cleanupExpiredItems(ctx context.Context, tableName string, now float64) error {
	for {
		// New transaction for each batch
		processedCount, err := s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
			count := 0
			// Open Directories
			ttlDir, err := s.dir.Open(tr, []string{"tables", tableName, "ttl"}, nil)
			if err != nil {
				return 0, nil
			}

			beginKey, _ := ttlDir.FDBRangeKeys()
			endTuple := tuple.Tuple{now}
			endKey := ttlDir.Pack(endTuple)

			r := fdb.SelectorRange{
				Begin: fdb.FirstGreaterOrEqual(beginKey),
				End:   fdb.FirstGreaterThan(endKey),
			}

			iter := tr.GetRange(r, fdb.RangeOptions{Limit: ttlBatchSize}).Iterator()

			type ItemToDelete struct {
				KeyTuple tuple.Tuple
				TTLKey   fdb.Key
			}
			var toDelete []ItemToDelete

			for iter.Advance() {
				kv, err := iter.Get()
				if err != nil {
					return 0, err
				}

				// Unpack key: (expiration, PK...)
				t, err := ttlDir.Unpack(kv.Key)
				if err != nil {
					continue
				}
				// t[0] is expiration. t[1:] is PK.
				if len(t) < 2 {
					continue
				}

				toDelete = append(toDelete, ItemToDelete{
					KeyTuple: t[1:],
					TTLKey:   kv.Key,
				})
				count++
			}

			if count == 0 {
				return 0, nil
			}

			// Perform Deletions
			table, err := s.getTableInternal(tr, tableName)
			if err != nil {
				return 0, err
			}

			for _, item := range toDelete {
				// Reconstruct key map from tuple
				keyMap, err := s.buildKeyMapFromTuple(table, item.KeyTuple)
				if err != nil {
					tr.Clear(item.TTLKey)
					continue
				}

				// Standard delete removes item and related index entries.
				_, err = s.deleteItemInternal(tr, table, keyMap, "", nil, nil, "NONE")
				if err != nil {
					return 0, err
				}
				tr.Clear(item.TTLKey)
			}

			return count, nil
		})

		if err != nil {
			return err
		}
		if processedCount.(int) == 0 {
			break // Done for this table
		}
		// Continue loop to process next batch
	}
	return nil
}

// Helper to reconstruct key map from tuple
func (s *FoundationDBStore) buildKeyMapFromTuple(table *models.Table, keyTuple tuple.Tuple) (map[string]models.AttributeValue, error) {
	if len(keyTuple) != len(table.KeySchema) {
		return nil, fmt.Errorf("tuple length mismatch")
	}

	res := make(map[string]models.AttributeValue)
	for i, ke := range table.KeySchema {
		elem := keyTuple[i]
		// Convert tuple element back to AV
		// Current `toTupleElement` handles S, N, B.
		// Reverse mapping:
		switch v := elem.(type) {
		case string:
			res[ke.AttributeName] = models.AttributeValue{S: &v}
		case float64:
			strVal := fmt.Sprintf("%g", v) // N is string storage
			// Convert float64 back to string representation for N.
			if ke.KeyType == "HASH" || ke.KeyType == "RANGE" {
				// Look up AttributeDefinition to determine type
				attrType := "S"
				for _, ad := range table.AttributeDefinitions {
					if ad.AttributeName == ke.AttributeName {
						attrType = ad.AttributeType
						break
					}
				}

				if attrType == "N" {
					res[ke.AttributeName] = models.AttributeValue{N: &strVal}
				} else {
					res[ke.AttributeName] = models.AttributeValue{N: &strVal}
				}
			}
		case []byte:
			// toTupleElement stores the Base64 string as []byte (without decoding).
			// So here v is the Base64 string bytes. We just need to convert back to string.
			s := string(v)
			res[ke.AttributeName] = models.AttributeValue{B: &s}
		case int64: // Should not happen for our keys unless old data
			strVal := fmt.Sprintf("%d", v)
			res[ke.AttributeName] = models.AttributeValue{N: &strVal}
		default:
			return nil, fmt.Errorf("unknown tuple type: %T", v)
		}
	}
	return res, nil
}
