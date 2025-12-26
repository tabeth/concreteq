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

// workerInterval usually would be config, but for MVP hardcode or allow override
var ttlWorkerInterval = 1 * time.Minute
var ttlBatchSize = 100 // items per transaction

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
	// 1. List all tables
	// This is a potentially heavy scan if there are many tables, but usually fine.
	// We need to find tables with TTL enabled.
	tables, _, err := s.ListTables(ctx, 1000, "") // Max 1000 tables for now
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
				// subspace might not exist yet if no items with TTL inserted
				return 0, nil
			}

			// Range: [0, now]
			// Key: (expiration, pk...)

			// We need a range from the start of the TTL subspace up to `now`
			// FDB tuples sort naturally.
			// Start: (0) or empty within subspace
			// End: (now) + 0xFF or just (now + epsilon)?
			// SelectorRange:
			// Begin: First key in ttlDir
			// End: FirstGreaterThan(pack(now))

			beginKey, _ := ttlDir.FDBRangeKeys() // Start of subspace
			endTuple := tuple.Tuple{now}
			endKey := ttlDir.Pack(endTuple) // Exact match for now
			// We want everything UP TO now (inclusive? DynamoDB deletes items with expiration <= now)
			// So FirstGreaterThan(now) covers now.

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
				// The prefix is ttlDir.Bytes()
				// We need to extract PK parts to call DeleteItem logic (or delete directly)
				// Deleting directly is dangerous if we miss indexes -> we should use deleteItemInternal?
				// But deleteItemInternal takes Map<String, AV>, and we have the PK tuple.
				// We can reconstruct the PK map if we know the schema.

				// Wait, to call deleteItemInternal we need table object with Request.
				// Let's get table schema first outside loop? No, inside transact is safer but costly.
				// We can just rely on deleteItemInternal's logic if we can form the key map.

				// Let's unpack.
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
					// Corrupt key? Just delete the TTL entry so we don't loop forever?
					tr.Clear(item.TTLKey)
					continue
				}

				// Call internal delete.
				// Disabling "Stream" for TTL deletes? DynamoDB says they appear in streams with distinct system identity.
				// But for MVP, just standard delete is fine, or pass a flag "isTTLDelete" if we want to be fancy.
				// Standard `deleteItemInternal` updates indexes and history.
				_, err = s.deleteItemInternal(tr, table, keyMap, "", nil, nil, "NONE")
				if err != nil {
					// log error? cancel transaction?
					// If we fail to delete item, we shouldn't clear TTL index or we lose track.
					return 0, err // Retry transaction
				}

				// Explicitly clear the TTL index entry to prevent infinite loops if deleteItemInternal
				// fails to find the item (stale index) or doesn't trigger updateTTLIndex correctly.
				// Clearing it twice (here and in updateTTLIndex) is safe/idempotent.
				tr.Clear(item.TTLKey)

				// Explicitly clear the TTL index entry?
				// `deleteItemInternal` clears ALL indexes.
				// We need to ensure `deleteItemInternal` knows about TTL index or generic index clearing covers it.
				// Currently, `deleteItemInternal` calls `updateIndexes` -> `deleteIndexEntries` (GSI/LSI only).
				// IT DOES NOT KNOW ABOUT TTL INDEX.
				// So we must manually clear the TTL entry OR teach `deleteItemInternal`/`updateIndexes` about TTL.
				// The Plan: "Modify `putItemInternal`, `deleteItemInternal`... to maintain a 'ttl' index".
				// So if I modify `deleteItemInternal` to clear TTL index, I don't need to do it here.
				// I will proceed assuming `deleteItemInternal` will handle it.
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
			// Wait, N is stored as float64 in tuple now (thanks to my integrity fix).
			// So we convert float64 back to string representation for N.
			// But what if it was S? KeySchema tells key type.
			if ke.KeyType == "HASH" || ke.KeyType == "RANGE" {
				// We need AttributeDefinition to know type?
				// KeySchema just says KEY TYPE (HASH/RANGE), not ATTRIBUTE TYPE (S/N/B).
				// We need to look up AttributeDefinition.
				attrType := "S" // Default?
				for _, ad := range table.AttributeDefinitions {
					if ad.AttributeName == ke.AttributeName {
						attrType = ad.AttributeType
						break
					}
				}

				if attrType == "N" {
					res[ke.AttributeName] = models.AttributeValue{N: &strVal}
				} else {
					// Fallback if schema says S but we got float (shouldn't happen for S)
					// Actually, if it is S, we should have gotten a string in switch?
					// But `toTupleElement` converts N to float64, S to string.
					// So if we are here (float64), it MUST be N.
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
