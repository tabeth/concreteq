package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
)

// updateIndexes maintains GSI and LSI entries when an item is created, updated, or deleted.
func (s *FoundationDBStore) updateIndexes(tr fdbadapter.FDBTransaction, table *models.Table, oldItem map[string]models.AttributeValue, newItem map[string]models.AttributeValue) error {
	// 1. Delete old index entries
	if len(oldItem) > 0 {
		if err := s.deleteIndexEntries(tr, table, oldItem); err != nil {
			return err
		}
	}

	// 2. Put new index entries
	if len(newItem) > 0 {
		if err := s.putIndexEntries(tr, table, newItem); err != nil {
			return err
		}
	}

	return nil
}

func (s *FoundationDBStore) deleteIndexEntries(tr fdbadapter.FDBTransaction, table *models.Table, item map[string]models.AttributeValue) error {
	// GSI
	for _, gsi := range table.GlobalSecondaryIndexes {
		if err := s.deleteIndexEntry(tr, table, gsi.IndexName, gsi.KeySchema, item); err != nil {
			return err
		}
	}
	// LSI
	for _, lsi := range table.LocalSecondaryIndexes {
		if err := s.deleteIndexEntry(tr, table, lsi.IndexName, lsi.KeySchema, item); err != nil {
			return err
		}
	}
	return nil
}

func (s *FoundationDBStore) putIndexEntries(tr fdbadapter.FDBTransaction, table *models.Table, item map[string]models.AttributeValue) error {
	// GSI
	for _, gsi := range table.GlobalSecondaryIndexes {
		if err := s.putIndexEntry(tr, table, gsi.IndexName, gsi.KeySchema, gsi.Projection, item); err != nil {
			return err
		}
	}
	// LSI
	for _, lsi := range table.LocalSecondaryIndexes {
		if err := s.putIndexEntry(tr, table, lsi.IndexName, lsi.KeySchema, lsi.Projection, item); err != nil {
			return err
		}
	}
	return nil
}

func (s *FoundationDBStore) deleteIndexEntry(tr fdbadapter.FDBTransaction, table *models.Table, indexName string, keySchema []models.KeySchemaElement, item map[string]models.AttributeValue) error {
	indexKey, err := s.buildIndexKeyTuple(table, keySchema, item)
	if err != nil {
		if err == ErrSkipIndex {
			return nil
		}
		return err
	}

	indexDir, err := s.dir.Open(tr, []string{"tables", table.TableName, "index", indexName}, nil)
	if err != nil {
		return err
	}

	tr.Clear(indexDir.Pack(indexKey))
	return nil
}

func (s *FoundationDBStore) putIndexEntry(tr fdbadapter.FDBTransaction, table *models.Table, indexName string, keySchema []models.KeySchemaElement, projection models.Projection, item map[string]models.AttributeValue) error {
	indexKey, err := s.buildIndexKeyTuple(table, keySchema, item)
	if err != nil {
		if err == ErrSkipIndex {
			return nil // Skip indexing if attributes missing
		}
		// Propagate other errors (e.g. invalid type)
		return err
	}

	indexDir, err := s.dir.Open(tr, []string{"tables", table.TableName, "index", indexName}, nil)
	if err != nil {
		return err
	}

	// Project attributes
	var val []byte
	if projection.ProjectionType == "KEYS_ONLY" {
		val = []byte("{}")
	} else {
		// Project attributes
		projected := make(map[string]models.AttributeValue)

		if projection.ProjectionType == "ALL" {
			projected = item
		} else if projection.ProjectionType == "INCLUDE" {
			// Copy keys + included
			for _, ks := range table.KeySchema {
				if v, ok := item[ks.AttributeName]; ok {
					projected[ks.AttributeName] = v
				}
			}
			for _, n := range projection.NonKeyAttributes {
				if v, ok := item[n]; ok {
					projected[n] = v
				}
			}
		}

		if len(projected) > 0 {
			b, err := json.Marshal(projected)
			if err != nil {
				return err
			}
			val = b
		}
	}

	tr.Set(indexDir.Pack(indexKey), val)
	return nil
}

var ErrSkipIndex = fmt.Errorf("skip index")

// buildIndexKeyTuple constructs the tuple key for an index entry.
func (s *FoundationDBStore) buildIndexKeyTuple(table *models.Table, indexKeySchema []models.KeySchemaElement, item map[string]models.AttributeValue) (tuple.Tuple, error) {
	// Tuple: (IndexPK, [IndexSK], TablePK, [TableSK])
	var parts tuple.Tuple

	// 1. Index Keys
	for _, ke := range indexKeySchema {
		val, ok := item[ke.AttributeName]
		if !ok {
			return nil, ErrSkipIndex
		}
		// Convert val to comparable
		v, err := toFDBElement(val)
		if err != nil {
			return nil, err
		}
		parts = append(parts, v)
	}

	// 2. Table Keys (to ensure uniqueness)
	// We append TablePK (and TableSK if exists) to the tuple.
	// BUT, if the Index Key ALREADY includes Table Key (e.g. LSI shares PK), we shouldn't duplicate it?
	// FDB logic: The tuple forms the key.
	// GSI: (GSI_PK, GSI_SK, TablePK, TableSK).
	// LSI: (TablePK, LSI_SK, TableSK). (TablePK is already in indexKeySchema[0]).

	// Optimization: Append Table Keys ONLY if they are not already in Index Keys?
	// Or just blindly append them? strict deduplication?
	// DynamoDB GSI keys are (Partition Key, Sort Key, Table Partition Key, Table Sort Key).
	// If any overlap, they are merged? No, generally they are distinct unless same name.

	for _, ke := range table.KeySchema {
		// Check if already in parts?
		// Simple approach: Always append table keys.
		// For LSI: indexKeySchema[0] IS TablePK. So we duplicate it?
		// (TablePK, LSI_SK, TablePK, TableSK). This is redundant but works for uniqueness.
		// Better: Check if attribute name is already in indexKeySchema.

		isInData := false
		for _, ike := range indexKeySchema {
			if ike.AttributeName == ke.AttributeName {
				isInData = true
				break
			}
		}

		if !isInData {
			val, ok := item[ke.AttributeName]
			if !ok {
				return nil, fmt.Errorf("missing table key attribute")
			}
			v, err := toFDBElement(val)
			if err != nil {
				return nil, err
			}
			parts = append(parts, v)
		}
	}

	return parts, nil
}

// toFDBElement converts AttributeValue to tuple element
func toFDBElement(v models.AttributeValue) (tuple.TupleElement, error) {
	if v.S != nil {
		return *v.S, nil
	}
	if v.N != nil {
		var f float64
		if _, err := fmt.Sscanf(*v.N, "%f", &f); err != nil {
			return nil, fmt.Errorf("invalid number format: %s", *v.N)
		}
		return f, nil
	}
	if v.B != nil {
		return []byte(*v.B), nil
	}
	return nil, fmt.Errorf("unsupported index key type")
}

// backfillIndex scans the table and adds entries for the new index.
// This runs in a background goroutine.
func (s *FoundationDBStore) backfillIndex(ctx context.Context, tableName string, indexName string) {
	// 1. Get Table Schema (for projection/key schema)
	// We need to do this in a transaction.
	var table *models.Table
	val, err := s.db.ReadTransact(func(tr fdbadapter.FDBReadTransaction) (interface{}, error) {
		res, err := s.getTableInternal(tr, tableName)
		return res, err
	})
	if err != nil {
		fmt.Printf("Failed to get table for backfill: %v\n", err)
		return
	}
	if val != nil {
		table = val.(*models.Table)
	}
	if table == nil {
		// Table not found or deleted
		return
	}

	// Find the target index definition
	var targetGSI *models.GlobalSecondaryIndex
	for _, gsi := range table.GlobalSecondaryIndexes {
		if gsi.IndexName == indexName {
			targetGSI = &gsi
			break
		}
	}

	if targetGSI == nil {
		fmt.Printf("Index %s not found in metadata for backfill\n", indexName)
		return
	}

	// 2. Scan All Items
	// Similar logic to Scan, but simple full table iteration.
	// We use the "data" subspace.

	startKey := []byte{} // Start from beginning
	batchSize := 100

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			fmt.Printf("Backfill for %s cancelled\n", indexName)
			return
		default:
		}

		// Transaction for batch
		lastRead, err := s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
			count := 0
			var lastKey []byte

			tableDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
			if err != nil {
				return nil, err
			}
			dataDir := tableDir.Sub(tuple.Tuple{"data"})

			// Construct range
			var r fdb.Range
			if len(startKey) == 0 {
				r = dataDir
			} else {
				_, end := dataDir.FDBRangeKeys()
				r = fdb.SelectorRange{
					Begin: fdb.FirstGreaterThan(fdb.Key(startKey)),
					End:   fdb.FirstGreaterOrEqual(end),
				}
			}

			iter := tr.GetRange(r, fdb.RangeOptions{Limit: batchSize}).Iterator()

			for iter.Advance() {
				kv, err := iter.Get()
				if err != nil {
					return nil, err
				}

				// Ensure key is within dataSubspace
				if !dataDir.Contains(kv.Key) {
					break
				}

				var item map[string]models.AttributeValue
				if err := json.Unmarshal(kv.Value, &item); err != nil {
					fmt.Printf("Backfill: Unmarshal error for key %v: %v\n", kv.Key, err)
					continue
				}

				if err := s.putIndexEntry(tr, table, targetGSI.IndexName, targetGSI.KeySchema, targetGSI.Projection, item); err != nil {
					if err != ErrSkipIndex {
						fmt.Printf("Backfill: putIndexEntry error: %v\n", err)
					}
				}

				lastKey = kv.Key
				count++
			}
			return lastKey, nil
		})

		if err != nil {
			fmt.Printf("Backfill error: %v\n", err)
			return // Retry? simplified for now.
		}

		if lastRead == nil {
			break // Done
		}

		lk := lastRead.([]byte)
		if len(lk) == 0 {
			break
		}
		startKey = lk
	}

	// 4. Update Status to ACTIVE
	_, err = s.db.Transact(func(tr fdbadapter.FDBTransaction) (interface{}, error) {
		// Refresh table metadata to avoid overwriting other updates
		t, err := s.getTableInternal(tr, tableName)
		if err != nil {
			return nil, err
		}
		if t == nil {
			return nil, ErrTableNotFound
		}

		// Update status
		found := false
		for i, gsi := range t.GlobalSecondaryIndexes {
			if gsi.IndexName == indexName {
				t.GlobalSecondaryIndexes[i].IndexStatus = "ACTIVE"
				found = true
				break
			}
		}

		if found {
			return s.saveTableInternal(tr, t)
		}
		return nil, nil
	})

	if err != nil {
		fmt.Printf("Failed to set index ACTIVE: %v\n", err)
	} else {
		// fmt.Printf("Backfill complete for %s\n", indexName)
	}
}

// saveTableInternal helper (if not exists, we use existing logic or inline it)
func (s *FoundationDBStore) saveTableInternal(tr fdbadapter.FDBTransaction, table *models.Table) (interface{}, error) {
	tableBytes, err := json.Marshal(table)
	if err != nil {
		return nil, err
	}
	tableDir, err := s.dir.Open(tr, []string{"tables", table.TableName}, nil)
	if err != nil {
		return nil, err
	}
	tr.Set(tableDir.Pack(tuple.Tuple{"metadata"}), tableBytes)
	return nil, nil
}
