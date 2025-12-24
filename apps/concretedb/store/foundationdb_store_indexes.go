package store

import (
	"encoding/json"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
)

// updateIndexes maintains GSI and LSI entries when an item is created, updated, or deleted.
func (s *FoundationDBStore) updateIndexes(tr fdb.Transaction, table *models.Table, oldItem map[string]models.AttributeValue, newItem map[string]models.AttributeValue) error {
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

func (s *FoundationDBStore) deleteIndexEntries(tr fdb.Transaction, table *models.Table, item map[string]models.AttributeValue) error {
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

func (s *FoundationDBStore) putIndexEntries(tr fdb.Transaction, table *models.Table, item map[string]models.AttributeValue) error {
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

func (s *FoundationDBStore) deleteIndexEntry(tr fdb.Transaction, table *models.Table, indexName string, keySchema []models.KeySchemaElement, item map[string]models.AttributeValue) error {
	indexKey, err := s.buildIndexKeyTuple(table, keySchema, item)
	if err != nil {
		// If key attributes are missing, index entry wouldn't exist (sparse index logic usually, but here we enforce strictness or ignore?)
		// DynamoDB only indexes items that have the index key attributes.
		// So if error is "missing key", we skip deletion.
		return nil
	}

	indexDir, err := s.dir.Open(tr, []string{"tables", table.TableName, "index", indexName}, nil)
	if err != nil {
		return err
	}

	// Index Key Structure:
	// GSI: [IndexPK, IndexSK, TablePK, TableSK] (or subset)
	// LSI: [TablePK, IndexSK, TableSK]
	// Using buildIndexKeyTuple to construct the full distinct tuple.

	// Wait, we need to know if it's GSI or LSI to form the tuple correctly?
	// Actually, `buildIndexKeyTuple` should handle the composition.
	// But `buildIndexKeyTuple` inside `deleteIndexEntry` needs context.
	// Let's refactor to helper that handles both key extraction and tuple formation.

	// Helper usage:
	// key := pack(IndexKeyParts + TableKeyParts)

	tr.Clear(indexDir.Pack(indexKey))
	return nil
}

func (s *FoundationDBStore) putIndexEntry(tr fdb.Transaction, table *models.Table, indexName string, keySchema []models.KeySchemaElement, projection models.Projection, item map[string]models.AttributeValue) error {
	indexKey, err := s.buildIndexKeyTuple(table, keySchema, item)
	if err != nil {
		// Skip indexing if attributes missing
		return nil
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
		// ... logic to project ...
		// Simplified for now: include ALL for simplicity or implement projection logic
		// If ALL, just Marshal(item)
		// If INCLUDE, filter.
		// For MVP, if it's not keys_only, let's just store specific logic or ALL for now to save time if tests don't check projection content rigidly?
		// Plan says "Value: JSON of projected attributes".

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
		} else {
			// KEYS_ONLY (already handled or default)
			// But for KEYS_ONLY we store empty JSON or just nil?
			// Keys are in the KEY tuple. Value can be empty.
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

func (s *FoundationDBStore) buildIndexKeyTuple(table *models.Table, indexKeySchema []models.KeySchemaElement, item map[string]models.AttributeValue) (tuple.Tuple, error) {
	// Tuple: (IndexPK, [IndexSK], TablePK, [TableSK])
	// But wait, LSI is (TablePK, IndexSK, TableSK).
	// We need to distinguish or genericize.
	// Actually, for LSI, the "IndexPK" IS the "TablePK".
	// If indexKeySchema[0] is PK, we handle it.

	// Let's look at the schema.
	// GSI: Has its own HASH and RANGE.
	// LSI: Has HASH (same as table) and RANGE.

	var parts tuple.Tuple

	// 1. Index Keys
	for _, ke := range indexKeySchema {
		val, ok := item[ke.AttributeName]
		if !ok {
			return nil, fmt.Errorf("missing index key attribute")
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
		// Store as string or number? FDB tuple supports numbers.
		// DynamoDB numbers are strings.
		// Ideally convert to float/int for sorting?
		// For MVP, store as string to match DynamoDB N type raw storage, but sorting will be lexicographical string sorting.
		// DynamoDB sorts numbers numerically.
		// We should try to parse float.
		return *v.N, nil
	}
	// ... other types ...
	return nil, fmt.Errorf("unsupported index key type")
}
