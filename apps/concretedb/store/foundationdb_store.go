package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/kiroku-core/libs/fdb/directory"
)

// FoundationDBStore implements the Store interface using FoundationDB.
type FoundationDBStore struct {
	db  fdb.Database
	dir directory.Directory
}

// NewFoundationDBStore creates a new store connected to FoundationDB.
func NewFoundationDBStore(db fdb.Database) *FoundationDBStore {
	fmt.Println("Creating FDB store.")
	return &FoundationDBStore{
		db: db,
		// Initialize Directory Layer with a prefix for isolation
		dir: directory.NewDirectoryLayer(subspace.Sub(tuple.Tuple{"concretedb"}), subspace.Sub(tuple.Tuple{"content"}), true),
	}
}

// CreateTable persists a new table's metadata within a FoundationDB transaction.
func (s *FoundationDBStore) CreateTable(ctx context.Context, table *models.Table) error {
	fmt.Println("Creating table :", table.TableName)
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Simulating instant provisioning for ConcreteDB
		table.Status = models.StatusActive

		// Marshal the table metadata to JSON
		tableBytes, err := json.Marshal(table)
		if err != nil {
			return nil, err
		}

		// Create or Open the directory for this table
		// Path: ["tables", tableName]
		// This allocates a unique prefix for the table's data
		tableDir, err := s.dir.CreateOrOpen(tr, []string{"tables", table.TableName}, nil)
		if err != nil {
			return nil, err
		}

		// Use a specific key regarding metadata within that directory
		metaKey := tableDir.Pack(tuple.Tuple{"metadata"})

		// Check if it already has metadata (implies existence)
		// Note: CreateOrOpen is idempotent, effectively "GetOrConnect".
		// We explicitly check if metadata exists to enforce "ErrTableExists"
		existingVal, err := tr.Get(metaKey).Get()
		if err != nil {
			return nil, err
		}
		if existingVal != nil {
			return nil, ErrTableExists
		}

		// Save metadata
		tr.Set(metaKey, tableBytes)
		return nil, nil
	})
	return err
}

// GetTable retrieves a table's metadata by name.
func (s *FoundationDBStore) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	val, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		// Check if directory exists first
		exists, err := s.dir.Exists(rtr, []string{"tables", tableName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}

		// Open the directory (read-only safe if exists)
		tableDir, err := s.dir.Open(rtr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}

		return rtr.Get(tableDir.Pack(tuple.Tuple{"metadata"})).Get()
	})
	if err != nil {
		return nil, err
	}

	tableBytes, ok := val.([]byte)
	if !ok || len(tableBytes) == 0 {
		return nil, nil
	}

	var table models.Table
	if err := json.Unmarshal(tableBytes, &table); err != nil {
		return nil, err
	}

	return &table, nil
}

func (s *FoundationDBStore) DeleteTable(ctx context.Context, tableName string) (*models.Table, error) {
	val, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Check existence
		exists, err := s.dir.Exists(tr, []string{"tables", tableName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrTableNotFound
		}

		tableDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		metaKey := tableDir.Pack(tuple.Tuple{"metadata"})

		existingVal, err := tr.Get(metaKey).Get()
		if err != nil {
			return nil, err
		}
		if existingVal == nil {
			// Should strictly not happen if dir exists, but possible if inconsistent
			return nil, ErrTableNotFound
		}

		var table models.Table
		if err := json.Unmarshal(existingVal, &table); err != nil {
			return nil, err
		}

		// Mark as deleting (optional if we really delete it immediately, but good for return value)
		table.Status = models.StatusDeleting

		// Actually delete the directory (metadata + data)
		// This removes the prefix mapping and all data under it.
		if _, err := s.dir.Remove(tr, []string{"tables", tableName}); err != nil {
			return nil, err
		}

		return &table, nil
	})

	if err != nil {
		return nil, err
	}
	return val.(*models.Table), nil
}

func (s *FoundationDBStore) ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
	// We fetch one more than the limit to determine if there are more tables.
	// If limit is 0 (unspecified), we can pick a default, or just pass 0 if fdb supports it (usually means all).
	// DynamoDB default is usually larger, but let's stick to user request.
	// If the user didn't specify a limit, we might want to default to 100 or something.
	fetchLimit := limit
	if fetchLimit > 0 {
		fetchLimit++
	} else {
		fetchLimit = 101 // Default fetch size if none provided
	}

	res, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		options := directory.ListOptions{
			Limit: fetchLimit,
			After: exclusiveStartTableName,
		}
		return s.dir.List(rtr, []string{"tables"}, options)
	})
	if err != nil {
		return nil, "", err
	}

	tableNames := res.([]string)
	lastEvaluatedTableName := ""

	if limit > 0 && len(tableNames) > limit {
		// We have more results than requested
		lastEvaluatedTableName = tableNames[limit-1]
		tableNames = tableNames[:limit]
	} else if len(tableNames) > 0 {
		// If we didn't hit the limit (or no limit was set but we fetched default),
		// we check if we need pagination.
		// If fetchLimit was set to 101 (default) and we got 101, then we have a LEK.
		if limit == 0 && len(tableNames) == 101 {
			lastEvaluatedTableName = tableNames[100-1] // The 100th item is the last one we return
			tableNames = tableNames[:100]
		}
	}

	return tableNames, lastEvaluatedTableName, nil
}

// PutItem writes an item to the table.
func (s *FoundationDBStore) PutItem(ctx context.Context, tableName string, item map[string]models.AttributeValue) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Get Table Metadata to understand Key Schema
		// Note: In an optimized system, we would cache this.
		table, err := s.getTableInternal(tr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}

		// 2. Extract Key Fields
		keyTuple, err := s.buildKeyTuple(table, item)
		if err != nil {
			return nil, err
		}

		// 3. Serialize Item
		itemBytes, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}

		// 4. Store
		// Path: ["tables", tableName] -> Pack("data", <pk_tuple>)
		tableDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
		tr.Set(itemKey, itemBytes)

		return nil, nil
	})
	return err
}

// GetItem retrieves an item by key.
func (s *FoundationDBStore) GetItem(ctx context.Context, tableName string, key map[string]models.AttributeValue) (map[string]models.AttributeValue, error) {
	val, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		// 1. Get Table Metadata
		table, err := s.getTableInternal(rtr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, nil // Table not found implies item not found
		}

		// 2. Extract Key Fields from the *request key map*
		keyTuple, err := s.buildKeyTuple(table, key)
		if err != nil {
			return nil, err
		}

		// 3. Fetch
		tableDir, err := s.dir.Open(rtr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
		return rtr.Get(itemKey).Get()
	})
	if err != nil {
		return nil, err
	}

	valBytes, ok := val.([]byte)
	if !ok || len(valBytes) == 0 {
		return nil, nil
	}

	// 4. Deserialize
	var item map[string]models.AttributeValue
	if err := json.Unmarshal(valBytes, &item); err != nil {
		return nil, err
	}
	return item, nil
}

// DeleteItem deletes an item by key.
func (s *FoundationDBStore) DeleteItem(ctx context.Context, tableName string, key map[string]models.AttributeValue) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Get Table Metadata
		table, err := s.getTableInternal(tr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, models.New("ResourceNotFoundException", "Requested resource not found")
		}

		// 2. Extract Key Fields
		keyTuple, err := s.buildKeyTuple(table, key)
		if err != nil {
			return nil, err
		}

		// 3. Delete
		tableDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
		tr.Clear(itemKey)

		return nil, nil
	})
	return err
}

// Helpers

func (s *FoundationDBStore) getTableInternal(rtr fdb.ReadTransaction, tableName string) (*models.Table, error) {
	exists, err := s.dir.Exists(rtr, []string{"tables", tableName})
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	tableDir, err := s.dir.Open(rtr, []string{"tables", tableName}, nil)
	if err != nil {
		return nil, err
	}
	val, err := rtr.Get(tableDir.Pack(tuple.Tuple{"metadata"})).Get()
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var table models.Table
	if err := json.Unmarshal(val, &table); err != nil {
		return nil, err
	}
	return &table, nil
}

func (s *FoundationDBStore) buildKeyTuple(table *models.Table, item map[string]models.AttributeValue) ([]tuple.TupleElement, error) {
	var pkHashName, pkRangeName string
	for _, k := range table.KeySchema {
		if k.KeyType == "HASH" {
			pkHashName = k.AttributeName
		} else if k.KeyType == "RANGE" {
			pkRangeName = k.AttributeName
		}
	}

	if pkHashName == "" {
		return nil, fmt.Errorf("table %s has no HASH key", table.TableName) // Should not happen for valid tables
	}

	hashVal, ok := item[pkHashName]
	if !ok {
		return nil, fmt.Errorf("missing HASH key: %s", pkHashName)
	}
	hashTupleElem, err := toTupleElement(hashVal)
	if err != nil {
		return nil, err
	}

	elems := []tuple.TupleElement{hashTupleElem}

	if pkRangeName != "" {
		rangeVal, ok := item[pkRangeName]
		if !ok {
			return nil, fmt.Errorf("missing RANGE key: %s", pkRangeName)
		}
		rangeTupleElem, err := toTupleElement(rangeVal)
		if err != nil {
			return nil, err
		}
		elems = append(elems, rangeTupleElem)
	}

	return elems, nil
}

func toTupleElement(av models.AttributeValue) (tuple.TupleElement, error) {
	if av.S != nil {
		return *av.S, nil
	}
	if av.N != nil {
		// Store numbers as strings/bytes or convert?
		// FDB Tuple layer supports integers and floats. DynamoDB numbers are arbitrary precision strings.
		// For MVP simplicity and sorting correctness for common numbers, maybe store as string?
		// Actually, DynamoDB strings sort differently than numbers.
		// If we store as string in tuple, "10" comes before "2". That's wrong for numbers.
		// We should try to parse as float64 or int64 if possible for ordering.
		// But for exact equality matching (Hash key), string is fine.
		// Range keys need proper ordering.
		// Let's store as string for MVP to ensure round-trip accuracy, realizing range queries might be lexicographical on the string rep.
		return *av.N, nil
	}
	if av.B != nil {
		return []byte(*av.B), nil
	}
	// ... other types ...
	// For PKs, usually only S, N, B are allowed.
	return nil, fmt.Errorf("unsupported key type or null")
}
