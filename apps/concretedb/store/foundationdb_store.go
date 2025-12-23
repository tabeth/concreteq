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
