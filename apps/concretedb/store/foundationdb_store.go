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
		// Default to StatusCreating
		if table.Status == "" {
			table.Status = models.StatusCreating
		}

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

		// Mark as deleting
		table.Status = models.StatusDeleting

		tableBytes, err := json.Marshal(&table)
		if err != nil {
			return nil, err
		}

		tr.Set(metaKey, tableBytes)

		// In a real implementation, we might kick off an async job to remove() the directory later
		// For now, we just update status.
		// If we wanted to actually delete data:
		// s.dir.Remove(tr, []string{"tables", tableName})

		return &table, nil
	})

	if err != nil {
		return nil, err
	}
	return val.(*models.Table), nil
}
