package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
)

// FoundationDBStore implements the Store interface using FoundationDB.
type FoundationDBStore struct {
	db fdb.Database
	ss subspace.Subspace // Use subspaces to organize keys
}

// NewFoundationDBStore creates a new store connected to FoundationDB.
func NewFoundationDBStore(db fdb.Database) *FoundationDBStore {
	fmt.Println("Creating FDB store.")
	return &FoundationDBStore{
		db: db,
		// Create a subspace for all table metadata
		ss: subspace.Sub("tables", "metadata"),
	}
}

// CreateTable persists a new table's metadata within a FoundationDB transaction.
func (s *FoundationDBStore) CreateTable(ctx context.Context, table *models.Table) error {
	fmt.Println("Creating table :", table.TableName)
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Check if a table with this name already exists
		key := s.ss.Pack(tuple.Tuple{table.TableName})
		fmt.Println("Checking for presence of table :", table.TableName)
		existingVal, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if existingVal != nil {
			return nil, ErrTableExists
		}

		// Default to StatusCreating
		if table.Status == "" {
			table.Status = models.StatusCreating
		}

		// Marshal the table metadata to JSON
		tableBytes, err := json.Marshal(table)
		if err != nil {
			return nil, err
		}

		// Set the key with the serialized data
		tr.Set(key, tableBytes)
		return nil, nil
	})
	return err
}

// GetTable retrieves a table's metadata by name.
func (s *FoundationDBStore) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	val, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		key := s.ss.Pack(tuple.Tuple{tableName})
		return rtr.Get(key).Get()
	})
	if err != nil {
		return nil, err
	}

	tableBytes, ok := val.([]byte)
	if !ok || len(tableBytes) == 0 {
		// Table not found
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
		key := s.ss.Pack(tuple.Tuple{tableName})

		// Get the existing table
		existingVal, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if existingVal == nil {
			return nil, ErrTableNotFound // Table does not exist
		}

		// Unmarshal, update status, and marshal back
		var table models.Table
		if err := json.Unmarshal(existingVal, &table); err != nil {
			return nil, err
		}

		// This is the core logic: change the table's status
		table.Status = models.StatusDeleting

		tableBytes, err := json.Marshal(&table)
		if err != nil {
			return nil, err
		}

		// Save the updated record
		tr.Set(key, tableBytes)

		// Return the updated table object
		return &table, nil
	})

	if err != nil {
		return nil, err
	}
	return val.(*models.Table), nil
}
