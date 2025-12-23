package store

import (
	"context"
	"errors"

	"github.com/tabeth/concretedb/models"
)

// ErrTableExists is returned when trying to create a table that already exists.
var ErrTableExists = errors.New("table already exists")
var ErrTableNotFound = errors.New("table not found")

// Store defines the interface for all persistence operations for table metadata.
type Store interface {
	// CreateTable persists a new table's metadata.
	// It must return ErrTableExists if a table with the same name already exists.
	CreateTable(ctx context.Context, table *models.Table) error

	// GetTable retrieves a table's metadata by name.
	// It should return nil, nil if the table is not found.
	GetTable(ctx context.Context, tableName string) (*models.Table, error)

	// DeleteTable deletes the table - effectively the reverse of CreateTable
	// Attempting to delete a table that is not present results in an error.
	DeleteTable(ctx context.Context, tableName string) (*models.Table, error)

	// ListTables lists the tables in the database.
	// It supports pagination via limit and exclusiveStartTableName.
	ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error)
}
