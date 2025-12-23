package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store"
)

// TableServicer defines the interface for our table service.
// The API layer will depend on this interface.
type TableServicer interface {
	CreateTable(ctx context.Context, table *models.Table) (*models.Table, error)
	DeleteTable(ctx context.Context, tableName string) (*models.Table, error)
	ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error)
	GetTable(ctx context.Context, tableName string) (*models.Table, error)
}

// TableService contains the business logic for table operations.
type TableService struct {
	store store.Store
}

// NewTableService creates a new TableService.
func NewTableService(store store.Store) *TableService {
	return &TableService{store: store}
}

// CreateTable validates the request, creates a new table, persists it,
// and returns the persisted table .
// It now accepts a `*models.Table` instead of an API request object.
func (s *TableService) CreateTable(ctx context.Context, table *models.Table) (*models.Table, error) {
	if err := validateTable(table); err != nil {
		return nil, err
	}

	// Default to StatusCreating
	// Later creating will be queued up. For now it's done in a single, long request.
	// Handling the queued version will later by the default, with a flag introduced to make it atomic.
	if table.Status == "" {
		table.Status = models.StatusCreating
	}

	if err := s.store.CreateTable(ctx, table); err != nil {
		if errors.Is(err, store.ErrTableExists) {
			// Translate storage error to a specific API error
			return nil, models.New("ResourceInUseException", fmt.Sprintf("Table already exists: %s", table.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to persist table: %v", err))
	}

	return table, nil
}

// Add the DeleteTable method to the TableService struct
// At some point in the future, DeleteTable will use a SQS interface which will queue up the work that's needed for deletion.
// For now the deletion will happen entirely in the request-response, so it can take a while.
func (s *TableService) DeleteTable(ctx context.Context, tableName string) (*models.Table, error) {
	if tableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	// The storage layer will handle the transaction of finding and updating the status.
	deletedTable, err := s.store.DeleteTable(ctx, tableName)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			// Translate storage error to a specific API error
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", tableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to delete table: %v", err))
	}

	return deletedTable, nil
}

// ListTables retrieves a list of table names.
func (s *TableService) ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
	// Simple validation
	if limit < 0 {
		return nil, "", models.New("ValidationException", "Limit must be non-negative")
	}

	tableNames, lastEvaluatedTableName, err := s.store.ListTables(ctx, limit, exclusiveStartTableName)
	if err != nil {
		return nil, "", models.New("InternalFailure", fmt.Sprintf("failed to list tables: %v", err))
	}

	return tableNames, lastEvaluatedTableName, nil
}

// GetTable retrieves a table's metadata by name.
func (s *TableService) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	if tableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	table, err := s.store.GetTable(ctx, tableName)
	if err != nil {
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to get table: %v", err))
	}
	if table == nil {
		return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", tableName))
	}

	return table, nil
}

// validateTable performs business rule validation on the internal db model. In general all table validation
// should occur here. Types of table validaiton that might happen here is schema validation,
// presence of various appropriate values, and others.
func validateTable(table *models.Table) error {
	if table.TableName == "" {
		return models.New("ValidationException", "TableName cannot be empty")
	}
	if len(table.KeySchema) == 0 {
		return models.New("ValidationException", "KeySchema cannot be empty")
	}
	return nil
}
