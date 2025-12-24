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

	// PutItem writes an item to the table. It replaces any existing item with the same key.
	PutItem(ctx context.Context, tableName string, item map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)

	// GetItem retrieves an item from the table by its key.
	// It returns nil, nil if the item is not found.
	GetItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, consistentRead bool) (map[string]models.AttributeValue, error)

	// DeleteItem deletes an item from the table by its key.
	// It is idempotent and succeeds even if the item does not exist.
	DeleteItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)

	// UpdateItem updates an existing item's attributes or adds a new item to the table if it does not already exist.
	UpdateItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)

	// Scan scans the table.
	Scan(ctx context.Context, tableName string, filterExpression string, projectionExpression string, expressionAttributeNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error)

	// Query queries the table.
	Query(ctx context.Context, tableName string, indexName string, keyConditionExpression string, filterExpression string, projectionExpression string, expressionAttributeNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error)

	// BatchGetItem retrieves multiple items from multiple tables.
	BatchGetItem(ctx context.Context, requestItems map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error)

	// BatchWriteItem puts or deletes multiple items in multiple tables.
	BatchWriteItem(ctx context.Context, requestItems map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error)

	// TransactGetItems retrieves multiple items from one or more tables in a single atomic transaction.
	TransactGetItems(ctx context.Context, transactItems []models.TransactGetItem) ([]models.ItemResponse, error)

	// TransactWriteItems writes multiple items to one or more tables in a single atomic transaction.
	TransactWriteItems(ctx context.Context, transactItems []models.TransactWriteItem, clientRequestToken string) error
}
