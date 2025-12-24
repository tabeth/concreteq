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

	// Item Operations
	PutItem(ctx context.Context, request *models.PutItemRequest) (*models.PutItemResponse, error)
	GetItem(ctx context.Context, request *models.GetItemRequest) (*models.GetItemResponse, error)
	DeleteItem(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error)
	UpdateItem(ctx context.Context, request *models.UpdateItemRequest) (*models.UpdateItemResponse, error)
	Scan(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error)
	Query(ctx context.Context, input *models.QueryRequest) (*models.QueryResponse, error)
	BatchGetItem(ctx context.Context, input *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error)
	BatchWriteItem(ctx context.Context, input *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error)
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

// Item Operations

// PutItem writes an item to the table.
func (s *TableService) PutItem(ctx context.Context, request *models.PutItemRequest) (*models.PutItemResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if len(request.Item) == 0 {
		return nil, models.New("ValidationException", "Item cannot be empty")
	}

	attributes, err := s.store.PutItem(ctx, request.TableName, request.Item, request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, request.ReturnValues)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", request.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to put item: %v", err))
	}
	return &models.PutItemResponse{Attributes: attributes}, nil
}

// GetItem retrieves an item by key.
func (s *TableService) GetItem(ctx context.Context, request *models.GetItemRequest) (*models.GetItemResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if len(request.Key) == 0 {
		return nil, models.New("ValidationException", "Key cannot be empty")
	}

	item, err := s.store.GetItem(ctx, request.TableName, request.Key, request.ConsistentRead)
	if err != nil {
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to get item: %v", err))
	}
	// Note: Item not found just returns nil, nil in DynamoDB GetItem (with empty Item in response),
	// unless we specifically want to distinguish. The store returns nil, nil.
	// The API handler will map this to an empty Item field if nil.
	return &models.GetItemResponse{Item: item}, nil
}

// DeleteItem deletes an item by key.
func (s *TableService) DeleteItem(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if len(request.Key) == 0 {
		return nil, models.New("ValidationException", "Key cannot be empty")
	}

	attributes, err := s.store.DeleteItem(ctx, request.TableName, request.Key, request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, request.ReturnValues)
	if err != nil {
		// DeleteItem in DynamoDB is idempotent.
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", request.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to delete item: %v", err))
	}
	return &models.DeleteItemResponse{Attributes: attributes}, nil
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

// UpdateItem updates an item.
func (s *TableService) UpdateItem(ctx context.Context, request *models.UpdateItemRequest) (*models.UpdateItemResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if len(request.Key) == 0 {
		return nil, models.New("ValidationException", "Key cannot be empty")
	}

	// Call the store
	attributes, err := s.store.UpdateItem(ctx, request.TableName, request.Key, request.UpdateExpression, request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, request.ReturnValues)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", request.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to update item: %v", err))
	}

	return &models.UpdateItemResponse{Attributes: attributes}, nil
}

// Scan retrieves items from the table.
func (s *TableService) Scan(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error) {
	if input.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	// Call the store
	items, lastKey, err := s.store.Scan(ctx, input.TableName, input.FilterExpression, input.ProjectionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, input.Limit, input.ExclusiveStartKey, input.ConsistentRead)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", input.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to scan table: %v", err))
	}

	return &models.ScanResponse{
		Items:            items,
		Count:            int32(len(items)),
		ScannedCount:     int32(len(items)), // For now same as Count
		LastEvaluatedKey: lastKey,
	}, nil
}

// Query retrieves items based on key conditions (partition key equality).
func (s *TableService) Query(ctx context.Context, input *models.QueryRequest) (*models.QueryResponse, error) {
	if input.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if input.KeyConditionExpression == "" {
		return nil, models.New("ValidationException", "KeyConditionExpression cannot be empty")
	}

	// Call the store
	items, lastKey, err := s.store.Query(ctx, input.TableName, input.IndexName, input.KeyConditionExpression, input.FilterExpression, input.ProjectionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, input.Limit, input.ExclusiveStartKey, input.ConsistentRead)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", input.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to query table: %v", err))
	}

	val := int32(len(items))

	return &models.QueryResponse{
		Items:            items,
		Count:            val,
		ScannedCount:     val,
		LastEvaluatedKey: lastKey,
	}, nil
}

// BatchGetItem retrieves items from multiple tables.
func (s *TableService) BatchGetItem(ctx context.Context, input *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error) {
	if len(input.RequestItems) == 0 {
		return nil, models.New("ValidationException", "RequestItems cannot be empty")
	}

	responses, unprocessed, err := s.store.BatchGetItem(ctx, input.RequestItems)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", "One or more requested tables not found")
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to batch get items: %v", err))
	}

	return &models.BatchGetItemResponse{
		Responses:       responses,
		UnprocessedKeys: unprocessed,
	}, nil
}

// BatchWriteItem writes/deletes items in multiple tables.
func (s *TableService) BatchWriteItem(ctx context.Context, input *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error) {
	if len(input.RequestItems) == 0 {
		return nil, models.New("ValidationException", "RequestItems cannot be empty")
	}

	// Basic validation of item count (DynamoDB limit is 25)
	count := 0
	for _, reqs := range input.RequestItems {
		count += len(reqs)
	}
	if count > 25 {
		return nil, models.New("ValidationException", "Member must have length less than or equal to 25")
	}

	unprocessed, err := s.store.BatchWriteItem(ctx, input.RequestItems)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", "One or more requested tables not found")
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to batch write items: %v", err))
	}

	return &models.BatchWriteItemResponse{
		UnprocessedItems: unprocessed,
	}, nil
}
