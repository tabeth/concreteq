package service

import (
	"context"
	"errors"
	"testing"
"github.com/stretchr/testify/assert"

	"github.com/tabeth/concretedb/models"
	st "github.com/tabeth/concretedb/store"
)

// mockStore is a mock implementation of the st.Store interface for testing.
type mockStore struct {
	CreateTableFunc func(ctx context.Context, table *models.Table) error
	GetTableFunc    func(ctx context.Context, tableName string) (*models.Table, error)
	DeleteTableFunc func(ctx context.Context, tableName string) (*models.Table, error) // <-- ADD THIS
	ListTablesFunc  func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error)
	UpdateTableFunc func(ctx context.Context, tableName string, streamSpec *models.StreamSpecification) (*models.Table, error)
	// Item Operations
	PutItemFunc            func(ctx context.Context, tableName string, item map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)
	GetItemFunc            func(ctx context.Context, tableName string, key map[string]models.AttributeValue, projectionExpression string, exprAttrNames map[string]string, consistentRead bool) (map[string]models.AttributeValue, error)
	DeleteItemFunc         func(ctx context.Context, tableName string, key map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)
	UpdateItemFunc         func(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)
	ScanFunc               func(ctx context.Context, tableName string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error)
	QueryFunc              func(ctx context.Context, tableName string, indexName string, keyConditionExpression string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error)
	BatchGetItemFunc       func(ctx context.Context, requestItems map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error)
	BatchWriteItemFunc     func(ctx context.Context, requestItems map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error)
	TransactGetItemsFunc   func(ctx context.Context, transactItems []models.TransactGetItem) ([]models.ItemResponse, error)
	TransactWriteItemsFunc func(ctx context.Context, transactItems []models.TransactWriteItem, clientRequestToken string) error
	ListStreamsFunc        func(ctx context.Context, tableName string, limit int, exclusiveStartStreamArn string) ([]models.StreamSummary, string, error)
	DescribeStreamFunc     func(ctx context.Context, streamArn string, limit int, exclusiveStartShardId string) (*models.StreamDescription, error)
	GetShardIteratorFunc   func(ctx context.Context, streamArn string, shardId string, iteratorType string, sequenceNumber string) (string, error)
	GetRecordsFunc         func(ctx context.Context, shardIterator string, limit int) ([]models.Record, string, error)
}

func (m *mockStore) CreateTable(ctx context.Context, table *models.Table) error {
	return m.CreateTableFunc(ctx, table)
}

func (m *mockStore) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	return m.GetTableFunc(ctx, tableName)
}

// Add this method to satisfy the st.Store interface
func (m *mockStore) DeleteTable(ctx context.Context, tableName string) (*models.Table, error) {
	return m.DeleteTableFunc(ctx, tableName)
}

func (m *mockStore) ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
	return m.ListTablesFunc(ctx, limit, exclusiveStartTableName)
}

func (m *mockStore) UpdateTable(ctx context.Context, tableName string, streamSpec *models.StreamSpecification) (*models.Table, error) {
	return m.UpdateTableFunc(ctx, tableName, streamSpec)
}

func (m *mockStore) PutItem(ctx context.Context, tableName string, item map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	return m.PutItemFunc(ctx, tableName, item, conditionExpression, exprAttrNames, exprAttrValues, returnValues)
}

func (m *mockStore) GetItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, projectionExpression string, exprAttrNames map[string]string, consistentRead bool) (map[string]models.AttributeValue, error) {
	return m.GetItemFunc(ctx, tableName, key, projectionExpression, exprAttrNames, consistentRead)
}

func (m *mockStore) DeleteItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	return m.DeleteItemFunc(ctx, tableName, key, conditionExpression, exprAttrNames, exprAttrValues, returnValues)
}

func (m *mockStore) UpdateItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	return m.UpdateItemFunc(ctx, tableName, key, updateExpression, conditionExpression, exprAttrNames, exprAttrValues, returnValues)
}

func (m *mockStore) Scan(ctx context.Context, tableName string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
	return m.ScanFunc(ctx, tableName, filterExpression, projectionExpression, exprAttrNames, exprAttrValues, limit, exclusiveStartKey, consistentRead)
}

func (m *mockStore) Query(ctx context.Context, tableName string, indexName string, keyConditionExpression string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
	return m.QueryFunc(ctx, tableName, indexName, keyConditionExpression, filterExpression, projectionExpression, exprAttrNames, expressionAttributeValues, limit, exclusiveStartKey, consistentRead)
}

func (m *mockStore) BatchGetItem(ctx context.Context, requestItems map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
	return m.BatchGetItemFunc(ctx, requestItems)
}

func (m *mockStore) BatchWriteItem(ctx context.Context, requestItems map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error) {
	return m.BatchWriteItemFunc(ctx, requestItems)
}

func (m *mockStore) TransactGetItems(ctx context.Context, transactItems []models.TransactGetItem) ([]models.ItemResponse, error) {
	return m.TransactGetItemsFunc(ctx, transactItems)
}

func (m *mockStore) TransactWriteItems(ctx context.Context, transactItems []models.TransactWriteItem, clientRequestToken string) error {
	return m.TransactWriteItemsFunc(ctx, transactItems, clientRequestToken)
}

func (m *mockStore) ListStreams(ctx context.Context, tableName string, limit int, exclusiveStartStreamArn string) ([]models.StreamSummary, string, error) {
	return m.ListStreamsFunc(ctx, tableName, limit, exclusiveStartStreamArn)
}

func (m *mockStore) DescribeStream(ctx context.Context, streamArn string, limit int, exclusiveStartShardId string) (*models.StreamDescription, error) {
	return m.DescribeStreamFunc(ctx, streamArn, limit, exclusiveStartShardId)
}

func (m *mockStore) GetShardIterator(ctx context.Context, streamArn string, shardId string, iteratorType string, sequenceNumber string) (string, error) {
	return m.GetShardIteratorFunc(ctx, streamArn, shardId, iteratorType, sequenceNumber)
}

func (m *mockStore) GetRecords(ctx context.Context, shardIterator string, limit int) ([]models.Record, string, error) {
	return m.GetRecordsFunc(ctx, shardIterator, limit)
}

func TestTableService_DeleteTable_NotFound(t *testing.T) {
	mock := &mockStore{
		DeleteTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			// Mock the storage layer returning a "not found" error
			return nil, st.ErrTableNotFound
		},
	}
	service := NewTableService(mock)

	_, err := service.DeleteTable(context.Background(), "non-existent-table")

	var apiErr *models.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected an *models.APIError, but got %T", err)
	}

	if apiErr.Type != "ResourceNotFoundException" {
		t.Errorf("expected error type ResourceNotFoundException, got %s", apiErr.Type)
	}
}

func TestTableService_CreateTable_Success(t *testing.T) {
	mock := &mockStore{
		CreateTableFunc: func(ctx context.Context, table *models.Table) error {
			return nil // Mock success
		},
	}
	service := NewTableService(mock)

	table := &models.Table{
		TableName: "test-service-table",
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}

	resp, err := service.CreateTable(context.Background(), table)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if resp.TableName != "test-service-table" {
		t.Errorf("unexpected table name: got %s, want %s", resp.TableName, "test-service-table")
	}
	if resp.Status != models.StatusCreating {
		t.Errorf("unexpected table status: got %s, want %s", resp.Status, "CREATING")
	}
}

func TestTableService_CreateTable_ValidationError(t *testing.T) {
	service := NewTableService(&mockStore{}) // Store won't be called

	// Test with an invalid models.Table (empty name)
	table := &models.Table{TableName: ""}

	_, err := service.CreateTable(context.Background(), table)

	// CHANGED: We now check for our new models.APIError type.
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected an *models.APIError, but got %T", err)
	}

	if apiErr.Type != "ValidationException" {
		t.Errorf("expected error type ValidationException, got %s", apiErr.Type)
	}
}

func TestTableService_CreateTable_EmptyKeySchema(t *testing.T) {
	service := NewTableService(&mockStore{})
	table := &models.Table{
		TableName: "valid-name",
		KeySchema: []models.KeySchemaElement{}, // Empty
	}
	_, err := service.CreateTable(context.Background(), table)
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "ValidationException" {
		t.Errorf("expected ValidationException for empty KeySchema")
	}
}

func TestTableService_CreateTable_ResourceInUse(t *testing.T) {
	mock := &mockStore{
		CreateTableFunc: func(ctx context.Context, table *models.Table) error {
			return st.ErrTableExists // Mock that the table already exists
		},
	}
	service := NewTableService(mock)

	table := &models.Table{
		TableName: "existing-table",
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}

	_, err := service.CreateTable(context.Background(), table)

	var apiErr *models.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected an *models.APIError, but got %T", err)
	}

	if apiErr.Type != "ResourceInUseException" {
		t.Errorf("expected error type ResourceInUseException, got %s", apiErr.Type)
	}
}

func TestTableService_ListTables(t *testing.T) {
	mock := &mockStore{
		ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
			return []string{"t1", "t2"}, "t2", nil
		},
	}
	service := NewTableService(mock)

	names, lastEval, err := service.ListTables(context.Background(), 10, "")
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}
	if len(names) != 2 {
		t.Errorf("expected 2 names, got %d", len(names))
	}
	if lastEval != "t2" {
		t.Errorf("expected lastEval 't2', got '%s'", lastEval)
	}
}

func TestTableService_GetTable_Success(t *testing.T) {
	mock := &mockStore{
		GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return &models.Table{TableName: tableName, Status: models.StatusActive}, nil
		},
	}
	service := NewTableService(mock)

	table, err := service.GetTable(context.Background(), "my-table")
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}
	if table.TableName != "my-table" {
		t.Errorf("expected table name 'my-table', got '%s'", table.TableName)
	}
}

func TestTableService_ListTables_Error(t *testing.T) {
	mock := &mockStore{
		ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
			return nil, "", errors.New("db error")
		},
	}
	service := NewTableService(mock)

	_, _, err := service.ListTables(context.Background(), 10, "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *models.APIError, got %T", err)
	}
	if apiErr.Type != "InternalFailure" {
		t.Errorf("expected InternalFailure, got %s", apiErr.Type)
	}
}

func TestTableService_ListTables_InvalidLimit(t *testing.T) {
	service := NewTableService(&mockStore{})
	_, _, err := service.ListTables(context.Background(), -1, "")
	if err == nil {
		t.Fatal("expected error for invalid limit")
	}
	var apiErr *models.APIError
	if errors.As(err, &apiErr) {
		if apiErr.Type != "ValidationException" {
			t.Errorf("expected ValidationException, got %s", apiErr.Type)
		}
	}
}

func TestTableService_GetTable_Error(t *testing.T) {
	mock := &mockStore{
		GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return nil, errors.New("db error")
		},
	}
	service := NewTableService(mock)

	_, err := service.GetTable(context.Background(), "t")
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
		t.Errorf("expected InternalFailure APIError")
	}
}

func TestTableService_GetTable_EmptyName(t *testing.T) {
	service := NewTableService(&mockStore{})
	_, err := service.GetTable(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "ValidationException" {
		t.Errorf("expected ValidationException")
	}
}

func TestTableService_DeleteTable_GenericError(t *testing.T) {
	mock := &mockStore{
		DeleteTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return nil, errors.New("generic error")
		},
	}
	service := NewTableService(mock)
	_, err := service.DeleteTable(context.Background(), "t")
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
		t.Errorf("expected InternalFailure")
	}
}

func TestTableService_DeleteTable_EmptyName(t *testing.T) {
	service := NewTableService(&mockStore{})
	_, err := service.DeleteTable(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "ValidationException" {
		t.Errorf("expected ValidationException")
	}
}

func TestTableService_CreateTable_StoreError(t *testing.T) {
	mock := &mockStore{
		CreateTableFunc: func(ctx context.Context, table *models.Table) error {
			return errors.New("random error")
		},
	}
	service := NewTableService(mock)
	table := &models.Table{
		TableName: "test",
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}
	_, err := service.CreateTable(context.Background(), table)
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
		t.Errorf("expected InternalFailure")
	}
}

// Item Operation Tests

func TestTableService_PutItem(t *testing.T) {
	mock := &mockStore{
		PutItemFunc: func(ctx context.Context, tableName string, item map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
			if tableName == "error-table" {
				return nil, errors.New("store error")
			}
			if tableName == "missing-table" {
				return nil, st.ErrTableNotFound
			}
			return nil, nil
		},
	}
	service := NewTableService(mock)

	// Success
	_, err := service.PutItem(context.Background(), &models.PutItemRequest{
		TableName: "t",
		Item:      map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}

	// Validation: Empty table name
	_, err = service.PutItem(context.Background(), &models.PutItemRequest{
		TableName: "",
		Item:      map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected validation error for empty table name")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "ValidationException" {
			t.Errorf("expected ValidationException, got %v", err)
		}
	}

	// Validation: Empty item
	_, err = service.PutItem(context.Background(), &models.PutItemRequest{
		TableName: "t",
		Item:      nil,
	})
	if err == nil {
		t.Error("expected validation error for empty item")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "ValidationException" {
			t.Errorf("expected ValidationException, got %v", err)
		}
	}

	// Store Error: Table Not Found
	_, err = service.PutItem(context.Background(), &models.PutItemRequest{
		TableName: "missing-table",
		Item:      map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected error for missing table")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "ResourceNotFoundException" {
			t.Errorf("expected ResourceNotFoundException, got %v", err)
		}
	}

	// Store Error: Internal Failure
	_, err = service.PutItem(context.Background(), &models.PutItemRequest{
		TableName: "error-table",
		Item:      map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected error for store failure")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
			t.Errorf("expected InternalFailure, got %v", err)
		}
	}
}

func TestTableService_GetItem(t *testing.T) {
	mock := &mockStore{
		GetItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, projectionExpression string, exprAttrNames map[string]string, consistentRead bool) (map[string]models.AttributeValue, error) {
			if tableName == "error-table" {
				return nil, errors.New("store error")
			}
			if tableName == "success-table" {
				return map[string]models.AttributeValue{"id": {S: new(string)}}, nil
			}
			return nil, nil
		},
	}
	service := NewTableService(mock)

	// Success
	resp, err := service.GetItem(context.Background(), &models.GetItemRequest{
		TableName: "success-table",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}
	if resp.Item == nil {
		t.Error("expected item, got nil")
	}

	// Validation: Empty table name
	_, err = service.GetItem(context.Background(), &models.GetItemRequest{
		TableName: "",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected validation error for empty table name")
	}

	// Validation: Empty key
	_, err = service.GetItem(context.Background(), &models.GetItemRequest{
		TableName: "t",
		Key:       nil,
	})
	if err == nil {
		t.Error("expected validation error for empty key")
	}

	// Store Error
	_, err = service.GetItem(context.Background(), &models.GetItemRequest{
		TableName: "error-table",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected error for store failure")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
			t.Errorf("expected InternalFailure, got %v", err)
		}
	}
}

func TestTableService_DeleteItem(t *testing.T) {
	mock := &mockStore{
		DeleteItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
			if tableName == "error-table" {
				return nil, errors.New("store error")
			}
			if tableName == "missing-table" {
				return nil, st.ErrTableNotFound
			}
			return nil, nil
		},
	}
	service := NewTableService(mock)

	// Success
	_, err := service.DeleteItem(context.Background(), &models.DeleteItemRequest{
		TableName: "t",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}

	// Validation: Empty table name
	_, err = service.DeleteItem(context.Background(), &models.DeleteItemRequest{
		TableName: "",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected validation error for empty table name")
	}

	// Validation: Empty key
	_, err = service.DeleteItem(context.Background(), &models.DeleteItemRequest{
		TableName: "t",
		Key:       nil,
	})
	if err == nil {
		t.Error("expected validation error for empty key")
	}

	// Store Error: Table Not Found
	_, err = service.DeleteItem(context.Background(), &models.DeleteItemRequest{
		TableName: "missing-table",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected error for missing table")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "ResourceNotFoundException" {
			t.Errorf("expected ResourceNotFoundException, got %v", err)
		}
	}

	// Store Error: Internal Failure
	_, err = service.DeleteItem(context.Background(), &models.DeleteItemRequest{
		TableName: "error-table",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected error for store failure")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
			t.Errorf("expected InternalFailure, got %v", err)
		}
	}
}

func TestTableService_UpdateItem(t *testing.T) {
	mock := &mockStore{
		UpdateItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
			if tableName == "error-table" {
				return nil, errors.New("store error")
			}
			if tableName == "missing-table" {
				return nil, st.ErrTableNotFound
			}
			return map[string]models.AttributeValue{"id": {S: new(string)}}, nil
		},
	}
	service := NewTableService(mock)

	// Success
	_, err := service.UpdateItem(context.Background(), &models.UpdateItemRequest{
		TableName: "t",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}

	// Validation: Empty table name
	_, err = service.UpdateItem(context.Background(), &models.UpdateItemRequest{
		TableName: "",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected validation error for empty table name")
	}

	// Validation: Empty key
	_, err = service.UpdateItem(context.Background(), &models.UpdateItemRequest{
		TableName: "t",
		Key:       nil,
	})
	if err == nil {
		t.Error("expected validation error for empty key")
	}

	// Store Error: Table Not Found
	_, err = service.UpdateItem(context.Background(), &models.UpdateItemRequest{
		TableName: "missing-table",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected error for missing table")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "ResourceNotFoundException" {
			t.Errorf("expected ResourceNotFoundException, got %v", err)
		}
	}

	// Store Error: Internal Failure
	_, err = service.UpdateItem(context.Background(), &models.UpdateItemRequest{
		TableName: "error-table",
		Key:       map[string]models.AttributeValue{"id": {S: new(string)}},
	})
	if err == nil {
		t.Error("expected error for store failure")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
			t.Errorf("expected InternalFailure, got %v", err)
		}
	}
}

func TestTableService_Scan(t *testing.T) {
	mock := &mockStore{
		ScanFunc: func(ctx context.Context, tableName string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
			if tableName == "error-table" {
				return nil, nil, errors.New("store error")
			}
			return []map[string]models.AttributeValue{}, nil, nil
		},
	}
	service := NewTableService(mock)

	// Success
	_, err := service.Scan(context.Background(), &models.ScanRequest{
		TableName: "t",
	})
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}

	// Validation: Empty table name
	_, err = service.Scan(context.Background(), &models.ScanRequest{
		TableName: "",
	})
	if err == nil {
		t.Error("expected validation error for empty table name")
	}

	// Store Error
	_, err = service.Scan(context.Background(), &models.ScanRequest{
		TableName: "error-table",
	})
	if err == nil {
		t.Error("expected error for store failure")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
			t.Errorf("expected InternalFailure, got %v", err)
		}
	}
}

func TestTableService_Query(t *testing.T) {
	mock := &mockStore{
		QueryFunc: func(ctx context.Context, tableName string, indexName string, keyConditionExpression string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
			if tableName == "error-table" {
				return nil, nil, errors.New("store error")
			}
			if tableName == "missing-table" {
				return nil, nil, st.ErrTableNotFound
			}
			return []map[string]models.AttributeValue{}, nil, nil
		},
	}
	service := NewTableService(mock)

	// Success
	_, err := service.Query(context.Background(), &models.QueryRequest{
		TableName:              "t",
		KeyConditionExpression: "id = :v",
	})
	if err != nil {
		t.Errorf("expected success, got %v", err)
	}

	// Validation: Empty table name
	_, err = service.Query(context.Background(), &models.QueryRequest{
		TableName:              "",
		KeyConditionExpression: "id = :v",
	})
	if err == nil {
		t.Error("expected validation error for empty table name")
	}

	// Validation: Empty KeyConditionExpression
	_, err = service.Query(context.Background(), &models.QueryRequest{
		TableName:              "t",
		KeyConditionExpression: "",
	})
	if err == nil {
		t.Error("expected validation error for empty KeyConditionExpression")
	}

	// Store Error: Table NotFound
	_, err = service.Query(context.Background(), &models.QueryRequest{
		TableName:              "missing-table",
		KeyConditionExpression: "id = :v",
	})
	if err == nil {
		t.Error("expected error for missing table")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "ResourceNotFoundException" {
			t.Errorf("expected ResourceNotFoundException, got %v", err)
		}
	}

	// Store Error
	_, err = service.Query(context.Background(), &models.QueryRequest{
		TableName:              "error-table",
		KeyConditionExpression: "id = :v",
	})
	if err == nil {
		t.Error("expected error for store failure")
	} else {
		var apiErr *models.APIError
		if !errors.As(err, &apiErr) || apiErr.Type != "InternalFailure" {
			t.Errorf("expected InternalFailure, got %v", err)
		}
	}
}

func TestTableService_DeleteTable_Success(t *testing.T) {
	mock := &mockStore{
		DeleteTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return &models.Table{TableName: tableName, Status: models.StatusDeleting}, nil
		},
	}
	service := NewTableService(mock)
	resp, err := service.DeleteTable(context.Background(), "t")
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if resp.Status != models.StatusDeleting {
		t.Errorf("expected DELETING status")
	}
}

func TestTableService_GetTable_NotFoundResult(t *testing.T) {
	mock := &mockStore{
		GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return nil, nil // Not found
		},
	}
	service := NewTableService(mock)
	_, err := service.GetTable(context.Background(), "t")
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "ResourceNotFoundException" {
		t.Errorf("expected ResourceNotFoundException, got %v", err)
	}
}

func TestTableService_Scan_TableNotFound(t *testing.T) {
	mock := &mockStore{
		ScanFunc: func(ctx context.Context, tableName string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
			return nil, nil, st.ErrTableNotFound
		},
	}
	service := NewTableService(mock)
	_, err := service.Scan(context.Background(), &models.ScanRequest{TableName: "t"})
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "ResourceNotFoundException" {
		t.Errorf("expected ResourceNotFoundException, got %v", err)
	}
}

func TestTableService_Query_TableNotFound(t *testing.T) {
	mock := &mockStore{
		QueryFunc: func(ctx context.Context, tableName string, indexName string, keyConditionExpression string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
			return nil, nil, st.ErrTableNotFound
		},
	}
	service := NewTableService(mock)
	_, err := service.Query(context.Background(), &models.QueryRequest{TableName: "t", KeyConditionExpression: "pk=v"})
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) || apiErr.Type != "ResourceNotFoundException" {
		t.Errorf("expected ResourceNotFoundException, got %v", err)
	}
}

func TestTableService_ScanQuery_Coverage(t *testing.T) {
	mock := &mockStore{
		ScanFunc: func(ctx context.Context, tableName string, filter string, proj string, names map[string]string, values map[string]models.AttributeValue, limit int32, lastKey map[string]models.AttributeValue, consistent bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
			if tableName == "error" {
				return nil, nil, errors.New("scan error")
			}
			return []map[string]models.AttributeValue{{"id": {S: strPtr("1")}}}, nil, nil
		},
		QueryFunc: func(ctx context.Context, tableName string, indexName string, keyCond string, filter string, proj string, names map[string]string, values map[string]models.AttributeValue, limit int32, lastKey map[string]models.AttributeValue, consistent bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
			if tableName == "error" {
				return nil, nil, errors.New("query error")
			}
			return []map[string]models.AttributeValue{{"id": {S: strPtr("1")}}}, nil, nil
		},
	}
	service := NewTableService(mock)

	// Scan Success with Capacity
	res, err := service.Scan(context.Background(), &models.ScanRequest{TableName: "t", ReturnConsumedCapacity: "TOTAL"})
	assert.NoError(t, err)
	assert.NotNil(t, res.ConsumedCapacity)

	// Scan Store Error
	_, err = service.Scan(context.Background(), &models.ScanRequest{TableName: "error"})
	assert.Error(t, err)

	// Query Success with Capacity
	qres, err := service.Query(context.Background(), &models.QueryRequest{TableName: "t", KeyConditionExpression: "pk = :v", ReturnConsumedCapacity: "TOTAL"})
	assert.NoError(t, err)
	assert.NotNil(t, qres.ConsumedCapacity)

	// Query Store Error
	_, err = service.Query(context.Background(), &models.QueryRequest{TableName: "error"})
	assert.Error(t, err)
}

func TestTableService_BatchTransact_DetailedCoverage(t *testing.T) {
	mock := &mockStore{
		BatchWriteItemFunc: func(ctx context.Context, items map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error) {
			return nil, nil
		},
		TransactWriteItemsFunc: func(ctx context.Context, items []models.TransactWriteItem, token string) error {
			if len(items) > 0 && items[0].Put != nil && items[0].Put.TableName == "error" {
				return errors.New("store error")
			}
			if len(items) > 0 && items[0].Put != nil && items[0].Put.TableName == "notfound" {
				return st.ErrTableNotFound
			}
			return nil
		},
	}
	service := NewTableService(mock)

	// BatchWriteItem with Put and Delete for capacity
	input := &models.BatchWriteItemRequest{
		RequestItems: map[string][]models.WriteRequest{
			"t1": {
				{PutRequest: &models.PutRequest{Item: map[string]models.AttributeValue{"id": {S: strPtr("1")}}}},
				{DeleteRequest: &models.DeleteRequest{Key: map[string]models.AttributeValue{"id": {S: strPtr("2")}}}},
			},
		},
		ReturnConsumedCapacity:      "TOTAL",
		ReturnItemCollectionMetrics: "SIZE",
	}
	bw, err := service.BatchWriteItem(context.Background(), input)
	assert.NoError(t, err)
	assert.NotEmpty(t, bw.ConsumedCapacity)
	assert.NotEmpty(t, bw.ItemCollectionMetrics)

	// TransactWriteItems with all types for capacity and metrics
	twInput := &models.TransactWriteItemsRequest{
		TransactItems: []models.TransactWriteItem{
			{Put: &models.PutItemRequest{TableName: "t1", Item: map[string]models.AttributeValue{"id": {S: strPtr("1")}}}},
			{Delete: &models.DeleteItemRequest{TableName: "t1", Key: map[string]models.AttributeValue{"id": {S: strPtr("2")}}}},
			{Update: &models.UpdateItemRequest{TableName: "t1", Key: map[string]models.AttributeValue{"id": {S: strPtr("3")}}}},
			{ConditionCheck: &models.ConditionCheck{TableName: "t1", Key: map[string]models.AttributeValue{"id": {S: strPtr("4")}}}},
		},
		ReturnConsumedCapacity:      "TOTAL",
		ReturnItemCollectionMetrics: "SIZE",
	}
	tw, err := service.TransactWriteItems(context.Background(), twInput)
	assert.NoError(t, err)
	assert.NotEmpty(t, tw.ConsumedCapacity)
	assert.NotEmpty(t, tw.ItemCollectionMetrics)

	// TransactWriteItems Store Error
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{
		TransactItems: []models.TransactWriteItem{{Put: &models.PutItemRequest{TableName: "error", Item: map[string]models.AttributeValue{"id": {S: strPtr("1")}}}}},
	})
	assert.Error(t, err)

	// TransactWriteItems Table Not Found
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{
		TransactItems: []models.TransactWriteItem{{Put: &models.PutItemRequest{TableName: "notfound", Item: map[string]models.AttributeValue{"id": {S: strPtr("1")}}}}},
	})
	assert.Error(t, err)
}

func TestTableService_BatchTransactGet_Coverage(t *testing.T) {
	mock := &mockStore{
		BatchGetItemFunc: func(ctx context.Context, items map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
			return map[string][]map[string]models.AttributeValue{
				"t1": {{"id": {S: strPtr("1")}}},
			}, nil, nil
		},
		TransactGetItemsFunc: func(ctx context.Context, items []models.TransactGetItem) ([]models.ItemResponse, error) {
			return []models.ItemResponse{{Item: map[string]models.AttributeValue{"id": {S: strPtr("1")}}}}, nil
		},
	}
	service := NewTableService(mock)

	// BatchGetItem Success with Capacity
	bg, err := service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{
		RequestItems: map[string]models.KeysAndAttributes{
			"t1": {
				Keys:           []map[string]models.AttributeValue{{"id": {S: strPtr("1")}}},
				ConsistentRead: true,
			},
		},
		ReturnConsumedCapacity: "TOTAL",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, bg.ConsumedCapacity)

	// TransactGetItems Success with Capacity
	tg, err := service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{
		TransactItems: []models.TransactGetItem{
			{Get: models.GetItemRequest{TableName: "t1", Key: map[string]models.AttributeValue{"id": {S: strPtr("1")}}, ConsistentRead: true}},
		},
		ReturnConsumedCapacity: "TOTAL",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, tg.ConsumedCapacity)
}

func TestTableService_GetItem_Parity(t *testing.T) {
	mock := &mockStore{
		GetItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, projectionExpression string, exprAttrNames map[string]string, consistentRead bool) (map[string]models.AttributeValue, error) {
			// Verify projection expression is passed
			if projectionExpression != "id, name" {
				t.Errorf("expected projection id, name, got %s", projectionExpression)
			}
			return map[string]models.AttributeValue{
				"id":   {S: strPtr("1")},
				"name": {S: strPtr("John")},
			}, nil
		},
	}
	service := NewTableService(mock)

	// Test Projection and Capacity
	resp, err := service.GetItem(context.Background(), &models.GetItemRequest{
		TableName:              "test-table",
		Key:                    map[string]models.AttributeValue{"id": {S: strPtr("1")}},
		ProjectionExpression:   "id, name",
		ReturnConsumedCapacity: "TOTAL",
	})

	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}

	if resp.Item == nil {
		t.Fatal("expected item, got nil")
	}

	if resp.ConsumedCapacity == nil {
		t.Fatal("expected ConsumedCapacity, got nil")
	}

	if resp.ConsumedCapacity.TableName != "test-table" {
		t.Errorf("expected TableName test-table, got %s", resp.ConsumedCapacity.TableName)
	}

	// units = ceil( (2+1 + 4+4) / 4096 ) = ceil(11/4096) = 1.0 (eventual consistent)
	// names: id(2 letters), name(4 letters). values: "1"(1), "John"(4). Total roughly 11.
	if resp.ConsumedCapacity.CapacityUnits != 0.5 {
		t.Errorf("expected 0.5 units (eventual), got %f", resp.ConsumedCapacity.CapacityUnits)
	}
}

func TestTableService_PutItem_Capacity(t *testing.T) {
	mock := &mockStore{
		PutItemFunc: func(ctx context.Context, tableName string, item map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
			return nil, nil
		},
	}
	service := NewTableService(mock)

	resp, err := service.PutItem(context.Background(), &models.PutItemRequest{
		TableName:              "test-table",
		Item:                   map[string]models.AttributeValue{"id": {S: strPtr("1")}},
		ReturnConsumedCapacity: "TOTAL",
	})

	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}

	if resp.ConsumedCapacity == nil {
		t.Fatal("expected ConsumedCapacity, got nil")
	}

	// 1 WCU for small item
	if resp.ConsumedCapacity.CapacityUnits != 1.0 {
		t.Errorf("expected 1.0 units, got %f", resp.ConsumedCapacity.CapacityUnits)
	}
}
func TestTableService_UpdateItem_Metrics(t *testing.T) {
	mock := &mockStore{
		UpdateItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
			return nil, nil
		},
	}
	service := NewTableService(mock)

	resp, err := service.UpdateItem(context.Background(), &models.UpdateItemRequest{
		TableName:                   "test-table",
		Key:                         map[string]models.AttributeValue{"id": {S: strPtr("1")}},
		UpdateExpression:            "REMOVE oldAttr",
		ReturnItemCollectionMetrics: "SIZE",
	})

	if err != nil {
		t.Fatalf("UpdateItem failed: %v", err)
	}

	if resp.ItemCollectionMetrics == nil {
		t.Fatal("expected ItemCollectionMetrics, got nil")
	}

	if resp.ItemCollectionMetrics.ItemCollectionKey == nil {
		t.Error("expected ItemCollectionKey, got nil")
	}
}

func TestTableService_DeleteItem_Metrics(t *testing.T) {
	mock := &mockStore{
		DeleteItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
			return nil, nil
		},
	}
	service := NewTableService(mock)

	resp, err := service.DeleteItem(context.Background(), &models.DeleteItemRequest{
		TableName:                   "test-table",
		Key:                         map[string]models.AttributeValue{"id": {S: strPtr("1")}},
		ReturnItemCollectionMetrics: "SIZE",
	})

	if err != nil {
		t.Fatalf("DeleteItem failed: %v", err)
	}

	if resp.ItemCollectionMetrics == nil {
		t.Fatal("expected ItemCollectionMetrics, got nil")
	}
}

// stringPtr helper (duplicated or shared if exported, but local is safe)
func strPtr(s string) *string {
	return &s
}

func TestTableService_Streams(t *testing.T) {
	mock := &mockStore{
		ListStreamsFunc: func(ctx context.Context, tableName string, limit int, startArn string) ([]models.StreamSummary, string, error) {
			return []models.StreamSummary{{StreamArn: "arn"}}, "", nil
		},
		DescribeStreamFunc: func(ctx context.Context, arn string, limit int, startShard string) (*models.StreamDescription, error) {
			return &models.StreamDescription{StreamArn: arn}, nil
		},
		GetShardIteratorFunc: func(ctx context.Context, arn string, shardId string, iterType string, seq string) (string, error) {
			return "iterator", nil
		},
		GetRecordsFunc: func(ctx context.Context, iter string, limit int) ([]models.Record, string, error) {
			return []models.Record{}, "next", nil
		},
	}
	service := NewTableService(mock)

	// ListStreams
	streams, err := service.ListStreams(context.Background(), &models.ListStreamsRequest{})
	assert.NoError(t, err)
	assert.Len(t, streams.Streams, 1)

	// DescribeStream
	desc, err := service.DescribeStream(context.Background(), &models.DescribeStreamRequest{StreamArn: "arn"})
	assert.NoError(t, err)
	assert.Equal(t, "arn", desc.StreamDescription.StreamArn)

	// GetShardIterator
	iter, err := service.GetShardIterator(context.Background(), &models.GetShardIteratorRequest{StreamArn: "arn", ShardId: "shard", ShardIteratorType: "TRIM_HORIZON"})
	assert.NoError(t, err)
	assert.Equal(t, "iterator", iter.ShardIterator)

	// GetRecords
	recs, err := service.GetRecords(context.Background(), &models.GetRecordsRequest{ShardIterator: "iterator"})
	assert.NoError(t, err)
	assert.NotNil(t, recs.Records)
}

func TestTableService_BatchMethods(t *testing.T) {
	mock := &mockStore{
		BatchGetItemFunc: func(ctx context.Context, items map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
			return map[string][]map[string]models.AttributeValue{"t": {{}}}, nil, nil
		},
		BatchWriteItemFunc: func(ctx context.Context, items map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error) {
			return nil, nil
		},
	}
	service := NewTableService(mock)

	// BatchGetItem
	bg, err := service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{RequestItems: map[string]models.KeysAndAttributes{"t": {Keys: []map[string]models.AttributeValue{{"pk": {S: strPtr("1")}}}}}})
	assert.NoError(t, err)
	assert.NotEmpty(t, bg.Responses)

	// BatchWriteItem
	bw, err := service.BatchWriteItem(context.Background(), &models.BatchWriteItemRequest{RequestItems: map[string][]models.WriteRequest{"t": {{PutRequest: &models.PutRequest{Item: map[string]models.AttributeValue{"pk": {S: strPtr("1")}}}}}}})
	assert.NoError(t, err)
	assert.Empty(t, bw.UnprocessedItems)
}

func TestTableService_TransactMethods(t *testing.T) {
	mock := &mockStore{
		TransactGetItemsFunc: func(ctx context.Context, items []models.TransactGetItem) ([]models.ItemResponse, error) {
			return []models.ItemResponse{{Item: map[string]models.AttributeValue{"pk": {S: strPtr("1")}}}}, nil
		},
		TransactWriteItemsFunc: func(ctx context.Context, items []models.TransactWriteItem, token string) error {
			return nil
		},
	}
	service := NewTableService(mock)

	// TransactGetItems
	tg, err := service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{TransactItems: []models.TransactGetItem{{Get: models.GetItemRequest{TableName: "t", Key: map[string]models.AttributeValue{"pk": {S: strPtr("1")}}}}}})
	assert.NoError(t, err)
	assert.NotEmpty(t, tg.Responses)

	// TransactWriteItems
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{TransactItems: []models.TransactWriteItem{{Put: &models.PutItemRequest{TableName: "t", Item: map[string]models.AttributeValue{"pk": {S: strPtr("1")}}}}}})
	assert.NoError(t, err)
}

func TestTableService_StreamErrors(t *testing.T) {
	mock := &mockStore{
		ListStreamsFunc: func(ctx context.Context, tableName string, limit int, startArn string) ([]models.StreamSummary, string, error) {
			return nil, "", errors.New("store error")
		},
		DescribeStreamFunc: func(ctx context.Context, arn string, limit int, startShard string) (*models.StreamDescription, error) {
			return nil, errors.New("store error")
		},
		GetShardIteratorFunc: func(ctx context.Context, arn string, shardId string, iterType string, seq string) (string, error) {
			return "", errors.New("store error")
		},
		GetRecordsFunc: func(ctx context.Context, iter string, limit int) ([]models.Record, string, error) {
			return nil, "", errors.New("store error")
		},
	}
	service := NewTableService(mock)

	// ListStreams Error
	_, err := service.ListStreams(context.Background(), &models.ListStreamsRequest{})
	assert.Error(t, err)

	// DescribeStream Validation
	_, err = service.DescribeStream(context.Background(), &models.DescribeStreamRequest{StreamArn: ""})
	assert.Error(t, err)

	// DescribeStream Store Error
	_, err = service.DescribeStream(context.Background(), &models.DescribeStreamRequest{StreamArn: "arn"})
	assert.Error(t, err)

	// GetShardIterator Validation
	_, err = service.GetShardIterator(context.Background(), &models.GetShardIteratorRequest{})
	assert.Error(t, err)

	// GetShardIterator Store Error
	_, err = service.GetShardIterator(context.Background(), &models.GetShardIteratorRequest{StreamArn: "arn", ShardId: "1", ShardIteratorType: "LATEST"})
	assert.Error(t, err)

	// GetRecords Validation
	_, err = service.GetRecords(context.Background(), &models.GetRecordsRequest{ShardIterator: ""})
	assert.Error(t, err)

	// GetRecords Store Error
	_, err = service.GetRecords(context.Background(), &models.GetRecordsRequest{ShardIterator: "iter"})
	assert.Error(t, err)
}

func TestTableService_UpdateTable(t *testing.T) {
	mock := &mockStore{
		UpdateTableFunc: func(ctx context.Context, tableName string, spec *models.StreamSpecification) (*models.Table, error) {
			return &models.Table{TableName: tableName, StreamSpecification: spec}, nil
		},
	}
	service := NewTableService(mock)

	// Success
	up, err := service.UpdateTable(context.Background(), "table", &models.StreamSpecification{StreamEnabled: true})
	assert.NoError(t, err)
	assert.NotNil(t, up.StreamSpecification)
	assert.True(t, up.StreamSpecification.StreamEnabled)

	// Validation - Empty Name
	_, err = service.UpdateTable(context.Background(), "", &models.StreamSpecification{})
	assert.Error(t, err)

	// Store Error
	mock.UpdateTableFunc = func(ctx context.Context, tableName string, spec *models.StreamSpecification) (*models.Table, error) {
		return nil, errors.New("store error")
	}
	_, err = service.UpdateTable(context.Background(), "table", &models.StreamSpecification{})
	assert.Error(t, err)

	// Not Found Error
	mock.UpdateTableFunc = func(ctx context.Context, tableName string, spec *models.StreamSpecification) (*models.Table, error) {
		return nil, st.ErrTableNotFound
	}
	_, err = service.UpdateTable(context.Background(), "table", nil)
	assert.Error(t, err)
}

func TestTableService_BatchErrors(t *testing.T) {
	mock := &mockStore{}
	service := NewTableService(mock)

	// BatchGetItem Validation
	_, err := service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{})
	assert.Error(t, err)

	// BatchWriteItem Validation (Empty)
	_, err = service.BatchWriteItem(context.Background(), &models.BatchWriteItemRequest{})
	assert.Error(t, err)

	// BatchWriteItem Validation (Too many - mock not needed, logic is in service)
	manyItems := make(map[string][]models.WriteRequest)
	manyItems["t"] = make([]models.WriteRequest, 26)
	_, err = service.BatchWriteItem(context.Background(), &models.BatchWriteItemRequest{RequestItems: manyItems})
	assert.Error(t, err)

	// Store Errors
	mock.BatchGetItemFunc = func(ctx context.Context, items map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
		return nil, nil, errors.New("err")
	}
	_, err = service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{RequestItems: map[string]models.KeysAndAttributes{"t": {}}})
	assert.Error(t, err)

	mock.BatchWriteItemFunc = func(ctx context.Context, items map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error) {
		return nil, errors.New("err")
	}
	_, err = service.BatchWriteItem(context.Background(), &models.BatchWriteItemRequest{RequestItems: map[string][]models.WriteRequest{"t": {{}}}})
	assert.Error(t, err)

	// Store TableNotFound
	mock.BatchGetItemFunc = func(ctx context.Context, items map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
		return nil, nil, st.ErrTableNotFound
	}
	_, err = service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{RequestItems: map[string]models.KeysAndAttributes{"t": {}}})
	assert.Error(t, err)
}

func TestTableService_TransactErrors(t *testing.T) {
	mock := &mockStore{}
	service := NewTableService(mock)

	// TransactGet Validation
	_, err := service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{})
	assert.Error(t, err)

	// TransactWrite Validation (Empty)
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{})
	assert.Error(t, err)

	// TransactWrite Validation (Too many)
	items := make([]models.TransactWriteItem, 26)
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{TransactItems: items})
	assert.Error(t, err)

	// Store Errors
	mock.TransactGetItemsFunc = func(ctx context.Context, items []models.TransactGetItem) ([]models.ItemResponse, error) {
		return nil, errors.New("err")
	}
	_, err = service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{TransactItems: []models.TransactGetItem{{}}})
	assert.Error(t, err)

	mock.TransactWriteItemsFunc = func(ctx context.Context, items []models.TransactWriteItem, token string) error {
		return errors.New("err")
	}
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{TransactItems: []models.TransactWriteItem{{}}})
	assert.Error(t, err)

	// Table Not Found
	mock.TransactGetItemsFunc = func(ctx context.Context, items []models.TransactGetItem) ([]models.ItemResponse, error) {
		return nil, st.ErrTableNotFound
	}
	_, err = service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{TransactItems: []models.TransactGetItem{{}}})
	assert.Error(t, err)
}
