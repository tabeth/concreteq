package service

import (
	"context"
	"errors"
	"testing"

	"github.com/tabeth/concretedb/models"
	st "github.com/tabeth/concretedb/store"
)

// mockStore is a mock implementation of the st.Store interface for testing.
type mockStore struct {
	CreateTableFunc func(ctx context.Context, table *models.Table) error
	GetTableFunc    func(ctx context.Context, tableName string) (*models.Table, error)
	DeleteTableFunc func(ctx context.Context, tableName string) (*models.Table, error) // <-- ADD THIS
	ListTablesFunc  func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error)
	// Item Operations
	PutItemFunc    func(ctx context.Context, tableName string, item map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)
	GetItemFunc    func(ctx context.Context, tableName string, key map[string]models.AttributeValue, consistentRead bool) (map[string]models.AttributeValue, error)
	DeleteItemFunc func(ctx context.Context, tableName string, key map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)
	UpdateItemFunc func(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error)
	ScanFunc       func(ctx context.Context, tableName string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error)
	QueryFunc      func(ctx context.Context, tableName string, keyConditionExpression string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error)
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

func (m *mockStore) PutItem(ctx context.Context, tableName string, item map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	return m.PutItemFunc(ctx, tableName, item, returnValues)
}

func (m *mockStore) GetItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, consistentRead bool) (map[string]models.AttributeValue, error) {
	return m.GetItemFunc(ctx, tableName, key, consistentRead)
}

func (m *mockStore) DeleteItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	return m.DeleteItemFunc(ctx, tableName, key, returnValues)
}

func (m *mockStore) UpdateItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	return m.UpdateItemFunc(ctx, tableName, key, updateExpression, exprAttrNames, exprAttrValues, returnValues)
}

func (m *mockStore) Scan(ctx context.Context, tableName string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
	return m.ScanFunc(ctx, tableName, filterExpression, projectionExpression, exprAttrNames, exprAttrValues, limit, exclusiveStartKey, consistentRead)
}

func (m *mockStore) Query(ctx context.Context, tableName string, keyConditionExpression string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
	return m.QueryFunc(ctx, tableName, keyConditionExpression, filterExpression, projectionExpression, exprAttrNames, expressionAttributeValues, limit, exclusiveStartKey, consistentRead)
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
		PutItemFunc: func(ctx context.Context, tableName string, item map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
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
		GetItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, consistentRead bool) (map[string]models.AttributeValue, error) {
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
		DeleteItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
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
		UpdateItemFunc: func(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
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
		QueryFunc: func(ctx context.Context, tableName string, keyConditionExpression string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
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
		QueryFunc: func(ctx context.Context, tableName string, keyConditionExpression string, filterExpression string, projectionExpression string, exprAttrNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
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
