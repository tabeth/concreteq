package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

// Represents the mocked out methods for the Table Service (table_service.go)
type mockTableService struct {
	CreateTableFunc    func(ctx context.Context, table *models.Table) (*models.Table, error)
	DeleteTableFunc    func(ctx context.Context, tableName string) (*models.Table, error)
	ListTablesFunc     func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error)
	GetTableFunc       func(ctx context.Context, tableName string) (*models.Table, error)
	PutItemFunc        func(ctx context.Context, request *models.PutItemRequest) (*models.PutItemResponse, error)
	GetItemFunc        func(ctx context.Context, request *models.GetItemRequest) (*models.GetItemResponse, error)
	DeleteItemFunc     func(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error)
	UpdateItemFunc     func(ctx context.Context, request *models.UpdateItemRequest) (*models.UpdateItemResponse, error)
	ScanFunc           func(ctx context.Context, request *models.ScanRequest) (*models.ScanResponse, error)
	QueryFunc          func(ctx context.Context, request *models.QueryRequest) (*models.QueryResponse, error)
	BatchGetItemFunc   func(ctx context.Context, request *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error)
	BatchWriteItemFunc func(ctx context.Context, request *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error)
}

// CreateTable is the method required to satisfy the interface.
func (m *mockTableService) CreateTable(ctx context.Context, table *models.Table) (*models.Table, error) {
	return m.CreateTableFunc(ctx, table)
}

func (m *mockTableService) ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
	return m.ListTablesFunc(ctx, limit, exclusiveStartTableName)
}

func (m *mockTableService) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	return m.GetTableFunc(ctx, tableName)
}

func (m *mockTableService) PutItem(ctx context.Context, request *models.PutItemRequest) (*models.PutItemResponse, error) {
	return m.PutItemFunc(ctx, request)
}

func (m *mockTableService) GetItem(ctx context.Context, request *models.GetItemRequest) (*models.GetItemResponse, error) {
	return m.GetItemFunc(ctx, request)
}

func (m *mockTableService) DeleteItem(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error) {
	return m.DeleteItemFunc(ctx, request)
}

func (m *mockTableService) UpdateItem(ctx context.Context, request *models.UpdateItemRequest) (*models.UpdateItemResponse, error) {
	return m.UpdateItemFunc(ctx, request)
}

func (m *mockTableService) Scan(ctx context.Context, request *models.ScanRequest) (*models.ScanResponse, error) {
	return m.ScanFunc(ctx, request)
}

func (m *mockTableService) Query(ctx context.Context, request *models.QueryRequest) (*models.QueryResponse, error) {
	return m.QueryFunc(ctx, request)
}

func (m *mockTableService) BatchGetItem(ctx context.Context, request *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error) {
	return m.BatchGetItemFunc(ctx, request)
}

func (m *mockTableService) BatchWriteItem(ctx context.Context, request *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error) {
	return m.BatchWriteItemFunc(ctx, request)
}

func TestCreateTableHandler_Success(t *testing.T) {
	// 1. Setup mock service
	mockService := &mockTableService{
		CreateTableFunc: func(ctx context.Context, table *models.Table) (*models.Table, error) {
			// The mock now returns a *models.Table
			return table, nil
		},
	}
	// This now works because mockTableService implements the interface NewDynamoDBHandler expects.
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	// 2. Create request
	reqBody := `{"TableName": "test-api-table", "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}], "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}]}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	// 3. Execute request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// 4. Assert response
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK; got %v", resp.Status)
	}

	var respBody models.CreateTableResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if respBody.TableDescription.TableName != "test-api-table" {
		t.Errorf("expected table name 'test-api-table', got '%s'", respBody.TableDescription.TableName)
	}
}

func TestCreateTableHandler_ResourceInUse(t *testing.T) {
	// 1. Setup mock service to return an error
	mockService := &mockTableService{
		CreateTableFunc: func(ctx context.Context, table *models.Table) (*models.Table, error) {
			// The mock now returns an models.APIError
			return nil, models.New("ResourceInUseException", "Table already exists")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	// 2. Create request
	reqBody := `{"TableName": "existing-table"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")

	// 3. Execute request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// 4. Assert response
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status Bad Request; got %v", resp.Status)
	}
	var errBody models.ErrorResponse
	json.NewDecoder(resp.Body).Decode(&errBody)
	if errBody.Type != "ResourceInUseException" {
		t.Errorf("expected error type ResourceInUseException, got %s", errBody.Type)
	}
}

func (m *mockTableService) DeleteTable(ctx context.Context, tableName string) (*models.Table, error) {
	return m.DeleteTableFunc(ctx, tableName)
}

func TestDeleteTableHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		DeleteTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			// Mock service returns a table in the DELETING state
			return &models.Table{
				TableName: tableName,
				Status:    models.StatusDeleting,
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "table-to-delete"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK; got %v", resp.Status)
	}

	var respBody models.DeleteTableResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if respBody.TableDescription.TableStatus != "DELETING" {
		t.Errorf("expected table status DELETING, got %s", respBody.TableDescription.TableStatus)
	}
}

func TestListTablesHandler(t *testing.T) {
	mockService := &mockTableService{
		ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
			return []string{"table1", "table2"}, "table2", nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK; got %v", resp.Status)
	}

	var respBody models.ListTablesResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if len(respBody.TableNames) != 2 {
		t.Errorf("expected 2 tables, got %d", len(respBody.TableNames))
	}
	if respBody.LastEvaluatedTableName != "table2" {
		t.Errorf("expected LastEvaluatedTableName 'table2', got '%s'", respBody.LastEvaluatedTableName)
	}
}

func TestDescribeTableHandler(t *testing.T) {
	mockService := &mockTableService{
		GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return &models.Table{
				TableName: "test-table",
				Status:    models.StatusActive,
				KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "id", AttributeType: "S"},
				},
				CreationDateTime: time.Now(),
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK; got %v", resp.Status)
	}

	var respBody models.DescribeTableResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if respBody.Table.TableName != "test-table" {
		t.Errorf("expected table name 'test-table', got '%s'", respBody.Table.TableName)
	}
	if respBody.Table.TableStatus != "ACTIVE" {
		t.Errorf("expected status ACTIVE, got %s", respBody.Table.TableStatus)
	}
}

// Moved to bottom for consolidation

func TestCreateTableHandler_ServiceError(t *testing.T) {
	mockService := &mockTableService{
		CreateTableFunc: func(ctx context.Context, table *models.Table) (*models.Table, error) {
			return nil, models.New("InternalFailure", "something went wrong")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status InternalServerError; got %v", resp.Status)
	}
}

func TestCreateTableHandler_GenericError(t *testing.T) {
	mockService := &mockTableService{
		CreateTableFunc: func(ctx context.Context, table *models.Table) (*models.Table, error) {
			return nil, errors.New("generic error") // Not an APIError
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

func TestDeleteTableHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestDeleteTableHandler_ServiceError(t *testing.T) {
	mockService := &mockTableService{
		DeleteTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return nil, models.New("InternalFailure", "fail")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{"TableName":"t"}`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

func TestListTablesHandler_Failure(t *testing.T) {
	mockService := &mockTableService{
		ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
			return nil, "", models.New("InternalFailure", "fail")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{}`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

func TestDescribeTableHandler_Failure(t *testing.T) {
	mockService := &mockTableService{
		GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return nil, models.New("ResourceNotFoundException", "not found")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{"TableName":"t"}`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400 (ResourceNotFound->APIError->BadRequest), got %d", resp.StatusCode)
	}
}

func TestDynamoDBHandler_InvalidMethod(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405 MethodNotAllowed, got %d", resp.StatusCode)
	}
}

func TestDynamoDBHandler_UnknownOperation(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{}`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UnknownOp")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400 BadRequest for unknown op, got %d", resp.StatusCode)
	}
}

func TestListTablesHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestHandler_MissingTarget(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{}`))
	// No X-Amz-Target header

	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for missing target, got %d", resp.StatusCode)
	}
}

func TestDescribeTableHandler_SerializationError(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{invalid`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")

	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for serialization error, got %d", resp.StatusCode)
	}
}

func TestPutItemHandler(t *testing.T) {
	// 1. Setup mock service
	mockService := &mockTableService{
		PutItemFunc: func(ctx context.Context, req *models.PutItemRequest) (*models.PutItemResponse, error) {
			if req.TableName != "test-table" {
				return nil, models.New("ValidationException", "wrong table")
			}
			if req.Item["id"].S == nil || *req.Item["id"].S != "123" {
				return nil, models.New("ValidationException", "wrong item")
			}
			return &models.PutItemResponse{}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	// 2. Request
	reqBody := `{"TableName": "test-table", "Item": {"id": {"S": "123"}, "val": {"N": "456"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestGetItemHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		GetItemFunc: func(ctx context.Context, req *models.GetItemRequest) (*models.GetItemResponse, error) {
			s := "123"
			if req.TableName == "test-table" && *req.Key["id"].S == "123" {
				return &models.GetItemResponse{
					Item: map[string]models.AttributeValue{
						"id":  {S: &s},
						"val": {S: &s},
					},
				}, nil
			}
			return &models.GetItemResponse{}, nil // Not found
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Key": {"id": {"S": "123"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var respBody models.GetItemResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if respBody.Item == nil || *respBody.Item["val"].S != "123" {
		t.Error("expected item to be returned")
	}
}

func TestGetItemHandler_NotFound(t *testing.T) {
	mockService := &mockTableService{
		GetItemFunc: func(ctx context.Context, req *models.GetItemRequest) (*models.GetItemResponse, error) {
			return &models.GetItemResponse{}, nil // Not found
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Key": {"id": {"S": "999"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	var respBody models.GetItemResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if respBody.Item != nil {
		t.Error("expected empty item")
	}
}

func TestDeleteItemHandler(t *testing.T) {
	mockService := &mockTableService{
		DeleteItemFunc: func(ctx context.Context, req *models.DeleteItemRequest) (*models.DeleteItemResponse, error) {
			return &models.DeleteItemResponse{}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Key": {"id": {"S": "123"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestPutItemHandler_Error(t *testing.T) {
	mockService := &mockTableService{
		PutItemFunc: func(ctx context.Context, req *models.PutItemRequest) (*models.PutItemResponse, error) {
			return nil, models.New("InternalFailure", "fail")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Item": {"id": {"S": "123"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

func TestGetItemHandler_Error(t *testing.T) {
	mockService := &mockTableService{
		GetItemFunc: func(ctx context.Context, req *models.GetItemRequest) (*models.GetItemResponse, error) {
			return nil, models.New("InternalFailure", "fail")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Key": {"id": {"S": "123"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

func TestDeleteItemHandler_Error(t *testing.T) {
	mockService := &mockTableService{
		DeleteItemFunc: func(ctx context.Context, req *models.DeleteItemRequest) (*models.DeleteItemResponse, error) {
			return nil, models.New("InternalFailure", "fail")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Key": {"id": {"S": "123"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

func TestPutItemHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestGetItemHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestDeleteItemHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestQueryHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		QueryFunc: func(ctx context.Context, req *models.QueryRequest) (*models.QueryResponse, error) {
			if req.TableName != "test-table" {
				return nil, models.New("ValidationException", "wrong table")
			}
			s := "item1"
			return &models.QueryResponse{
				Items: []map[string]models.AttributeValue{
					{"id": {S: &s}},
				},
				Count: 1,
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "KeyConditionExpression": "id = :v", "ExpressionAttributeValues": {":v": {"S": "item1"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Query")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var respBody models.QueryResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if len(respBody.Items) != 1 {
		t.Errorf("expected 1 item, got %d", len(respBody.Items))
	}
}

func TestQueryHandler_Failure(t *testing.T) {
	mockService := &mockTableService{
		QueryFunc: func(ctx context.Context, req *models.QueryRequest) (*models.QueryResponse, error) {
			return nil, models.New("InternalFailure", "fail")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Query")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

func TestUpdateItemHandler_Error(t *testing.T) {
	mockService := &mockTableService{
		UpdateItemFunc: func(ctx context.Context, req *models.UpdateItemRequest) (*models.UpdateItemResponse, error) {
			return nil, models.New("ResourceNotFoundException", "Table not found")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Key": {"id": {"S": "123"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestScanHandler_Error(t *testing.T) {
	mockService := &mockTableService{
		ScanFunc: func(ctx context.Context, req *models.ScanRequest) (*models.ScanResponse, error) {
			return nil, models.New("InternalFailure", "Internal error")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

func TestQueryHandler_Error(t *testing.T) {
	mockService := &mockTableService{
		QueryFunc: func(ctx context.Context, req *models.QueryRequest) (*models.QueryResponse, error) {
			return nil, models.New("ValidationException", "Invalid query")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Query")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestDescribeTableHandler_Error(t *testing.T) {
	mockService := &mockTableService{
		GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			return nil, models.New("ResourceNotFoundException", "Table not found")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}
}

func TestUpdateItemHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateItem")

	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestScanHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")

	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestScanHandler_SerializationError(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{invalid`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestQueryHandler_SerializationError(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{invalid`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Query")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestUpdateItemHandler_SerializationError(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{invalid`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateItem")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestScanHandler_ESK(t *testing.T) {
	mockService := &mockTableService{
		ScanFunc: func(ctx context.Context, req *models.ScanRequest) (*models.ScanResponse, error) {
			if req.ExclusiveStartKey != nil && *req.ExclusiveStartKey["id"].S == "start" {
				return &models.ScanResponse{}, nil
			}
			return nil, fmt.Errorf("wrong ESK")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()
	reqBody := `{"TableName": "t", "ExclusiveStartKey": {"id": {"S": "start"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestCreateTableHandler_SerializationError(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{invalid`))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestUpdateItemHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		UpdateItemFunc: func(ctx context.Context, req *models.UpdateItemRequest) (*models.UpdateItemResponse, error) {
			return &models.UpdateItemResponse{}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Key": {"id": {"S": "123"}}, "UpdateExpression": "SET #v = :v"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestUpdateItemHandler_ServiceError(t *testing.T) {
	mockService := &mockTableService{
		UpdateItemFunc: func(ctx context.Context, req *models.UpdateItemRequest) (*models.UpdateItemResponse, error) {
			return nil, models.New("InternalFailure", "fail")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "test-table", "Key": {"id": {"S": "123"}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", resp.StatusCode)
	}
}

// End of file

func TestBatchGetItemHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		BatchGetItemFunc: func(ctx context.Context, req *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error) {
			s := "item1"
			return &models.BatchGetItemResponse{
				Responses: map[string][]map[string]models.AttributeValue{
					"table1": {
						{"id": {S: &s}},
					},
				},
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"RequestItems": {"table1": {"Keys": [{"id": {"S": "1"}}]}}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.BatchGetItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var respBody models.BatchGetItemResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if len(respBody.Responses["table1"]) != 1 {
		t.Errorf("expected 1 item, got %d", len(respBody.Responses["table1"]))
	}
}

func TestBatchWriteItemHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		BatchWriteItemFunc: func(ctx context.Context, req *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error) {
			return &models.BatchWriteItemResponse{}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"RequestItems": {"table1": [{"PutRequest": {"Item": {"id": {"S": "1"}}}}]}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.BatchWriteItem")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var respBody models.BatchWriteItemResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if len(respBody.UnprocessedItems) != 0 {
		t.Errorf("expected 0 unprocessed items, got %d", len(respBody.UnprocessedItems))
	}
}
