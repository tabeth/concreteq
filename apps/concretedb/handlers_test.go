package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

// Represents the mocked out methods for the Table Service (table_service.go)
type mockTableService struct {
	CreateTableFunc func(ctx context.Context, table *models.Table) (*models.Table, error)
	DeleteTableFunc func(ctx context.Context, tableName string) (*models.Table, error)
	ListTablesFunc  func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error)
	GetTableFunc    func(ctx context.Context, tableName string) (*models.Table, error)
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

func TestCreateTableHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "broken` // Invalid JSON
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status BadRequest; got %v", resp.Status)
	}
}

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

func TestDescribeTableHandler_InvalidJSON(t *testing.T) {
	handler := NewDynamoDBHandler(&mockTableService{})
	server := httptest.NewServer(handler)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(`{`))
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
