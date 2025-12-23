package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tabeth/concretedb/models"
)

// Represents the mocked out methods for the Table Service (table_service.go)
type mockTableService struct {
	CreateTableFunc func(ctx context.Context, table *models.Table) (*models.Table, error)
	DeleteTableFunc func(ctx context.Context, tableName string) (*models.Table, error)
}

// CreateTable is the method required to satisfy the interface.
func (m *mockTableService) CreateTable(ctx context.Context, table *models.Table) (*models.Table, error) {
	return m.CreateTableFunc(ctx, table)
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
