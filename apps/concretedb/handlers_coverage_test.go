package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestHandlers_GlobalTables(t *testing.T) {
	mockService := &mockTableService{
		CreateGlobalTableFunc: func(ctx context.Context, req *models.CreateGlobalTableRequest) (*models.CreateGlobalTableResponse, error) {
			if req.GlobalTableName == "ErrorGT" {
				return nil, models.New("InternalFailure", "error")
			}
			return &models.CreateGlobalTableResponse{
				GlobalTableDescription: models.GlobalTableDescription{
					GlobalTableName:   req.GlobalTableName,
					GlobalTableStatus: "CREATING",
				},
			}, nil
		},
		UpdateGlobalTableFunc: func(ctx context.Context, req *models.UpdateGlobalTableRequest) (*models.UpdateGlobalTableResponse, error) {
			return &models.UpdateGlobalTableResponse{
				GlobalTableDescription: models.GlobalTableDescription{
					GlobalTableName:   req.GlobalTableName,
					GlobalTableStatus: "UPDATING",
				},
			}, nil
		},
		DescribeGlobalTableFunc: func(ctx context.Context, globalTableName string) (*models.DescribeGlobalTableResponse, error) {
			return &models.DescribeGlobalTableResponse{
				GlobalTableDescription: models.GlobalTableDescription{
					GlobalTableName:   globalTableName,
					GlobalTableStatus: "ACTIVE",
				},
			}, nil
		},
		ListGlobalTablesFunc: func(ctx context.Context, req *models.ListGlobalTablesRequest) (*models.ListGlobalTablesResponse, error) {
			return &models.ListGlobalTablesResponse{
				GlobalTables: []models.GlobalTable{{GlobalTableName: "GT1"}},
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService) // Middleware nil for testing

	// 1. CreateGlobalTable
	body, _ := json.Marshal(models.CreateGlobalTableRequest{GlobalTableName: "NewGT"})
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateGlobalTable")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	// Error case
	body, _ = json.Marshal(models.CreateGlobalTableRequest{GlobalTableName: "ErrorGT"})
	req = httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateGlobalTable")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	// 2. UpdateGlobalTable
	body, _ = json.Marshal(models.UpdateGlobalTableRequest{GlobalTableName: "NewGT"})
	req = httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateGlobalTable")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	// 3. DescribeGlobalTable
	body, _ = json.Marshal(models.DescribeGlobalTableRequest{GlobalTableName: "NewGT"})
	req = httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeGlobalTable")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	// 4. ListGlobalTables
	body, _ = json.Marshal(models.ListGlobalTablesRequest{})
	req = httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListGlobalTables")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestHandlers_UpdateTable(t *testing.T) {
	mockService := &mockTableService{
		UpdateTableFunc: func(ctx context.Context, req *models.UpdateTableRequest) (*models.Table, error) {
			return &models.Table{TableName: req.TableName, Status: models.TableStatus("UPDATING")}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)

	body, _ := json.Marshal(models.UpdateTableRequest{TableName: "Table1"})
	req := httptest.NewRequest("POST", "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateTable")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}
