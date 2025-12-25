package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandlers_BatchTransact_Errors(t *testing.T) {
	mockService := &mockTableService{}
	handler := NewDynamoDBHandler(mockService)

	tests := []struct {
		name   string
		target string
		body   string
		status int
	}{
		{"BatchGetItem_BadJSON", "DynamoDB_20120810.BatchGetItem", "{", http.StatusBadRequest},
		{"BatchWriteItem_BadJSON", "DynamoDB_20120810.BatchWriteItem", "{", http.StatusBadRequest},
		{"TransactGetItems_BadJSON", "DynamoDB_20120810.TransactGetItems", "{", http.StatusBadRequest},
		{"TransactWriteItems_BadJSON", "DynamoDB_20120810.TransactWriteItems", "{", http.StatusBadRequest},

		{"CreateGlobalTable_BadJSON", "DynamoDB_20120810.CreateGlobalTable", "{", http.StatusBadRequest},
		{"UpdateGlobalTable_BadJSON", "DynamoDB_20120810.UpdateGlobalTable", "{", http.StatusBadRequest},
		{"DescribeGlobalTable_BadJSON", "DynamoDB_20120810.DescribeGlobalTable", "{", http.StatusBadRequest},
		{"ListGlobalTables_BadJSON", "DynamoDB_20120810.ListGlobalTables", "{", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte(tt.body)))
			req.Header.Set("X-Amz-Target", tt.target)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			assert.Equal(t, tt.status, rr.Code)
		})
	}
}
