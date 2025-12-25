package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tabeth/concretedb/models"
)

func TestStreamsHandlers_ErrorPaths(t *testing.T) {
	mockService := &mockTableService{
		ListStreamsFunc: func(ctx context.Context, request *models.ListStreamsRequest) (*models.ListStreamsResponse, error) {
			return nil, models.New("InternalFailure", "mock error")
		},
		DescribeStreamFunc: func(ctx context.Context, request *models.DescribeStreamRequest) (*models.DescribeStreamResponse, error) {
			return nil, errors.New("generic error")
		},
		GetShardIteratorFunc: func(ctx context.Context, request *models.GetShardIteratorRequest) (*models.GetShardIteratorResponse, error) {
			// Test validation error if any, or generic
			return nil, models.New("ResourceNotFoundException", "stream not found")
		},
		GetRecordsFunc: func(ctx context.Context, request *models.GetRecordsRequest) (*models.GetRecordsResponse, error) {
			return nil, models.New("ExpiredIteratorException", "expired")
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	tests := []struct {
		name   string
		target string
		body   string
		status int
	}{
		{
			name:   "ListStreams - Bad JSON",
			target: "DynamoDB_20120810.ListStreams",
			body:   "{invalid",
			status: http.StatusBadRequest,
		},
		{
			name:   "ListStreams - Service Error",
			target: "DynamoDB_20120810.ListStreams",
			body:   "{}",
			status: http.StatusInternalServerError, // Handlers usually map errors to 400? or 500? api_handler maps generic to 500, models.Error to 400
		},
		{
			name:   "DescribeStream - Service Generic Error",
			target: "DynamoDB_20120810.DescribeStream",
			body:   `{"StreamArn":"arn:1"}`,
			status: http.StatusInternalServerError,
		},
		{
			name:   "GetShardIterator - Service Model Error",
			target: "DynamoDB_20120810.GetShardIterator",
			body:   `{"StreamArn":"arn:1", "ShardId":"s1", "ShardIteratorType":"LATEST"}`,
			status: http.StatusBadRequest, // ResourceNotFound is 400
		},
		{
			name:   "GetRecords - Service Model Error",
			target: "DynamoDB_20120810.GetRecords",
			body:   `{"ShardIterator":"iter"}`,
			status: http.StatusBadRequest, // ExpiredIterator is 400
		},
		{
			name:   "DescribeStream - Bad JSON",
			target: "DynamoDB_20120810.DescribeStream",
			body:   "{invalid",
			status: http.StatusBadRequest,
		},
		{
			name:   "GetShardIterator - Bad JSON",
			target: "DynamoDB_20120810.GetShardIterator",
			body:   "{invalid",
			status: http.StatusBadRequest,
		},
		{
			name:   "GetRecords - Bad JSON",
			target: "DynamoDB_20120810.GetRecords",
			body:   "{invalid",
			status: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(tc.body))
			req.Header.Set("X-Amz-Target", tc.target)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Errorf("Expected status %d, got %d", tc.status, resp.StatusCode)
			}
		})
	}
}
