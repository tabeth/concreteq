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

func TestListStreamsHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		ListStreamsFunc: func(ctx context.Context, request *models.ListStreamsRequest) (*models.ListStreamsResponse, error) {
			return &models.ListStreamsResponse{
				Streams: []models.StreamSummary{
					{StreamArn: "arn:test", TableName: "TestTable"},
				},
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListStreams")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var respBody models.ListStreamsResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if len(respBody.Streams) != 1 || respBody.Streams[0].TableName != "TestTable" {
		t.Errorf("unexpected response: %+v", respBody)
	}
}

func TestDescribeStreamHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		DescribeStreamFunc: func(ctx context.Context, request *models.DescribeStreamRequest) (*models.DescribeStreamResponse, error) {
			return &models.DescribeStreamResponse{
				StreamDescription: models.StreamDescription{
					StreamArn:    "arn:test",
					StreamStatus: "ENABLED",
				},
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"StreamArn": "arn:test"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeStream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var respBody models.DescribeStreamResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if respBody.StreamDescription.StreamArn != "arn:test" {
		t.Errorf("unexpected ARN: %s", respBody.StreamDescription.StreamArn)
	}
}

func TestGetShardIteratorHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		GetShardIteratorFunc: func(ctx context.Context, request *models.GetShardIteratorRequest) (*models.GetShardIteratorResponse, error) {
			return &models.GetShardIteratorResponse{
				ShardIterator: "iterator-123",
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"StreamArn": "arn:test", "ShardId": "shard-0", "ShardIteratorType": "LATEST"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetShardIterator")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var respBody models.GetShardIteratorResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if respBody.ShardIterator != "iterator-123" {
		t.Errorf("unexpected iterator: %s", respBody.ShardIterator)
	}
}

func TestGetRecordsHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		GetRecordsFunc: func(ctx context.Context, request *models.GetRecordsRequest) (*models.GetRecordsResponse, error) {
			return &models.GetRecordsResponse{
				Records: []models.Record{
					{EventID: "evt1"},
				},
				NextShardIterator: "next-iter",
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"ShardIterator": "iterator-123"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetRecords")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var respBody models.GetRecordsResponse
	json.NewDecoder(resp.Body).Decode(&respBody)
	if len(respBody.Records) != 1 {
		t.Errorf("expected 1 record, got %d", len(respBody.Records))
	}
}
