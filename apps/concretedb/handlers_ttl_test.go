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

func TestUpdateTimeToLiveHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		UpdateTimeToLiveFunc: func(ctx context.Context, req *models.UpdateTimeToLiveRequest) (*models.UpdateTimeToLiveResponse, error) {
			if req.TableName != "ttl-table" {
				return nil, models.New("ResourceNotFoundException", "table not found")
			}
			return &models.UpdateTimeToLiveResponse{
				TimeToLiveSpecification: req.TimeToLiveSpecification,
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "ttl-table", "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "ttl"}}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateTimeToLive")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK, got %v", resp.Status)
	}

	var respBody models.UpdateTimeToLiveResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !respBody.TimeToLiveSpecification.Enabled {
		t.Error("expected Enabled=true")
	}
}

func TestDescribeTimeToLiveHandler_Success(t *testing.T) {
	mockService := &mockTableService{
		DescribeTimeToLiveFunc: func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
			return &models.DescribeTimeToLiveResponse{
				TimeToLiveDescription: models.TimeToLiveDescription{
					TimeToLiveStatus: "ENABLED",
					AttributeName:    "ttl",
				},
			}, nil
		},
	}
	handler := NewDynamoDBHandler(mockService)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody := `{"TableName": "ttl-table"}`
	req, _ := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(reqBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTimeToLive")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status OK, got %v", resp.Status)
	}

	var respBody models.DescribeTimeToLiveResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if respBody.TimeToLiveDescription.TimeToLiveStatus != "ENABLED" {
		t.Errorf("expected ENABLED, got %s", respBody.TimeToLiveDescription.TimeToLiveStatus)
	}
}
