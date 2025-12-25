package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/service"
	"github.com/tabeth/concretedb/ttl"
)

func TestIntegration_TTL(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	// 1. Manually start TTL Worker for this test
	// We need 'tableService' which isn't currently exported from setupIntegrationTest app struct.
	// But we have app.store, so we can re-create it.
	ts := service.NewTableService(app.store)
	worker := ttl.NewTTLWorker(ts, 100*time.Millisecond)
	worker.Start()
	defer worker.Stop()

	tableName := "ttl-integration-table"

	// 2. Create Table
	t.Run("CreateTable", func(t *testing.T) {
		reqBody := models.CreateTableRequest{
			TableName:             tableName,
			KeySchema:             []models.KeySchemaElement{{AttributeName: "PK", KeyType: "HASH"}},
			AttributeDefinitions:  []models.AttributeDefinition{{AttributeName: "PK", AttributeType: "S"}},
			ProvisionedThroughput: models.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("CreateTable request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("CreateTable status: %d", resp.StatusCode)
		}
	})

	// 3. Enable TTL
	t.Run("EnableTTL", func(t *testing.T) {
		reqBody := models.UpdateTimeToLiveRequest{
			TableName:               tableName,
			TimeToLiveSpecification: models.TimeToLiveSpecification{Enabled: true, AttributeName: "expiry"},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateTimeToLive")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("UpdateTimeToLive request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("UpdateTimeToLive status: %d", resp.StatusCode)
		}
	})

	strPtr := func(s string) *string { return &s }

	// 4. Put Expired Item
	t.Run("PutExpiredItem", func(t *testing.T) {
		expiredTime := time.Now().Add(-1 * time.Hour).Unix()
		expiredStr := strconv.FormatInt(expiredTime, 10)

		reqBody := models.PutItemRequest{
			TableName: tableName,
			Item: map[string]models.AttributeValue{
				"PK":     {S: strPtr("expired-item")},
				"expiry": {N: strPtr(expiredStr)},
			},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PutItem request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("PutItem status: %d", resp.StatusCode)
		}
	})

	// 5. Put Valid Item
	t.Run("PutValidItem", func(t *testing.T) {
		futureTime := time.Now().Add(1 * time.Hour).Unix()
		futureStr := strconv.FormatInt(futureTime, 10)

		reqBody := models.PutItemRequest{
			TableName: tableName,
			Item: map[string]models.AttributeValue{
				"PK":     {S: strPtr("valid-item")},
				"expiry": {N: strPtr(futureStr)},
			},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PutItem request failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("PutItem status: %d", resp.StatusCode)
		}
	})

	// 6. Wait for Cleanup
	time.Sleep(1 * time.Second) // 100ms interval, 1s should be plenty

	// 7. Verify Deletion
	t.Run("VerifyDeletion", func(t *testing.T) {
		// Check expired item
		reqBody := models.GetItemRequest{
			TableName: tableName,
			Key:       map[string]models.AttributeValue{"PK": {S: strPtr("expired-item")}},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GetItem request failed: %v", err)
		}
		defer resp.Body.Close()

		var getResp models.GetItemResponse
		json.NewDecoder(resp.Body).Decode(&getResp)
		if getResp.Item != nil {
			t.Error("Expected expired-item to be deleted, but it exists")
		}

		// Check valid item
		reqBody = models.GetItemRequest{
			TableName: tableName,
			Key:       map[string]models.AttributeValue{"PK": {S: strPtr("valid-item")}},
		}
		bodyBytes, _ = json.Marshal(reqBody)
		req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GetItem request failed: %v", err)
		}
		defer resp.Body.Close()

		json.NewDecoder(resp.Body).Decode(&getResp)
		if getResp.Item == nil {
			t.Error("Expected valid-item to exist, but it is gone")
		}
	})
}
