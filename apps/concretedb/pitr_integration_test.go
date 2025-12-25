package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

func TestIntegration_PITR(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	tableName := "pitr-test-table"

	// 1. Create Table
	createTableReq := models.CreateTableRequest{
		TableName:             tableName,
		KeySchema:             []models.KeySchemaElement{{AttributeName: "PK", KeyType: "HASH"}},
		AttributeDefinitions:  []models.AttributeDefinition{{AttributeName: "PK", AttributeType: "S"}},
		ProvisionedThroughput: models.ProvisionedThroughput{ReadCapacityUnits: 5, WriteCapacityUnits: 5},
	}
	createBody, _ := json.Marshal(createTableReq)
	req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(createBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateTable status: %d", resp.StatusCode)
	}

	// 2. Enable PITR
	pitrReq := models.UpdateContinuousBackupsRequest{
		TableName: tableName,
		PointInTimeRecoverySpecification: models.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: true,
		},
	}
	pitrBody, _ := json.Marshal(pitrReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(pitrBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.UpdateContinuousBackups")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("UpdateContinuousBackups failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("UpdateContinuousBackups status: %d", resp.StatusCode)
	}

	strPtr := func(s string) *string { return &s }

	// 3. Put Item "A" (v1)
	putReq := models.PutItemRequest{
		TableName: tableName,
		Item: map[string]models.AttributeValue{
			"PK":   {S: strPtr("item1")},
			"Data": {S: strPtr("v1")},
		},
	}
	putBody, _ := json.Marshal(putReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(putBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PutItem v1 failed: %v", err)
	}
	resp.Body.Close()

	// Wait to ensure timestamp advances
	time.Sleep(10 * time.Millisecond)
	// Capture timestamp T1 *after* v1 is written (or valid restore time for v1)
	t1Time := float64(time.Now().UnixNano()) / 1e9

	// Wait before v2
	time.Sleep(100 * time.Millisecond)

	// 4. Update Item "A" (v2)
	putReq.Item["Data"] = models.AttributeValue{S: strPtr("v2")}
	putBody, _ = json.Marshal(putReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(putBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("PutItem v2 failed")
	}
	resp.Body.Close()

	time.Sleep(10 * time.Millisecond)
	t2Time := float64(time.Now().UnixNano()) / 1e9

	// Wait before delete
	time.Sleep(100 * time.Millisecond)

	// 5. Delete Item "A"
	delReq := models.DeleteItemRequest{
		TableName: tableName,
		Key: map[string]models.AttributeValue{
			"PK": {S: strPtr("item1")},
		},
	}
	delBody, _ := json.Marshal(delReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(delBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteItem")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteItem failed")
	}
	resp.Body.Close()

	time.Sleep(10 * time.Millisecond)
	t3Time := float64(time.Now().UnixNano()) / 1e9

	// Helper to wait for table to become ACTIVE
	waitForTableActive := func(tableName string) {
		for i := 0; i < 50; i++ {
			descReq := models.DescribeTableRequest{TableName: tableName}
			descBody, _ := json.Marshal(descReq)
			req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(descBody))
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")
			resp, err := http.DefaultClient.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				var descResp models.DescribeTableResponse
				json.NewDecoder(resp.Body).Decode(&descResp)
				resp.Body.Close()
				if descResp.Table.TableStatus == "ACTIVE" {
					return
				}
			} else if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("Table %s did not become ACTIVE", tableName)
	}

	// Helper to check item in restored table
	checkItem := func(restoredTable string, expectedData string) {
		waitForTableActive(restoredTable)

		getReq := models.GetItemRequest{
			TableName: restoredTable,
			Key: map[string]models.AttributeValue{
				"PK": {S: strPtr("item1")},
			},
		}
		getBody, _ := json.Marshal(getReq)
		req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(getBody))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GetItem on %s failed: %v", restoredTable, err)
		}
		defer resp.Body.Close()

		var getResp models.GetItemResponse
		json.NewDecoder(resp.Body).Decode(&getResp)

		if expectedData == "" {
			if getResp.Item != nil {
				t.Errorf("Expected nil item in %s, got %v", restoredTable, getResp.Item)
			}
		} else {
			if getResp.Item == nil {
				t.Errorf("Expected item %s in %s, got nil", expectedData, restoredTable)
			} else if getResp.Item["Data"].S == nil || *getResp.Item["Data"].S != expectedData {
				t.Errorf("Expected data %s in %s, got %v", expectedData, restoredTable, getResp.Item)
			}
		}
	}

	// 6. Restore to T1 (Should have v1)
	restoreT1Name := "restored-table-v1"
	restoreReq := models.RestoreTableToPointInTimeRequest{
		SourceTableName: tableName,
		TargetTableName: restoreT1Name,
		RestoreDateTime: t1Time,
	}
	restoreBody, _ := json.Marshal(restoreReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(restoreBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.RestoreTableToPointInTime")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Restore T1 failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Restore T1 status: %d", resp.StatusCode)
	}
	checkItem(restoreT1Name, "v1")

	// 7. Restore to T2 (Should have v2)
	restoreT2Name := "restored-table-v2"
	restoreReq.TargetTableName = restoreT2Name
	restoreReq.RestoreDateTime = t2Time
	restoreBody, _ = json.Marshal(restoreReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(restoreBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.RestoreTableToPointInTime")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Restore T2 failed: err=%v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Restore T2 failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	resp.Body.Close()
	checkItem(restoreT2Name, "v2")

	// 8. Restore to T3 (Should be deleted)
	restoreT3Name := "restored-table-v3"
	restoreReq.TargetTableName = restoreT3Name
	restoreReq.RestoreDateTime = t3Time
	restoreBody, _ = json.Marshal(restoreReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(restoreBody))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.RestoreTableToPointInTime")
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Restore T3 failed")
	}
	resp.Body.Close()
	checkItem(restoreT3Name, "")
}
