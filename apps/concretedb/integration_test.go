package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/tabeth/concretedb/config"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/service"
	"github.com/tabeth/concretedb/store"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

type testApp struct {
	server  *http.Server
	store   *store.FoundationDBStore
	baseURL string
}

func setupIntegrationTest(t *testing.T) (*testApp, func()) {
	t.Helper()
	fdbtest.SkipIfFDBUnavailable(t)

	// In TestMain, we set up testDB. Use it.
	// We need to re-open if it's not accessible or just rely on global testDB if exported?
	// main_test.go exports testDB variable publicly.
	if testDB == (fdb.Database{}) {
		var err error
		testDB, err = fdb.OpenDefault()
		if err != nil {
			t.Fatalf("Failed to open FDB: %v", err)
		}
	}

	// Clean "tables" directory
	_, err := testDB.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Check availability first to avoid error on non-existence if Remove errors on missing
		// actually Remove returns (false, nil) if not found usually, unless standard directory layer differs.
		// Let's rely on standard behavior: returns (bool, error)
		if _, err := directory.Root().Remove(tr, []string{"tables"}); err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to clean DB: %v", err)
	}

	// Initialize Store and Service
	fdbStore := store.NewFoundationDBStore(testDB)
	tableService := service.NewTableService(fdbStore)
	handler := NewDynamoDBHandler(tableService)

	// Setup Server
	mux := http.NewServeMux()
	mux.Handle("/", handler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to find free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	baseURL := fmt.Sprintf("http://localhost:%d", port)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server
	waitForServer(t, baseURL)

	app := &testApp{
		server:  server,
		store:   fdbStore,
		baseURL: baseURL,
	}

	teardown := func() {
		server.Shutdown(context.Background())
	}

	return app, teardown
}

func waitForServer(t *testing.T, baseURL string) {
	for i := 0; i < 20; i++ {
		resp, err := http.Get(baseURL + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Server failed to start")
}

func TestIntegration_TableLifecycle(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	tableName := "integ-test-table"

	// 1. Create Table
	t.Run("CreateTable", func(t *testing.T) {
		reqBody := models.CreateTableRequest{
			TableName: tableName,
			KeySchema: []models.KeySchemaElement{
				{AttributeName: "PK", KeyType: "HASH"},
			},
			AttributeDefinitions: []models.AttributeDefinition{
				{AttributeName: "PK", AttributeType: "S"},
			},
			ProvisionedThroughput: models.ProvisionedThroughput{
				ReadCapacityUnits:  5,
				WriteCapacityUnits: 5,
			},
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
		var respBody models.CreateTableResponse
		if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
			t.Fatalf("Failed to decode CreateTable response: %v", err)
		}
		if respBody.TableDescription.TableName != tableName {
			t.Errorf("Expected TableName %s, got %s", tableName, respBody.TableDescription.TableName)
		}
		if respBody.TableDescription.TableStatus != "ACTIVE" {
			t.Errorf("Expected TableStatus ACTIVE, got %s", respBody.TableDescription.TableStatus)
		}
	})

	// 2. List Tables
	t.Run("ListTables", func(t *testing.T) {
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBufferString("{}"))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("ListTables request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("ListTables status: %d", resp.StatusCode)
		}

		var listResp models.ListTablesResponse
		json.NewDecoder(resp.Body).Decode(&listResp)

		found := false
		for _, name := range listResp.TableNames {
			if name == tableName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ListTables did not return created table %s", tableName)
		}
	})

	// 3. Describe Table
	t.Run("DescribeTable", func(t *testing.T) {
		reqBody := models.DescribeTableRequest{TableName: tableName}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DescribeTable failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("DescribeTable status: %d", resp.StatusCode)
		}
		var descResp models.DescribeTableResponse
		json.NewDecoder(resp.Body).Decode(&descResp)
		if descResp.Table.TableName != tableName {
			t.Errorf("DescribeTable TableName mismatch")
		}
	})

	// 4. Delete Table
	t.Run("DeleteTable", func(t *testing.T) {
		reqBody := models.DeleteTableRequest{TableName: tableName}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteTable")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DeleteTable failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("DeleteTable status: %d", resp.StatusCode)
		}
	})

	// 5. Verify Deletion
	t.Run("VerifyDeletion", func(t *testing.T) {
		reqBody := models.DescribeTableRequest{TableName: tableName}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("VerifyDeletion failed: %v", err)
		}
		defer resp.Body.Close()

		// Expect 400 ResourceNotFound
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected 400 after deletion, got %d", resp.StatusCode)
		}
	})
}

func TestIntegration_ItemOperations(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	tableName := "item-table"

	// Helper for checking success
	checkStatus := func(t *testing.T, resp *http.Response, expected int) {
		if resp.StatusCode != expected {
			buf := new(bytes.Buffer)
			buf.ReadFrom(resp.Body)
			t.Errorf("Expected status %d, got %d. Body: %s", expected, resp.StatusCode, buf.String())
		}
	}

	// 1. Create Table
	t.Run("CreateTable", func(t *testing.T) {
		reqBody := models.CreateTableRequest{
			TableName:             tableName,
			KeySchema:             []models.KeySchemaElement{{AttributeName: "PK", KeyType: "HASH"}},
			AttributeDefinitions:  []models.AttributeDefinition{{AttributeName: "PK", AttributeType: "S"}},
			ProvisionedThroughput: models.ProvisionedThroughput{ReadCapacityUnits: 5, WriteCapacityUnits: 5},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("CreateTable request failed: %v", err)
		}
		defer resp.Body.Close()
		checkStatus(t, resp, http.StatusOK)
	})

	strPtr := func(s string) *string { return &s }

	// 2. Put Item
	t.Run("PutItem", func(t *testing.T) {
		reqBody := models.PutItemRequest{
			TableName: tableName,
			Item: map[string]models.AttributeValue{
				"PK":   {S: strPtr("item1")},
				"Data": {S: strPtr("payload")},
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
		checkStatus(t, resp, http.StatusOK)
	})

	// 3. Get Item
	t.Run("GetItem", func(t *testing.T) {
		reqBody := models.GetItemRequest{
			TableName: tableName,
			Key: map[string]models.AttributeValue{
				"PK": {S: strPtr("item1")},
			},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GetItem request failed: %v", err)
		}
		defer resp.Body.Close()
		checkStatus(t, resp, http.StatusOK)

		var getResp models.GetItemResponse
		json.NewDecoder(resp.Body).Decode(&getResp)
		if getResp.Item == nil {
			t.Fatal("Expected item to be returned")
		}
		if getResp.Item["Data"].S == nil || *getResp.Item["Data"].S != "payload" {
			t.Errorf("Unexpected Data value: %v", getResp.Item["Data"])
		}
	})

	// 4. Delete Item
	t.Run("DeleteItem", func(t *testing.T) {
		reqBody := models.DeleteItemRequest{
			TableName: tableName,
			Key: map[string]models.AttributeValue{
				"PK": {S: strPtr("item1")},
			},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.DeleteItem")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("DeleteItem request failed: %v", err)
		}
		defer resp.Body.Close()
		checkStatus(t, resp, http.StatusOK)
	})

	// 5. Verify Item Deleted
	t.Run("VerifyItemDeleted", func(t *testing.T) {
		reqBody := models.GetItemRequest{
			TableName: tableName,
			Key: map[string]models.AttributeValue{
				"PK": {S: strPtr("item1")},
			},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GetItem request failed: %v", err)
		}
		defer resp.Body.Close()
		checkStatus(t, resp, http.StatusOK)

		var getResp models.GetItemResponse
		json.NewDecoder(resp.Body).Decode(&getResp)
		if getResp.Item != nil {
			t.Error("Expected item to be nil/empty after deletion")
		}
	})
}

func TestConfiguration(t *testing.T) {
	os.Setenv("CONCRETEDB_PORT", "9999")
	defer os.Unsetenv("CONCRETEDB_PORT")
	cfg := config.NewConfig()
	if cfg.Port != 9999 {
		t.Errorf("Expected port 9999, got %d", cfg.Port)
	}
}

func TestIntegration_BinaryKey(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	tableName := "binary-table"

	// Create Table with Binary Key
	reqBody := models.CreateTableRequest{
		TableName:             tableName,
		KeySchema:             []models.KeySchemaElement{{AttributeName: "ID", KeyType: "HASH"}},
		AttributeDefinitions:  []models.AttributeDefinition{{AttributeName: "ID", AttributeType: "B"}},
		ProvisionedThroughput: models.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateTable status: %d", resp.StatusCode)
	}

	// Put Item with Binary Key
	b64Key := "AQIDBA==" // Base64 for [1, 2, 3, 4]
	strPtr := func(s string) *string { return &s }

	putReq := models.PutItemRequest{
		TableName: tableName,
		Item: map[string]models.AttributeValue{
			"ID":  {B: strPtr(b64Key)},
			"Val": {S: strPtr("binary-val")},
		},
	}
	bodyBytes, _ = json.Marshal(putReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("PutItem status: %d", resp.StatusCode)
	}

	// Get Item
	getReq := models.GetItemRequest{
		TableName: tableName,
		Key: map[string]models.AttributeValue{
			"ID": {B: strPtr(b64Key)},
		},
	}
	bodyBytes, _ = json.Marshal(getReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GetItem failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetItem status: %d", resp.StatusCode)
	}
	var getResp models.GetItemResponse
	json.NewDecoder(resp.Body).Decode(&getResp)
	if getResp.Item == nil {
		t.Error("Expected item found")
	} else if getResp.Item["Val"].S == nil || *getResp.Item["Val"].S != "binary-val" {
		t.Errorf("Unexpected item value: %v", getResp.Item)
	}
}

func TestIntegration_InvalidKeyType(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	tableName := "invalid-key-table"

	// Create Table
	reqBody := models.CreateTableRequest{
		TableName:             tableName,
		KeySchema:             []models.KeySchemaElement{{AttributeName: "PK", KeyType: "HASH"}},
		AttributeDefinitions:  []models.AttributeDefinition{{AttributeName: "PK", AttributeType: "S"}},
		ProvisionedThroughput: models.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Put Item with Invalid Key Type (BOOL instead of S)
	boolVal := true
	putReq := models.PutItemRequest{
		TableName: tableName,
		Item: map[string]models.AttributeValue{
			"PK": {BOOL: &boolVal}, // Invalid type for FDB key construction
		},
	}
	bodyBytes, _ = json.Marshal(putReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}
	defer resp.Body.Close()

	// Should fail with Internal Error or Validation Error
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Expected 500 for invalid key type, got %d", resp.StatusCode)
	}
}

func TestIntegration_ValidationAndErrors(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	strPtr := func(s string) *string { return &s }

	// 1. Validation: Empty Table Name
	reqBody := models.PutItemRequest{
		TableName: "",
		Item:      map[string]models.AttributeValue{"id": {S: strPtr("1")}},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected 400 for empty table name, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 2. Validation: Empty Item
	reqBody = models.PutItemRequest{
		TableName: "any-table",
		Item:      map[string]models.AttributeValue{},
	}
	bodyBytes, _ = json.Marshal(reqBody)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected 400 for empty item, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 3. Table Not Found (PutItem)
	reqBody = models.PutItemRequest{
		TableName: "non-existent-table",
		Item:      map[string]models.AttributeValue{"id": {S: strPtr("1")}},
	}
	bodyBytes, _ = json.Marshal(reqBody)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	resp, _ = http.DefaultClient.Do(req)
	// Expect ResourceNotFoundException (400) or InternalFailure mapped?
	// Service maps ErrTableNotFound to ResourceNotFoundException (400)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected 400 for non-existent table put, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestIntegration_NumberKey(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	tableName := "number-key-table"

	// Create Table with Number Key
	reqBody := models.CreateTableRequest{
		TableName:             tableName,
		KeySchema:             []models.KeySchemaElement{{AttributeName: "ID", KeyType: "HASH"}},
		AttributeDefinitions:  []models.AttributeDefinition{{AttributeName: "ID", AttributeType: "N"}},
		ProvisionedThroughput: models.ProvisionedThroughput{ReadCapacityUnits: 1, WriteCapacityUnits: 1},
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateTable status: %d", resp.StatusCode)
	}

	// Put Item with Number Key
	numKey := "123.45"
	strPtr := func(s string) *string { return &s }

	putReq := models.PutItemRequest{
		TableName: tableName,
		Item: map[string]models.AttributeValue{
			"ID":  {N: strPtr(numKey)},
			"Val": {S: strPtr("num-val")},
		},
	}
	bodyBytes, _ = json.Marshal(putReq)
	req, _ = http.NewRequest("POST", app.baseURL, bytes.NewBuffer(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.PutItem")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PutItem failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("PutItem status: %d", resp.StatusCode)
	}
}
