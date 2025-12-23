package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
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
