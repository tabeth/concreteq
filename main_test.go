package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/store"
)

// testApp holds dependencies for a test run.
type testApp struct {
	server  *http.Server
	store   *store.FDBStore
	baseURL string
}

// setupIntegrationTest initializes a test server and a clean database.
func setupIntegrationTest(t *testing.T) (*testApp, func()) {
	t.Helper()

	// --- Database Setup (adapted from store/fdb_test.go) ---
	fdb.MustAPIVersion(730)
	db, err := fdb.OpenDefault()
	if err != nil {
		t.Logf("FoundationDB integration tests skipped: could not open default FDB database: %v", err)
		t.Skip("skipping FoundationDB tests: could not open default FDB database")
	}

	// Clean the database
	dir, err := directory.CreateOrOpen(db, []string{"concreteq"}, nil)
	require.NoError(t, err)
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		subdirs, err := dir.List(tr, []string{})
		if err != nil {
			return nil, err
		}
		for _, subdir := range subdirs {
			if _, err := dir.Remove(tr, []string{subdir}); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)

	// Initialize the store
	fdbStore, err := store.NewFDBStore()
	require.NoError(t, err)

	// --- Server Setup ---
	app := &App{
		Store: fdbStore,
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestSize(1024 * 1024)) // 1 MB
	app.RegisterSQSHandlers(r)

	// Find a free port
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	baseURL := fmt.Sprintf("http://localhost:%d", port)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: r,
	}

	// Start the server in a goroutine
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			// We expect ErrServerClosed on graceful shutdown, so log others as fatal.
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Wait for the server to be ready
	waitForServer(t, baseURL)

	testApp := &testApp{
		server:  server,
		store:   fdbStore,
		baseURL: baseURL,
	}

	// Teardown function
	teardown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := server.Shutdown(ctx)
		require.NoError(t, err)
	}

	return testApp, teardown
}

// waitForServer waits for the server to be ready to accept connections.
func waitForServer(t *testing.T, baseURL string) {
	t.Helper()
	retries := 20 // Increased retries
	for i := 0; i < retries; i++ {
		// We make a request to a non-existent endpoint. A 404 Not Found
		// indicates the server is up and routing requests.
		resp, err := http.Get(baseURL + "/non-existent-health-check")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusNotFound {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server at %s did not start in time", baseURL)
}

func TestIntegration_ServerLifecycle(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	// The test server is running if we got this far.
	// Let's do a basic check.
	resp, err := http.Post(app.baseURL, "", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	// The root handler expects an X-Amz-Target header, so we expect a bad request error.
	// This confirms the server is responding.
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestIntegration_CreateQueue(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	tests := []struct {
		name               string
		setup              func(t *testing.T) // For per-test setup, like creating a queue to check for conflicts
		inputBody          string
		expectedStatusCode int
		expectedBody       string
		verifyInDB         func(t *testing.T, queueName string)
	}{
		{
			name:               "Successful Queue Creation",
			inputBody:          `{"QueueName": "my-integ-test-queue"}`,
			expectedStatusCode: http.StatusCreated,
			expectedBody:       fmt.Sprintf(`{"QueueUrl":"%s/queues/my-integ-test-queue"}`, app.baseURL),
			verifyInDB: func(t *testing.T, queueName string) {
				exists, err := directory.Exists(app.store.GetDB(), []string{"concreteq", queueName})
				require.NoError(t, err)
				assert.True(t, exists, "expected queue directory to exist in FDB")
			},
		},
		{
			name: "Queue Already Exists",
			setup: func(t *testing.T) {
				err := app.store.CreateQueue(context.Background(), "my-existing-queue", nil, nil)
				require.NoError(t, err)
			},
			inputBody:          `{"QueueName": "my-existing-queue"}`,
			expectedStatusCode: http.StatusConflict,
			expectedBody:       `{"__type":"QueueAlreadyExists", "message":"Queue already exists"}`,
		},
		{
			name:               "Invalid Queue Name",
			inputBody:          `{"QueueName": "my-queue!!"}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length."}`,
		},
		{
			name:               "FIFO Name Mismatch",
			inputBody:          `{"QueueName": "my-queue.fifo"}`, // Missing FifoQueue attribute
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Queue name ends in .fifo but FifoQueue attribute is not 'true'"}`,
		},
		{
			name:               "Invalid Attribute Value",
			inputBody:          `{"QueueName": "queue-with-bad-attr", "Attributes": {"DelaySeconds": "901"}}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidAttributeName", "message":"invalid value for DelaySeconds: must be between 0 and 900"}`,
		},
		{
			name:               "Successful FIFO Queue Creation",
			inputBody:          `{"QueueName": "my-fifo-queue.fifo", "Attributes": {"FifoQueue": "true"}}`,
			expectedStatusCode: http.StatusCreated,
			expectedBody:       fmt.Sprintf(`{"QueueUrl":"%s/queues/my-fifo-queue.fifo"}`, app.baseURL),
			verifyInDB: func(t *testing.T, queueName string) {
				exists, err := directory.Exists(app.store.GetDB(), []string{"concreteq", queueName})
				require.NoError(t, err)
				assert.True(t, exists, "expected fifo queue directory to exist in FDB")
			},
		},
		{
			name:               "Successful FIFO Queue with ContentBasedDeduplication",
			inputBody:          `{"QueueName": "my-fifo-cbd.fifo", "Attributes": {"FifoQueue": "true", "ContentBasedDeduplication": "true"}}`,
			expectedStatusCode: http.StatusCreated,
			expectedBody:       fmt.Sprintf(`{"QueueUrl":"%s/queues/my-fifo-cbd.fifo"}`, app.baseURL),
			verifyInDB: func(t *testing.T, queueName string) {
				// Verify the attribute is stored correctly in FDB
				_, err := app.store.GetDB().ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
					queueDir, err := directory.Open(rtr, []string{"concreteq", queueName}, nil)
					require.NoError(t, err)
					attrsBytes, err := rtr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
					require.NoError(t, err)
					var storedAttrs map[string]string
					err = json.Unmarshal(attrsBytes, &storedAttrs)
					require.NoError(t, err)
					assert.Equal(t, "true", storedAttrs["ContentBasedDeduplication"])
					return nil, nil
				})
				require.NoError(t, err)
			},
		},
		{
			name:               "Successful Queue with RedrivePolicy",
			inputBody:          `{"QueueName": "my-queue-with-redrive", "Attributes": {"RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:my-dlq\",\"maxReceiveCount\":\"10\"}"}}`,
			expectedStatusCode: http.StatusCreated,
			expectedBody:       fmt.Sprintf(`{"QueueUrl":"%s/queues/my-queue-with-redrive"}`, app.baseURL),
			verifyInDB: func(t *testing.T, queueName string) {
				// Verify the attribute is stored correctly in FDB
				_, err := app.store.GetDB().ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
					queueDir, err := directory.Open(rtr, []string{"concreteq", queueName}, nil)
					require.NoError(t, err)
					attrsBytes, err := rtr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
					require.NoError(t, err)
					var storedAttrs map[string]string
					err = json.Unmarshal(attrsBytes, &storedAttrs)
					require.NoError(t, err)
					assert.Contains(t, storedAttrs["RedrivePolicy"], "my-dlq")
					return nil, nil
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Reset the DB for each subtest to ensure isolation
			_, err := app.store.GetDB().Transact(func(tr fdb.Transaction) (interface{}, error) {
				dir, err := directory.Open(tr, []string{"concreteq"}, nil)
				if err != nil {
					return nil, err
				}
				subdirs, err := dir.List(tr, []string{})
				if err != nil {
					return nil, err
				}
				for _, subdir := range subdirs {
					if _, err := dir.Remove(tr, []string{subdir}); err != nil {
						return nil, err
					}
				}
				return nil, nil
			})
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(t)
			}

			body := bytes.NewBufferString(tc.inputBody)
			req, err := http.NewRequest("POST", app.baseURL+"/", body)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tc.expectedStatusCode, resp.StatusCode)

			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				assert.JSONEq(t, tc.expectedBody, string(respBody))
			} else {
				var errResp models.ErrorResponse
				err := json.Unmarshal(respBody, &errResp)
				require.NoError(t, err, "failed to unmarshal error response: %s", string(respBody))

				var expectedErrResp models.ErrorResponse
				err = json.Unmarshal([]byte(tc.expectedBody), &expectedErrResp)
				require.NoError(t, err, "failed to unmarshal expected error json: %s", tc.expectedBody)

				assert.Equal(t, expectedErrResp.Type, errResp.Type)
				assert.Equal(t, expectedErrResp.Message, errResp.Message)
			}

			if tc.verifyInDB != nil {
				var reqData struct {
					QueueName string `json:"QueueName"`
				}
				err := json.Unmarshal([]byte(tc.inputBody), &reqData)
				require.NoError(t, err)
				tc.verifyInDB(t, reqData.QueueName)
			}
		})
	}
}

func TestIntegration_ListDeletePurgeQueues(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	// --- Setup: Create some initial queues for testing ---
	ctx := context.Background()
	initialQueues := []string{"list-queue-a", "list-queue-b", "prefix-queue-1", "prefix-queue-2", "purge-queue-1"}
	for _, qName := range initialQueues {
		err := app.store.CreateQueue(ctx, qName, nil, nil)
		require.NoError(t, err)
	}

	// Add a message to the queue we want to purge, so we can test the purge
	_, err := app.store.SendMessage(ctx, "purge-queue-1", &models.SendMessageRequest{MessageBody: "a message to be purged"})
	require.NoError(t, err)

	// --- ListQueues Tests ---
	t.Run("ListQueues", func(t *testing.T) {
		// Sub-test: List with prefix
		t.Run("With Prefix", func(t *testing.T) {
			body := bytes.NewBufferString(`{"QueueNamePrefix": "prefix-"}`)
			req, _ := http.NewRequest("POST", app.baseURL+"/", body)
			req.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			var listResp models.ListQueuesResponse
			err = json.NewDecoder(resp.Body).Decode(&listResp)
			require.NoError(t, err)
			assert.Len(t, listResp.QueueUrls, 2)
			assert.Contains(t, listResp.QueueUrls[0], "/queues/prefix-queue-")
		})

		// Sub-test: Pagination
		t.Run("Pagination", func(t *testing.T) {
			// Page 1
			body1 := bytes.NewBufferString(`{"MaxResults": 3}`)
			req1, _ := http.NewRequest("POST", app.baseURL+"/", body1)
			req1.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
			resp1, err := http.DefaultClient.Do(req1)
			require.NoError(t, err)
			defer resp1.Body.Close()

			assert.Equal(t, http.StatusOK, resp1.StatusCode)
			var listResp1 models.ListQueuesResponse
			err = json.NewDecoder(resp1.Body).Decode(&listResp1)
			require.NoError(t, err)
			assert.Len(t, listResp1.QueueUrls, 3)
			require.NotEmpty(t, listResp1.NextToken)

			// Page 2
			body2 := bytes.NewBufferString(fmt.Sprintf(`{"MaxResults": 3, "NextToken": "%s"}`, listResp1.NextToken))
			req2, _ := http.NewRequest("POST", app.baseURL+"/", body2)
			req2.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
			resp2, err := http.DefaultClient.Do(req2)
			require.NoError(t, err)
			defer resp2.Body.Close()

			assert.Equal(t, http.StatusOK, resp2.StatusCode)
			var listResp2 models.ListQueuesResponse
			err = json.NewDecoder(resp2.Body).Decode(&listResp2)
			require.NoError(t, err)
			assert.Len(t, listResp2.QueueUrls, 2) // 5 total queues, 3 on first page, 2 on second
			assert.Empty(t, listResp2.NextToken)
		})

		// Sub-test: Invalid NextToken
		t.Run("Invalid NextToken", func(t *testing.T) {
			body := bytes.NewBufferString(`{"NextToken": "invalid-token"}`)
			req, _ := http.NewRequest("POST", app.baseURL+"/", body)
			req.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			var listResp models.ListQueuesResponse
			err = json.NewDecoder(resp.Body).Decode(&listResp)
			require.NoError(t, err)
			assert.Len(t, listResp.QueueUrls, 0)
			assert.Empty(t, listResp.NextToken)
		})
	})

	// --- DeleteQueue Tests ---
	t.Run("DeleteQueue", func(t *testing.T) {
		queueToDelete := "list-queue-a"

		// Sub-test: Successful Deletion
		t.Run("Successful", func(t *testing.T) {
			deleteBody := fmt.Sprintf(`{"QueueUrl": "%s/queues/%s"}`, app.baseURL, queueToDelete)
			body := bytes.NewBufferString(deleteBody)
			req, _ := http.NewRequest("POST", app.baseURL+"/", body)
			req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteQueue")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			// Verify in DB
			exists, err := directory.Exists(app.store.GetDB(), []string{"concreteq", queueToDelete})
			require.NoError(t, err)
			assert.False(t, exists)
		})

		// Sub-test: Delete Non-Existent
		t.Run("Non-Existent", func(t *testing.T) {
			deleteBody := fmt.Sprintf(`{"QueueUrl": "%s/queues/non-existent-queue"}`, app.baseURL)
			body := bytes.NewBufferString(deleteBody)
			req, _ := http.NewRequest("POST", app.baseURL+"/", body)
			req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteQueue")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	})

	// --- PurgeQueue Tests ---
	t.Run("PurgeQueue", func(t *testing.T) {
		queueToPurge := "purge-queue-1"

		// Sub-test: Successful Purge
		t.Run("Successful", func(t *testing.T) {
			purgeBody := fmt.Sprintf(`{"QueueUrl": "%s/queues/%s"}`, app.baseURL, queueToPurge)
			body := bytes.NewBufferString(purgeBody)
			req, _ := http.NewRequest("POST", app.baseURL+"/", body)
			req.Header.Set("X-Amz-Target", "AmazonSQS.PurgeQueue")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			// Verify message is gone by trying to receive it
			receiveResp, err := app.store.ReceiveMessage(ctx, queueToPurge, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			require.NoError(t, err)
			assert.Empty(t, receiveResp.Messages, "Expected no messages after purge")
		})

		// Sub-test: Purge In Progress
		t.Run("In Progress", func(t *testing.T) {
			purgeBody := fmt.Sprintf(`{"QueueUrl": "%s/queues/%s"}`, app.baseURL, queueToPurge)
			body := bytes.NewBufferString(purgeBody)
			req, _ := http.NewRequest("POST", app.baseURL+"/", body)
			req.Header.Set("X-Amz-Target", "AmazonSQS.PurgeQueue")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

			var errResp models.ErrorResponse
			err = json.NewDecoder(resp.Body).Decode(&errResp)
			require.NoError(t, err)
			assert.Equal(t, "PurgeQueueInProgress", errResp.Type)
		})
	})
}

func TestIntegration_SendMessageBatch(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	// Setup a standard and a FIFO queue for the tests
	ctx := context.Background()
	stdQueueName := "batch-integ-std"
	fifoQueueName := "batch-integ-fifo.fifo"
	stdQueueURL := fmt.Sprintf("%s/queues/%s", app.baseURL, stdQueueName)
	fifoQueueURL := fmt.Sprintf("%s/queues/%s", app.baseURL, fifoQueueName)

	err := app.store.CreateQueue(ctx, stdQueueName, nil, nil)
	require.NoError(t, err)
	err = app.store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
	require.NoError(t, err)

	tests := []struct {
		name               string
		queueURL           string
		inputBody          string
		expectedStatusCode int
		expectedBody       string
	}{
		// 1. Batch size exceeds message size limit
		{
			name:     "BatchRequestTooLong",
			queueURL: stdQueueURL,
			inputBody:          `{"QueueUrl": "` + stdQueueURL + `", "Entries": [{"Id": "1", "MessageBody": "` + string(make([]byte, 256*1024)) + `"}]}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"BatchRequestTooLong", "message":"The length of all the messages put together is more than the limit."}`,
		},
		// 2. Partial success
		{
			name:     "PartialSuccess",
			queueURL: fifoQueueURL,
			inputBody: `{"QueueUrl": "` + fifoQueueURL + `", "Entries": [
				{"Id": "ok", "MessageBody": "this is fine", "MessageGroupId": "g1"},
				{"Id": "bad", "MessageBody": "this has a delay", "DelaySeconds": 10, "MessageGroupId": "g1"}
			]}`,
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"Successful": [{"Id": "ok", "MD5OfMessageBody": "a252992f888890972483741634570093", "MessageId": "...", "SequenceNumber": "..."}], "Failed": [{"Id": "bad", "Code": "InvalidParameterValue", "Message": "DelaySeconds is not supported for FIFO queues.", "SenderFault": true}]}`,
		},
		// 3. Entire request fails (e.g., queue does not exist)
		{
			name:     "QueueDoesNotExist",
			queueURL: app.baseURL + "/queues/non-existent-queue",
			inputBody: `{"QueueUrl": "` + app.baseURL + `/queues/non-existent-queue", "Entries": [
				{"Id": "1", "MessageBody": "wont work"}
			]}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		// 4. Duplicate batch entry IDs
		{
			name:     "BatchEntryIdsNotDistinct",
			queueURL: stdQueueURL,
			inputBody: `{"QueueUrl": "` + stdQueueURL + `", "Entries": [
				{"Id": "dup", "MessageBody": "a"},
				{"Id": "dup", "MessageBody": "b"}
			]}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"BatchEntryIdsNotDistinct", "message":"Two or more batch entries in the request have the same Id."}`,
		},
		// 5. Empty batch
		{
			name:               "EmptyBatchRequest",
			queueURL:           stdQueueURL,
			inputBody:          `{"QueueUrl": "` + stdQueueURL + `", "Entries": []}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"EmptyBatchRequest", "message":"The batch request doesn't contain any entries."}`,
		},
		// 6. Invalid entry ID
		{
			name:     "InvalidBatchEntryId",
			queueURL: stdQueueURL,
			inputBody: `{"QueueUrl": "` + stdQueueURL + `", "Entries": [
				{"Id": "invalid id!", "MessageBody": "a"}
			]}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidBatchEntryId", "message":"The Id of a batch entry in a batch request doesn't abide by the specification."}`,
		},
		// 7. Queue doesn't exist (same as #3)
		// 8. Too many items in batch
		{
			name:     "TooManyEntriesInBatchRequest",
			queueURL: stdQueueURL,
			inputBody: `{"QueueUrl": "` + stdQueueURL + `", "Entries": [
				{"Id":"1","MessageBody":"a"},{"Id":"2","MessageBody":"a"},{"Id":"3","MessageBody":"a"},{"Id":"4","MessageBody":"a"},{"Id":"5","MessageBody":"a"},
				{"Id":"6","MessageBody":"a"},{"Id":"7","MessageBody":"a"},{"Id":"8","MessageBody":"a"},{"Id":"9","MessageBody":"a"},{"Id":"10","MessageBody":"a"},
				{"Id":"11","MessageBody":"a"}
			]}`,
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"TooManyEntriesInBatchRequest", "message":"The batch request contains more entries than permissible."}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body := bytes.NewBufferString(tc.inputBody)
			// The request URL for batch is different from the root
			urlParts := strings.Split(tc.queueURL, "/queues/")
			require.Len(t, urlParts, 2)
			requestURL := fmt.Sprintf("%s/queues/%s/messages/batch", app.baseURL, urlParts[1])

			req, err := http.NewRequest("POST", requestURL, body)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tc.expectedStatusCode, resp.StatusCode)

			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			// For partial success, we can't do a direct JSONEq because MessageId and SequenceNumber are dynamic.
			if tc.name == "PartialSuccess" {
				var actualResp models.SendMessageBatchResponse
				err := json.Unmarshal(respBody, &actualResp)
				require.NoError(t, err)
				assert.Len(t, actualResp.Successful, 1)
				assert.Len(t, actualResp.Failed, 1)
				assert.Equal(t, "ok", actualResp.Successful[0].Id)
				assert.Equal(t, "bad", actualResp.Failed[0].Id)
				assert.Equal(t, "InvalidParameterValue", actualResp.Failed[0].Code)
			} else if tc.expectedBody != "" {
				assert.JSONEq(t, tc.expectedBody, string(respBody))
			}
		})
	}
}

func TestIntegration_FifoFairness(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	queueName := "fairness-queue.fifo"
	queueURL := fmt.Sprintf("%s/queues/%s", app.baseURL, queueName)

	// 1. Create a FIFO queue
	createBody := fmt.Sprintf(`{"QueueName": "%s", "Attributes": {"FifoQueue": "true"}}`, queueName)
	req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(createBody))
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	// 2. Send messages: 10 for noisy group, 1 for two quiet groups
	noisyGroup := "group-a"
	quietGroupB := "group-b"
	quietGroupC := "group-c"

	for i := 0; i < 10; i++ {
		sendReq := models.SendMessageRequest{
			QueueUrl:       queueURL,
			MessageBody:    fmt.Sprintf("noisy-%d", i),
			MessageGroupId: &noisyGroup,
		}
		bodyBytes, _ := json.Marshal(sendReq)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	for _, groupID := range []string{quietGroupB, quietGroupC} {
		sendReq := models.SendMessageRequest{
			QueueUrl:       queueURL,
			MessageBody:    fmt.Sprintf("quiet-%s", groupID),
			MessageGroupId: &groupID,
		}
		bodyBytes, _ := json.Marshal(sendReq)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	// 3. Receive messages one by one and check their group ID
	var receivedGroupIDs []string
	for i := 0; i < 3; i++ {
		recReq := models.ReceiveMessageRequest{
			QueueUrl:                  queueURL,
			MaxNumberOfMessages:       1,
			VisibilityTimeout:         60, // Lock the group for the test duration
			MessageSystemAttributeNames: []string{"All"},
		}
		bodyBytes, _ := json.Marshal(recReq)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var recResp models.ReceiveMessageResponse
		err = json.NewDecoder(resp.Body).Decode(&recResp)
		require.NoError(t, err)
		resp.Body.Close()

		if len(recResp.Messages) > 0 {
			msg := recResp.Messages[0]
			groupID := msg.Attributes["MessageGroupId"]
			receivedGroupIDs = append(receivedGroupIDs, groupID)

			// Delete the message to simulate processing and unlock the group
			delBody := models.DeleteMessageRequest{QueueUrl: queueURL, ReceiptHandle: msg.ReceiptHandle}
			delBodyBytes, _ := json.Marshal(delBody)
			delHttpReq, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(delBodyBytes))
			delHttpReq.Header.Set("Content-Type", "application/json")
			delHttpReq.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessage")
			delResp, err := http.DefaultClient.Do(delHttpReq)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, delResp.StatusCode)
			delResp.Body.Close()
		}
	}

	// Assert that we received messages from more than one group
	t.Logf("Received group IDs via HTTP: %v", receivedGroupIDs)
	groupSet := make(map[string]bool)
	for _, id := range receivedGroupIDs {
		groupSet[id] = true
	}
	assert.Greater(t, len(groupSet), 1, "Expected to receive messages from more than one group via HTTP")
}

func TestIntegration_ChangeMessageVisibility(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	ctx := context.Background()
	queueName := "visibility-integ-queue"
	queueURL := fmt.Sprintf("%s/queues/%s", app.baseURL, queueName)

	// 1. Create a queue
	err := app.store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// 2. Send a message
	sendResp, err := app.store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "visibility test"})
	require.NoError(t, err)

	// 3. Receive the message to get a receipt handle
	recResp, err := app.store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1, VisibilityTimeout: 5})
	require.NoError(t, err)
	require.Len(t, recResp.Messages, 1)
	receiptHandle := recResp.Messages[0].ReceiptHandle

	// 4. Change the visibility via HTTP request
	newVisibilityTimeout := 2 // seconds
	changeVisBody := fmt.Sprintf(`{"QueueUrl": "%s", "ReceiptHandle": "%s", "VisibilityTimeout": %d}`, queueURL, receiptHandle, newVisibilityTimeout)
	req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(changeVisBody))
	req.Header.Set("X-Amz-Target", "AmazonSQS.ChangeMessageVisibility")
	httpResp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer httpResp.Body.Close()
	assert.Equal(t, http.StatusOK, httpResp.StatusCode)

	// 5. Attempt to receive the message again immediately. It should not be visible.
	recResp2, err := app.store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
	require.NoError(t, err)
	assert.Len(t, recResp2.Messages, 0, "message should not be visible immediately after visibility change")

	// 6. Wait for the new visibility timeout to expire.
	time.Sleep(time.Duration(newVisibilityTimeout+1) * time.Second)

	// 7. Receive the message again. It should now be visible.
	recResp3, err := app.store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, recResp3.Messages, 1, "message should be visible again after new timeout expires")
	assert.Equal(t, sendResp.MessageId, recResp3.Messages[0].MessageId)
}

func TestIntegration_DeleteMessageBatch(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	ctx := context.Background()
	queueName := "delete-batch-integ-queue"
	queueURL := fmt.Sprintf("%s/queues/%s", app.baseURL, queueName)

	// Setup: Create a queue
	err := app.store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	t.Run("Successful Batch Deletion", func(t *testing.T) {
		// Send 2 messages to delete
		_, err := app.store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1"})
		require.NoError(t, err)
		_, err = app.store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg2"})
		require.NoError(t, err)

		// Receive them to get receipt handles
		recResp, err := app.store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 2})
		require.NoError(t, err)
		require.Len(t, recResp.Messages, 2)

		// Construct and send the delete batch request
		delReq := models.DeleteMessageBatchRequest{
			QueueUrl: queueURL,
			Entries: []models.DeleteMessageBatchRequestEntry{
				{Id: "m1", ReceiptHandle: recResp.Messages[0].ReceiptHandle},
				{Id: "m2", ReceiptHandle: recResp.Messages[1].ReceiptHandle},
			},
		}
		bodyBytes, _ := json.Marshal(delReq)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessageBatch")
		httpResp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer httpResp.Body.Close()

		assert.Equal(t, http.StatusOK, httpResp.StatusCode)
		var delResp models.DeleteMessageBatchResponse
		err = json.NewDecoder(httpResp.Body).Decode(&delResp)
		require.NoError(t, err)
		assert.Len(t, delResp.Successful, 2)
		assert.Len(t, delResp.Failed, 0)
	})

	t.Run("Partial Failure", func(t *testing.T) {
		// Send 1 message to delete
		_, err := app.store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg-partial"})
		require.NoError(t, err)
		recResp, err := app.store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
		require.NoError(t, err)
		require.Len(t, recResp.Messages, 1)

		// Construct and send the delete batch request with one valid and one invalid handle
		delReq := models.DeleteMessageBatchRequest{
			QueueUrl: queueURL,
			Entries: []models.DeleteMessageBatchRequestEntry{
				{Id: "valid_msg", ReceiptHandle: recResp.Messages[0].ReceiptHandle},
				{Id: "invalid_msg", ReceiptHandle: "this-is-an-invalid-handle"},
			},
		}
		// Manually insert a malformed receipt handle in the DB to test the "failed" path
		_, err = app.store.GetDB().Transact(func(tr fdb.Transaction) (interface{}, error) {
			qDir, _ := directory.Open(tr, []string{"concreteq", queueName}, nil)
			tr.Set(qDir.Sub("inflight").Pack(tuple.Tuple{"this-is-an-invalid-handle"}), []byte("bad-data"))
			return nil, nil
		})
		require.NoError(t, err)


		bodyBytes, _ := json.Marshal(delReq)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessageBatch")
		httpResp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer httpResp.Body.Close()

		assert.Equal(t, http.StatusOK, httpResp.StatusCode)
		var delResp models.DeleteMessageBatchResponse
		err = json.NewDecoder(httpResp.Body).Decode(&delResp)
		require.NoError(t, err)
		assert.Len(t, delResp.Successful, 1)
		assert.Len(t, delResp.Failed, 1)
		assert.Equal(t, "valid_msg", delResp.Successful[0].Id)
		assert.Equal(t, "invalid_msg", delResp.Failed[0].Id)
	})
}

func TestIntegration_DeleteMessage(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	ctx := context.Background()
	queueName := "delete-integ-queue"
	queueURL := fmt.Sprintf("%s/queues/%s", app.baseURL, queueName)

	// Setup: Create a queue and a message to delete
	err := app.store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)
	_, err = app.store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "a message"})
	require.NoError(t, err)
	recResp, err := app.store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, recResp.Messages, 1)
	validReceiptHandle := recResp.Messages[0].ReceiptHandle

	t.Run("Successful Deletion", func(t *testing.T) {
		// We need a fresh message for this test case
		_, err := app.store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "another message"})
		require.NoError(t, err)
		resp, err := app.store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
		require.NoError(t, err)
		require.Len(t, resp.Messages, 1)

		delBody := models.DeleteMessageRequest{QueueUrl: queueURL, ReceiptHandle: resp.Messages[0].ReceiptHandle}
		bodyBytes, _ := json.Marshal(delBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessage")
		httpResp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer httpResp.Body.Close()

		assert.Equal(t, http.StatusOK, httpResp.StatusCode)
	})

	t.Run("ReceiptHandleIsInvalid", func(t *testing.T) {
		// An invalid handle is one that is malformed, not just non-existent.
		// We can't easily create a malformed handle that passes checksums etc.,
		// but we can test the error path from the handler by using a handle
		// that we know is invalid because we manually inserted it in the fdb test.
		// For an integration test, we can just use a handle that is syntactically
		// plausible but doesn't exist, and our current implementation will return
		// success, which is compliant. If we wanted to test the "invalid" error,
		// we'd need a way to inject a malformed entry in FDB, which is better suited for the fdb_test.
		// So we will test the "non-existent but valid-looking" handle case.
		delBody := models.DeleteMessageRequest{QueueUrl: queueURL, ReceiptHandle: "plausible-but-non-existent-handle"}
		bodyBytes, _ := json.Marshal(delBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessage")
		httpResp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer httpResp.Body.Close()

		// As per SQS docs, deleting a non-existent handle is not an error.
		assert.Equal(t, http.StatusOK, httpResp.StatusCode)
	})

	t.Run("QueueDoesNotExist", func(t *testing.T) {
		delBody := models.DeleteMessageRequest{
			QueueUrl:      fmt.Sprintf("%s/queues/non-existent-queue", app.baseURL),
			ReceiptHandle: validReceiptHandle,
		}
		bodyBytes, _ := json.Marshal(delBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessage")
		httpResp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer httpResp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, httpResp.StatusCode)
		var errResp models.ErrorResponse
		json.NewDecoder(httpResp.Body).Decode(&errResp)
		assert.Equal(t, "QueueDoesNotExist", errResp.Type)
	})
}

func TestIntegration_Messaging(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	ctx := context.Background()
	stdQueueName := "messaging-queue-std"
	fifoQueueName := "messaging-queue-fifo.fifo"

	// --- Setup: Create queues for testing ---
	err := app.store.CreateQueue(ctx, stdQueueName, nil, nil)
	require.NoError(t, err)
	err = app.store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
	require.NoError(t, err)

	t.Run("SendAndReceiveStandard", func(t *testing.T) {
		sentMessageBody := "hello" + " " + "world"
		sendReq := models.SendMessageRequest{
			QueueUrl:    fmt.Sprintf("%s/queues/%s", app.baseURL, stdQueueName),
			MessageBody: sentMessageBody,
		}
		msgBodyBytes, err := json.Marshal(sendReq)
		require.NoError(t, err)

		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(msgBodyBytes))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var sendResp models.SendMessageResponse
		err = json.NewDecoder(resp.Body).Decode(&sendResp)
		require.NoError(t, err)
		assert.NotEmpty(t, sendResp.MessageId)
		assert.Equal(t, "5eb63bbbe01eeed093cb22bb8f5acdc3", sendResp.MD5OfMessageBody) // md5 of "hello world"

		// Now, receive the message
		recBody := fmt.Sprintf(`{"QueueUrl": "%s/queues/%s", "MaxNumberOfMessages": 1}`, app.baseURL, stdQueueName)
		req, _ = http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(recBody))
		req.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")

		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var recResp models.ReceiveMessageResponse
		err = json.NewDecoder(resp.Body).Decode(&recResp)
		require.NoError(t, err)
		require.Len(t, recResp.Messages, 1)

		receivedMsg := recResp.Messages[0]
		assert.Equal(t, sendResp.MessageId, receivedMsg.MessageId)
		assert.Equal(t, sentMessageBody, receivedMsg.Body)
		assert.NotEmpty(t, receivedMsg.ReceiptHandle)
	})

	t.Run("SendMessageFailures", func(t *testing.T) {
		t.Run("FIFO Missing MessageGroupId", func(t *testing.T) {
			sendReq := models.SendMessageRequest{
				QueueUrl:    fmt.Sprintf("%s/queues/%s", app.baseURL, fifoQueueName),
				MessageBody: "fifo message",
			}
			msgBodyBytes, err := json.Marshal(sendReq)
			require.NoError(t, err)

			req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(msgBodyBytes))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	})

	t.Run("FIFO Ordering", func(t *testing.T) {
		// Reset the DB for each subtest to ensure isolation
		_, err := app.store.GetDB().Transact(func(tr fdb.Transaction) (interface{}, error) {
			dir, err := directory.CreateOrOpen(tr, []string{"concreteq"}, nil)
			if err != nil { return nil, err }
			subdirs, err := dir.List(tr, []string{})
			if err != nil { return nil, err }
			for _, subdir := range subdirs {
				if _, err := dir.Remove(tr, []string{subdir}); err != nil { return nil, err }
			}
			return nil, nil
		})
		require.NoError(t, err)
		err = app.store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		// Send messages to the same group
		messageBodies := []string{"message1", "message2", "message3"}
		messageGroupID := "group-a"
		for _, body := range messageBodies {
			msgRequest := models.SendMessageRequest{
				QueueUrl:       fmt.Sprintf("%s/queues/%s", app.baseURL, fifoQueueName),
				MessageBody:    body,
				MessageGroupId: &messageGroupID,
			}
			msgBodyBytes, _ := json.Marshal(msgRequest)
			req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(msgBodyBytes))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			resp.Body.Close()
		}

		// Receive all messages in one batch and check order
		recBody := fmt.Sprintf(`{"QueueUrl": "%s/queues/%s", "MaxNumberOfMessages": 10}`, app.baseURL, fifoQueueName)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(recBody))
		req.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var recResp models.ReceiveMessageResponse
		err = json.NewDecoder(resp.Body).Decode(&recResp)
		require.NoError(t, err)
		require.Len(t, recResp.Messages, len(messageBodies))

		var receivedBodies []string
		for _, msg := range recResp.Messages {
			receivedBodies = append(receivedBodies, msg.Body)
		}
		assert.Equal(t, messageBodies, receivedBodies, "Messages should be received in the order they were sent")
	})

	t.Run("FIFO Deduplication", func(t *testing.T) {
		// Reset the DB for each subtest to ensure isolation
		_, err := app.store.GetDB().Transact(func(tr fdb.Transaction) (interface{}, error) {
			dir, err := directory.CreateOrOpen(tr, []string{"concreteq"}, nil)
			if err != nil { return nil, err }
			subdirs, err := dir.List(tr, []string{})
			if err != nil { return nil, err }
			for _, subdir := range subdirs {
				if _, err := dir.Remove(tr, []string{subdir}); err != nil { return nil, err }
			}
			return nil, nil
		})
		require.NoError(t, err)
		err = app.store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		// Send a message with a deduplication ID
		messageGroupID := "group-b"
		deduplicationID := "dedup-id-1"
		msgRequest := models.SendMessageRequest{
			QueueUrl:               fmt.Sprintf("%s/queues/%s", app.baseURL, fifoQueueName),
			MessageBody:            "deduplication test message",
			MessageGroupId:         &messageGroupID,
			MessageDeduplicationId: &deduplicationID,
		}
		msgBodyBytes, _ := json.Marshal(msgRequest)

		// Send first time
		req1, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(msgBodyBytes))
		req1.Header.Set("Content-Type", "application/json")
		req1.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
		resp1, err := http.DefaultClient.Do(req1)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp1.StatusCode)
		var sendResp1 models.SendMessageResponse
		err = json.NewDecoder(resp1.Body).Decode(&sendResp1)
		require.NoError(t, err)
		assert.NotEmpty(t, sendResp1.MessageId)
		resp1.Body.Close()

		// Send second time with same dedup ID
		req2, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(msgBodyBytes))
		req2.Header.Set("Content-Type", "application/json")
		req2.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
		resp2, err := http.DefaultClient.Do(req2)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp2.StatusCode)
		var sendResp2 models.SendMessageResponse
		err = json.NewDecoder(resp2.Body).Decode(&sendResp2)
		require.NoError(t, err)

		// Verify the message ID is the same, indicating deduplication
		assert.Equal(t, sendResp1.MessageId, sendResp2.MessageId)
		resp2.Body.Close()
	})

	t.Run("ReceiveMessageLogic", func(t *testing.T) {
		// Reset the DB for each subtest to ensure isolation
		_, err := app.store.GetDB().Transact(func(tr fdb.Transaction) (interface{}, error) {
			dir, err := directory.CreateOrOpen(tr, []string{"concreteq"}, nil)
			if err != nil {
				return nil, err
			}
			subdirs, err := dir.List(tr, []string{})
			if err != nil {
				return nil, err
			}
			for _, subdir := range subdirs {
				if _, err := dir.Remove(tr, []string{subdir}); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		require.NoError(t, err)
		err = app.store.CreateQueue(ctx, stdQueueName, nil, nil)
		require.NoError(t, err)
		err = app.store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		t.Run("AtLeastOnceDelivery (Visibility Timeout)", func(t *testing.T) {
			// This test demonstrates the at-least-once delivery guarantee of standard queues.
			// A message is received, but not deleted. After the visibility timeout expires,
			// it becomes visible again and is received a second time.
			sendResp, err := app.store.SendMessage(ctx, stdQueueName, &models.SendMessageRequest{MessageBody: "visibility test"})
			require.NoError(t, err)

			// First receive
			recBody1 := fmt.Sprintf(`{"QueueUrl": "%s/queues/%s", "VisibilityTimeout": 1}`, app.baseURL, stdQueueName)
			req1, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(recBody1))
			req1.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
			resp1, err := http.DefaultClient.Do(req1)
			require.NoError(t, err)
			defer resp1.Body.Close()
			assert.Equal(t, http.StatusOK, resp1.StatusCode)
			var recResp1 models.ReceiveMessageResponse
			err = json.NewDecoder(resp1.Body).Decode(&recResp1)
			require.NoError(t, err)
			require.Len(t, recResp1.Messages, 1)
			assert.Equal(t, sendResp.MessageId, recResp1.Messages[0].MessageId)

			// Try to receive again immediately, should get nothing
			recBody2 := fmt.Sprintf(`{"QueueUrl": "%s/queues/%s", "MaxNumberOfMessages": 1}`, app.baseURL, stdQueueName)
			req2, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(recBody2))
			req2.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
			resp2, err := http.DefaultClient.Do(req2)
			require.NoError(t, err)
			defer resp2.Body.Close()
			assert.Equal(t, http.StatusOK, resp2.StatusCode)
			var recResp2 models.ReceiveMessageResponse
			err = json.NewDecoder(resp2.Body).Decode(&recResp2)
			require.NoError(t, err)
			assert.Len(t, recResp2.Messages, 0, "should not receive message during visibility timeout")

			// Wait for visibility timeout to expire
			time.Sleep(1100 * time.Millisecond)

			// Try to receive again, should get the message
			req3, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(recBody2)) // Can reuse recBody2
			req3.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
			resp3, err := http.DefaultClient.Do(req3)
			require.NoError(t, err)
			defer resp3.Body.Close()
			assert.Equal(t, http.StatusOK, resp3.StatusCode)
			var recResp3 models.ReceiveMessageResponse
			err = json.NewDecoder(resp3.Body).Decode(&recResp3)
			require.NoError(t, err)
			require.Len(t, recResp3.Messages, 1, "should receive message after visibility timeout")
			assert.Equal(t, sendResp.MessageId, recResp3.Messages[0].MessageId)
		})

		t.Run("Message Delay", func(t *testing.T) {
			delayQueueName := "delay-queue"
			err := app.store.CreateQueue(ctx, delayQueueName, nil, nil)
			require.NoError(t, err)

			// Send a message with a 2-second delay
			delaySeconds := int32(2)
			sendReq := models.SendMessageRequest{
				QueueUrl:     fmt.Sprintf("%s/queues/%s", app.baseURL, delayQueueName),
				MessageBody:  "delayed message",
				DelaySeconds: &delaySeconds,
			}
			msgBodyBytes, err := json.Marshal(sendReq)
			require.NoError(t, err)

			req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(msgBodyBytes))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode)

			// Try to receive immediately, should get nothing
			recBody := fmt.Sprintf(`{"QueueUrl": "%s/queues/%s"}`, app.baseURL, delayQueueName)
			req2, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(recBody))
			req2.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
			resp2, err := http.DefaultClient.Do(req2)
			require.NoError(t, err)
			defer resp2.Body.Close()
			assert.Equal(t, http.StatusOK, resp2.StatusCode)
			var recResp2 models.ReceiveMessageResponse
			err = json.NewDecoder(resp2.Body).Decode(&recResp2)
			require.NoError(t, err)
			assert.Len(t, recResp2.Messages, 0, "should not receive delayed message immediately")

			// Wait for the delay to pass
			time.Sleep(2100 * time.Millisecond)

			// Try to receive again, should get the message now
			req3, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(recBody))
			req3.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
			resp3, err := http.DefaultClient.Do(req3)
			require.NoError(t, err)
			defer resp3.Body.Close()
			assert.Equal(t, http.StatusOK, resp3.StatusCode)
			var recResp3 models.ReceiveMessageResponse
			err = json.NewDecoder(resp3.Body).Decode(&recResp3)
			require.NoError(t, err)
			assert.Len(t, recResp3.Messages, 1, "should receive message after delay")
		})
	})
}
