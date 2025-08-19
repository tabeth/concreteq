package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/store"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockStore is a mock implementation of the Store interface for testing.
type MockStore struct {
	mock.Mock
}

func (m *MockStore) CreateQueue(ctx context.Context, name string, attributes, tags map[string]string) error {
	args := m.Called(ctx, name, attributes, tags)
	return args.Error(0)
}

func (m *MockStore) DeleteQueue(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}
func (m *MockStore) ListQueues(ctx context.Context, maxResults int, nextToken, queueNamePrefix string) ([]string, string, error) {
	args := m.Called(ctx, maxResults, nextToken, queueNamePrefix)
	var queues []string
	if args.Get(0) != nil {
		queues = args.Get(0).([]string)
	}
	return queues, args.String(1), args.Error(2)
}
func (m *MockStore) GetQueueAttributes(ctx context.Context, name string) (map[string]string, error) {
	return nil, nil
}
func (m *MockStore) SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error {
	return nil
}
func (m *MockStore) GetQueueURL(ctx context.Context, name string) (string, error) { return "", nil }
func (m *MockStore) PurgeQueue(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}
func (m *MockStore) SendMessage(ctx context.Context, queueName string, message *models.SendMessageRequest) (*models.SendMessageResponse, error) {
	args := m.Called(ctx, queueName, message)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.SendMessageResponse), args.Error(1)
}
func (m *MockStore) SendMessageBatch(ctx context.Context, queueName string, req *models.SendMessageBatchRequest) (*models.SendMessageBatchResponse, error) {
	args := m.Called(ctx, queueName, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.SendMessageBatchResponse), args.Error(1)
}
func (m *MockStore) ReceiveMessage(ctx context.Context, queueName string, req *models.ReceiveMessageRequest) (*models.ReceiveMessageResponse, error) {
	args := m.Called(ctx, queueName, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ReceiveMessageResponse), args.Error(1)
}
func (m *MockStore) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	args := m.Called(ctx, queueName, receiptHandle)
	return args.Error(0)
}
func (m *MockStore) DeleteMessageBatch(ctx context.Context, queueName string, receiptHandles []string) error {
	return nil
}
func (m *MockStore) ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int) error {
	return nil
}
func (m *MockStore) ChangeMessageVisibilityBatch(ctx context.Context, queueName string, entries map[string]int) error {
	return nil
}
func (m *MockStore) AddPermission(ctx context.Context, queueName, label string, permissions map[string][]string) error {
	return nil
}
func (m *MockStore) RemovePermission(ctx context.Context, queueName, label string) error { return nil }
func (m *MockStore) ListQueueTags(ctx context.Context, queueName string) (map[string]string, error) {
	return nil, nil
}
func (m *MockStore) TagQueue(ctx context.Context, queueName string, tags map[string]string) error {
	return nil
}
func (m *MockStore) UntagQueue(ctx context.Context, queueName string, tagKeys []string) error {
	return nil
}
func (m *MockStore) ListDeadLetterSourceQueues(ctx context.Context, queueURL string) ([]string, error) {
	return nil, nil
}
func (m *MockStore) StartMessageMoveTask(ctx context.Context, sourceArn, destinationArn string) (string, error) {
	return "", nil
}
func (m *MockStore) CancelMessageMoveTask(ctx context.Context, taskHandle string) error { return nil }
func (m *MockStore) ListMessageMoveTasks(ctx context.Context, sourceArn string) ([]string, error) {
	return nil, nil
}

func TestCreateQueueHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Standard Queue Creation",
			inputBody: `{"QueueName": "my-test-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "my-test-queue", mock.Anything, mock.Anything).Return(nil)
			},
			expectedStatusCode: http.StatusCreated,
			expectedBody:       `{"QueueUrl":"http://localhost:8080/queues/my-test-queue"}`,
		},
		{
			name:      "Successful FIFO Queue Creation",
			inputBody: `{"QueueName": "my-fifo-queue.fifo", "Attributes": {"FifoQueue": "true"}}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "my-fifo-queue.fifo", mock.Anything, mock.Anything).Return(nil)
			},
			expectedStatusCode: http.StatusCreated,
			expectedBody:       `{"QueueUrl":"http://localhost:8080/queues/my-fifo-queue.fifo"}`,
		},
		{
			name:               "Queue Name Too Long",
			inputBody:          `{"QueueName": "` + strings.Repeat("a", 81) + `"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length."}`,
		},
		{
			name:               "Invalid Characters in Queue Name",
			inputBody:          `{"QueueName": "invalid!"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length."}`,
		},
		{
			name:               "FIFO Name without FIFO Attribute",
			inputBody:          `{"QueueName": "my-queue.fifo"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Queue name ends in .fifo but FifoQueue attribute is not 'true'"}`,
		},
		{
			name:               "FIFO Attribute without FIFO Name",
			inputBody:          `{"QueueName": "my-queue", "Attributes": {"FifoQueue": "true"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"FifoQueue attribute is 'true' but queue name does not end in .fifo"}`,
		},
		{
			name:               "Invalid Attribute Value",
			inputBody:          `{"QueueName": "q", "Attributes": {"DelaySeconds": "901"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidAttributeName", "message":"invalid value for DelaySeconds: must be between 0 and 900"}`,
		},
		{
			name:               "Invalid Redrive Policy JSON",
			inputBody:          `{"QueueName": "q", "Attributes": {"RedrivePolicy": "not-json"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidAttributeName", "message":"invalid value for RedrivePolicy: must be a valid JSON object"}`,
		},
		{
			name:               "Invalid Redrive Policy MaxReceiveCount",
			inputBody:          `{"QueueName": "q", "Attributes": {"RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:my-dlq\",\"maxReceiveCount\":\"2000\"}"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidAttributeName", "message":"invalid value for RedrivePolicy: maxReceiveCount must be an integer between 1 and 1000"}`,
		},
		{
			name:               "FIFO Attribute on Standard Queue",
			inputBody:          `{"QueueName": "standard-q", "Attributes": {"DeduplicationScope": "messageGroup"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"DeduplicationScope is only valid for FIFO queues"}`,
		},
		{
			name:      "Queue Already Exists",
			inputBody: `{"QueueName": "existing-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "existing-queue", mock.Anything, mock.Anything).Return(store.ErrQueueAlreadyExists)
			},
			expectedStatusCode: http.StatusConflict,
			expectedBody:       `{"__type":"QueueAlreadyExists", "message":"Queue already exists"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/", bytes.NewBufferString(tc.inputBody))
			req.Host = "localhost:8080"
			req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") { // It's a JSON response
					// For successful responses, we check the whole body
					if rr.Code < 300 {
						assert.JSONEq(t, tc.expectedBody, rr.Body.String())
					} else {
						// For error responses, we check the type and message
						var errResp models.ErrorResponse
						err := json.Unmarshal(rr.Body.Bytes(), &errResp)
						require.NoError(t, err, "failed to unmarshal error response")

						var expectedErrResp models.ErrorResponse
						err = json.Unmarshal([]byte(tc.expectedBody), &expectedErrResp)
						require.NoError(t, err, "failed to unmarshal expected error response")

						assert.Equal(t, expectedErrResp, errResp)
					}
				} else { // It's a plain text error message from our old http.Error calls, which we should not have
					assert.Fail(t, "Received unexpected plain text error response", "Response body: %s", rr.Body.String())
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestSendMessageBatchHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name: "Successful Batch Send",
			inputBody: `{
				"QueueUrl": "http://localhost:8080/queues/my-queue",
				"Entries": [
					{"Id": "1", "MessageBody": "msg1"},
					{"Id": "2", "MessageBody": "msg2"}
				]
			}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SendMessageBatch", mock.Anything, "my-queue", mock.AnythingOfType("*models.SendMessageBatchRequest")).Return(&models.SendMessageBatchResponse{
					Successful: []models.SendMessageBatchResultEntry{
						{Id: "1", MessageId: "uuid1", MD5OfMessageBody: "md5-1"},
						{Id: "2", MessageId: "uuid2", MD5OfMessageBody: "md5-2"},
					},
					Failed: []models.BatchResultErrorEntry{},
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"Successful":[{"Id":"1","MessageId":"uuid1","MD5OfMessageBody":"md5-1"},{"Id":"2","MessageId":"uuid2","MD5OfMessageBody":"md5-2"}],"Failed":[]}`,
		},
		{
			name:               "Too Many Entries",
			inputBody:          `{"QueueUrl": "http://localhost/queues/q", "Entries": [` + strings.Repeat(`{"Id":"_","MessageBody":"_"},`, 10) + `{"Id":"_","MessageBody":"_"}]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"TooManyEntriesInBatchRequest", "message":"The batch request contains more entries than permissible."}`,
		},
		{
			name: "Duplicate Entry Ids",
			inputBody: `{
				"QueueUrl": "http://localhost/queues/q",
				"Entries": [
					{"Id": "1", "MessageBody": "msg1"},
					{"Id": "1", "MessageBody": "msg2"}
				]
			}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"BatchEntryIdsNotDistinct", "message":"Two or more batch entries in the request have the same Id."}`,
		},
		{
			name:               "Empty Batch Request",
			inputBody:          `{"QueueUrl": "http://localhost/queues/q", "Entries": []}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"EmptyBatchRequest", "message":"The batch request doesn't contain any entries."}`,
		},
		{
			name: "Batch Request Too Long",
			inputBody: `{
				"QueueUrl": "http://localhost/queues/q",
				"Entries": [
					{"Id": "1", "MessageBody": "` + strings.Repeat("a", 200*1024) + `"},
					{"Id": "2", "MessageBody": "` + strings.Repeat("b", 60*1024) + `"}
				]
			}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"BatchRequestTooLong", "message":"The length of all the messages put together is more than the limit."}`,
		},
		{
			name: "Queue Does Not Exist",
			inputBody: `{
				"QueueUrl": "http://localhost/queues/non-existent",
				"Entries": [{"Id": "1", "MessageBody": "msg1"}]
			}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SendMessageBatch", mock.Anything, "non-existent", mock.AnythingOfType("*models.SendMessageBatchRequest")).Return(nil, store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		{
			name: "Partial Success",
			inputBody: `{
				"QueueUrl": "http://localhost/queues/my-queue",
				"Entries": [
					{"Id": "1", "MessageBody": "valid"},
					{"Id": "2", "MessageBody": "invalid", "DelaySeconds": 901}
				]
			}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SendMessageBatch", mock.Anything, "my-queue", mock.AnythingOfType("*models.SendMessageBatchRequest")).Return(&models.SendMessageBatchResponse{
					Successful: []models.SendMessageBatchResultEntry{
						{Id: "1", MessageId: "uuid1", MD5OfMessageBody: "md5-1"},
					},
					Failed: []models.BatchResultErrorEntry{
						{Id: "2", Code: "InvalidParameterValue", Message: "DelaySeconds must be between 0 and 900.", SenderFault: true},
					},
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"Successful":[{"Id":"1","MessageId":"uuid1","MD5OfMessageBody":"md5-1"}],"Failed":[{"Id":"2","Code":"InvalidParameterValue","Message":"DelaySeconds must be between 0 and 900.","SenderFault":true}]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			// We need to register the specific handler for this test
			r.Post("/queues/{queueName}/messages/batch", app.SendMessageBatchHandler)

			// Extract queue name from the URL in the payload
			var payload struct {
				QueueUrl string `json:"QueueUrl"`
			}
			json.Unmarshal([]byte(tc.inputBody), &payload)
			queueName := chi.URLParam(httptest.NewRequest("POST", payload.QueueUrl, nil), "queueName")
			if queueName == "" && payload.QueueUrl != "" {
				queueName = payload.QueueUrl[strings.LastIndex(payload.QueueUrl, "/")+1:]
			}

			requestURL := "/queues/" + queueName + "/messages/batch"
			req, _ := http.NewRequest("POST", requestURL, bytes.NewBufferString(tc.inputBody))
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBody != "" {
				require.JSONEq(t, tc.expectedBody, rr.Body.String())
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestDeleteMessageHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Deletion",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "ReceiptHandle": "valid-handle"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteMessage", mock.Anything, "my-queue", "valid-handle").Return(nil).Once()
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       "",
		},
		{
			name:               "Missing QueueUrl",
			inputBody:          `{"ReceiptHandle": "a-handle"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
		},
		{
			name:               "Missing ReceiptHandle",
			inputBody:          `{"QueueUrl": "http://localhost/queues/my-queue"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a ReceiptHandle."}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "http://localhost/queues/non-existent-queue", "ReceiptHandle": "a-handle"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteMessage", mock.Anything, "non-existent-queue", "a-handle").Return(store.ErrQueueDoesNotExist).Once()
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		{
			name:      "Invalid Receipt Handle",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "ReceiptHandle": "invalid-handle"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteMessage", mock.Anything, "my-queue", "invalid-handle").Return(store.ErrInvalidReceiptHandle).Once()
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"ReceiptHandleIsInvalid", "message":"The specified receipt handle isn't valid."}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/", bytes.NewBufferString(tc.inputBody))
			req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessage")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)
			if tc.expectedBody != "" {
				require.JSONEq(t, tc.expectedBody, rr.Body.String())
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestReceiveMessageHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Receive",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/my-queue", "MaxNumberOfMessages": 2}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ReceiveMessage", mock.Anything, "my-queue", mock.AnythingOfType("*models.ReceiveMessageRequest")).Return(&models.ReceiveMessageResponse{
					Messages: []models.ResponseMessage{
						{
							MessageId:     "uuid1",
							ReceiptHandle: "receipt1",
							Body:          "hello",
							MD5OfBody:     "md5-hello",
						},
					},
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"Messages":[{"Attributes":null,"Body":"hello","MD5OfBody":"md5-hello","MessageId":"uuid1","ReceiptHandle":"receipt1"}]}`,
		},
		{
			name:               "Invalid MaxNumberOfMessages",
			inputBody:          `{"QueueUrl": "http://localhost:8080/queues/my-queue", "MaxNumberOfMessages": 11}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Value for parameter MaxNumberOfMessages is invalid. Reason: Must be an integer from 1 to 10."}`,
		},
		{
			name:               "Invalid VisibilityTimeout",
			inputBody:          `{"QueueUrl": "http://localhost:8080/queues/my-queue", "VisibilityTimeout": 99999}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Value for parameter VisibilityTimeout is invalid. Reason: Must be an integer from 0 to 43200."}`,
		},
		{
			name:               "Invalid ReceiveRequestAttemptId",
			inputBody:          `{"QueueUrl": "http://localhost:8080/queues/my-queue", "ReceiveRequestAttemptId": "toolong` + strings.Repeat("a", 128) + `"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"ReceiveRequestAttemptId can be up to 128 characters long."}`,
		},
		{
			name:               "Invalid System Attribute Name",
			inputBody:          `{"QueueUrl": "http://localhost:8080/queues/my-queue", "MessageSystemAttributeNames": ["InvalidName"]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidAttributeName", "message":"The attribute 'InvalidName' is not supported."}`,
		},
		{
			name:               "Invalid Custom Attribute Name",
			inputBody:          `{"QueueUrl": "http://localhost:8080/queues/my-queue", "MessageAttributeNames": ["AWS.Invalid"]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidAttributeName", "message":"The attribute name 'AWS.Invalid' is invalid."}`,
		},
		{
			name:               "Invalid WaitTimeSeconds",
			inputBody:          `{"QueueUrl": "http://localhost:8080/queues/my-queue", "WaitTimeSeconds": 21}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Value for parameter WaitTimeSeconds is invalid. Reason: Must be an integer from 0 to 20."}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/non-existent-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ReceiveMessage", mock.Anything, "non-existent-queue", mock.AnythingOfType("*models.ReceiveMessageRequest")).Return(nil, store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/", bytes.NewBufferString(tc.inputBody))
			req.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") { // It's a JSON response
					// For successful responses, we check the whole body
					if rr.Code < 300 {
						assert.JSONEq(t, tc.expectedBody, rr.Body.String())
					} else {
						// For error responses, we check the type and message
						var errResp models.ErrorResponse
						err := json.Unmarshal(rr.Body.Bytes(), &errResp)
						require.NoError(t, err, "failed to unmarshal error response")

						var expectedErrResp models.ErrorResponse
						err = json.Unmarshal([]byte(tc.expectedBody), &expectedErrResp)
						require.NoError(t, err, "failed to unmarshal expected error response")

						assert.Equal(t, expectedErrResp, errResp)
					}
				} else { // It's a plain text error message from our old http.Error calls, which we should not have
					assert.Fail(t, "Received unexpected plain text error response", "Response body: %s", rr.Body.String())
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestPurgeQueueHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Purge",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/my-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("PurgeQueue", mock.Anything, "my-queue").Return(nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       "",
		},
		{
			name:               "Missing QueueUrl",
			inputBody:          `{}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/non-existent"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("PurgeQueue", mock.Anything, "non-existent").Return(store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		{
			name:      "Purge In Progress",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/purging-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("PurgeQueue", mock.Anything, "purging-queue").Return(store.ErrPurgeQueueInProgress)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"PurgeQueueInProgress", "message":"Indicates that the specified queue previously received a PurgeQueue request within the last 60 seconds."}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/", bytes.NewBufferString(tc.inputBody))
			req.Header.Set("X-Amz-Target", "AmazonSQS.PurgeQueue")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)
			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") { // It's a JSON response
					// For successful responses, we check the whole body
					if rr.Code < 300 {
						assert.JSONEq(t, tc.expectedBody, rr.Body.String())
					} else {
						// For error responses, we check the type and message
						var errResp models.ErrorResponse
						err := json.Unmarshal(rr.Body.Bytes(), &errResp)
						require.NoError(t, err, "failed to unmarshal error response")

						var expectedErrResp models.ErrorResponse
						err = json.Unmarshal([]byte(tc.expectedBody), &expectedErrResp)
						require.NoError(t, err, "failed to unmarshal expected error response")

						assert.Equal(t, expectedErrResp, errResp)
					}
				} else { // It's a plain text error message from our old http.Error calls, which we should not have
					assert.Fail(t, "Received unexpected plain text error response", "Response body: %s", rr.Body.String())
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestDeleteQueueHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Deletion",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/my-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteQueue", mock.Anything, "my-queue").Return(nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       "",
		},
		{
			name:               "Missing QueueUrl",
			inputBody:          `{}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/non-existent"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteQueue", mock.Anything, "non-existent").Return(store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/", bytes.NewBufferString(tc.inputBody))
			req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteQueue")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)
			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") { // It's a JSON response
					// For successful responses, we check the whole body
					if rr.Code < 300 {
						assert.JSONEq(t, tc.expectedBody, rr.Body.String())
					} else {
						// For error responses, we check the type and message
						var errResp models.ErrorResponse
						err := json.Unmarshal(rr.Body.Bytes(), &errResp)
						require.NoError(t, err, "failed to unmarshal error response")

						var expectedErrResp models.ErrorResponse
						err = json.Unmarshal([]byte(tc.expectedBody), &expectedErrResp)
						require.NoError(t, err, "failed to unmarshal expected error response")

						assert.Equal(t, expectedErrResp, errResp)
					}
				} else { // It's a plain text error message from our old http.Error calls, which we should not have
					assert.Fail(t, "Received unexpected plain text error response", "Response body: %s", rr.Body.String())
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestListQueuesHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Listing",
			inputBody: `{"MaxResults": 1, "QueueNamePrefix": "test"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ListQueues", mock.Anything, 1, "", "test").Return([]string{"test-q1"}, "test-q1", nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"QueueUrls":["http://localhost:8080/queues/test-q1"],"NextToken":"test-q1"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/", bytes.NewBufferString(tc.inputBody))
			req.Host = "localhost:8080"
			req.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") { // It's a JSON response
					// For successful responses, we check the whole body
					if rr.Code < 300 {
						assert.JSONEq(t, tc.expectedBody, rr.Body.String())
					} else {
						// For error responses, we check the type and message
						var errResp models.ErrorResponse
						err := json.Unmarshal(rr.Body.Bytes(), &errResp)
						require.NoError(t, err, "failed to unmarshal error response")

						var expectedErrResp models.ErrorResponse
						err = json.Unmarshal([]byte(tc.expectedBody), &expectedErrResp)
						require.NoError(t, err, "failed to unmarshal expected error response")

						assert.Equal(t, expectedErrResp, errResp)
					}
				} else { // It's a plain text error message from our old http.Error calls, which we should not have
					assert.Fail(t, "Received unexpected plain text error response", "Response body: %s", rr.Body.String())
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestSendMessageHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Send to Standard Queue",
			inputBody: `{"MessageBody": "hello world", "QueueUrl": "http://localhost:8080/queues/my-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SendMessage", mock.Anything, "my-queue", mock.AnythingOfType("*models.SendMessageRequest")).Return(&models.SendMessageResponse{
					MessageId:      "some-uuid",
					MD5OfMessageBody: "5d41402abc4b2a76b9719d911017c592",
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"MD5OfMessageBody":"5d41402abc4b2a76b9719d911017c592","MessageId":"some-uuid"}`,
		},
		{
			name:      "Successful Send to FIFO Queue",
			inputBody: `{"MessageBody": "hello fifo", "QueueUrl": "http://localhost:8080/queues/my-queue.fifo", "MessageGroupId": "group1"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SendMessage", mock.Anything, "my-queue.fifo", mock.AnythingOfType("*models.SendMessageRequest")).Return(&models.SendMessageResponse{
					MessageId:      "some-uuid",
					MD5OfMessageBody: "some-md5",
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "Missing QueueUrl",
			inputBody:          `{"MessageBody": "hello"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
		},
		{
			name:               "Empty Message Body",
			inputBody:          `{"MessageBody": "", "QueueUrl": "http://localhost:8080/queues/my-queue"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"The message body must be between 1 and 262144 bytes long."}`,
		},
		{
			name:               "Message Body Too Long",
			inputBody:          `{"MessageBody": "` + strings.Repeat("a", 256*1024+1) + `", "QueueUrl": "http://localhost:8080/queues/my-queue"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"The message body must be between 1 and 262144 bytes long."}`,
		},
		{
			name:               "DelaySeconds with FIFO Queue",
			inputBody:          `{"MessageBody": "hello", "QueueUrl": "http://localhost:8080/queues/my-queue.fifo", "DelaySeconds": 10, "MessageGroupId": "group1"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"The request include parameter that is not valid for this queue type. Reason: DelaySeconds is not supported for FIFO queues."}`,
		},
		{
			name:               "Missing MessageGroupId for FIFO",
			inputBody:          `{"MessageBody": "hello", "QueueUrl": "http://localhost:8080/queues/my-queue.fifo"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a MessageGroupId."}`,
		},
		{
			name:               "MessageDeduplicationId with Standard Queue",
			inputBody:          `{"MessageBody": "hello", "QueueUrl": "http://localhost:8080/queues/my-queue", "MessageDeduplicationId": "dedup1"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"MessageDeduplicationId is supported only for FIFO queues."}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"MessageBody": "hello", "QueueUrl": "http://localhost:8080/queues/non-existent-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SendMessage", mock.Anything, "non-existent-queue", mock.AnythingOfType("*models.SendMessageRequest")).Return(nil, store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/", bytes.NewBufferString(tc.inputBody))
			req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") { // It's a JSON response
					// For successful responses, we check the whole body
					if rr.Code < 300 {
						assert.JSONEq(t, tc.expectedBody, rr.Body.String())
					} else {
						// For error responses, we check the type and message
						var errResp models.ErrorResponse
						err := json.Unmarshal(rr.Body.Bytes(), &errResp)
						require.NoError(t, err, "failed to unmarshal error response")

						var expectedErrResp models.ErrorResponse
						err = json.Unmarshal([]byte(tc.expectedBody), &expectedErrResp)
						require.NoError(t, err, "failed to unmarshal expected error response")

						assert.Equal(t, expectedErrResp, errResp)
					}
				} else { // It's a plain text error message from our old http.Error calls, which we should not have
					assert.Fail(t, "Received unexpected plain text error response", "Response body: %s", rr.Body.String())
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}
