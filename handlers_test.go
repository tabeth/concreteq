package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	args := m.Called(ctx, name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}
func (m *MockStore) SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error {
	args := m.Called(ctx, name, attributes)
	return args.Error(0)
}
func (m *MockStore) GetQueueURL(ctx context.Context, name string) (string, error) {
	args := m.Called(ctx, name)
	return args.String(0), args.Error(1)
}
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
func (m *MockStore) DeleteMessageBatch(ctx context.Context, queueName string, entries []models.DeleteMessageBatchRequestEntry) (*models.DeleteMessageBatchResponse, error) {
	args := m.Called(ctx, queueName, entries)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.DeleteMessageBatchResponse), args.Error(1)
}
func (m *MockStore) ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int) error {
	args := m.Called(ctx, queueName, receiptHandle, visibilityTimeout)
	return args.Error(0)
}
func (m *MockStore) ChangeMessageVisibilityBatch(ctx context.Context, queueName string, entries map[string]int) (*models.ChangeMessageVisibilityBatchResponse, error) {
	args := m.Called(ctx, queueName, entries)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ChangeMessageVisibilityBatchResponse), args.Error(1)
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

func TestValidateAttributes(t *testing.T) {
	tests := []struct {
		name       string
		attributes map[string]string
		expectErr  string
	}{
		// Valid cases (should not error)
		{"Valid DelaySeconds", map[string]string{"DelaySeconds": "10"}, ""},
		{"Valid MaximumMessageSize", map[string]string{"MaximumMessageSize": "2048"}, ""},
		{"Valid MessageRetentionPeriod", map[string]string{"MessageRetentionPeriod": "86400"}, ""},
		{"Valid ReceiveMessageWaitTimeSeconds", map[string]string{"ReceiveMessageWaitTimeSeconds": "10"}, ""},
		{"Valid VisibilityTimeout", map[string]string{"VisibilityTimeout": "300"}, ""},
		{"Valid FifoQueue", map[string]string{"FifoQueue": "true"}, ""},
		{"Valid ContentBasedDeduplication", map[string]string{"ContentBasedDeduplication": "false"}, ""},
		{"Valid RedrivePolicy", map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn","maxReceiveCount":"5"}`}, ""},
		{"Valid DeduplicationScope", map[string]string{"DeduplicationScope": "messageGroup"}, ""},
		{"Valid FifoThroughputLimit", map[string]string{"FifoThroughputLimit": "perQueue"}, ""},

		// Invalid cases
		{"Invalid DelaySeconds", map[string]string{"DelaySeconds": "abc"}, "invalid value for DelaySeconds: must be an integer"},
		{"Invalid MaximumMessageSize", map[string]string{"MaximumMessageSize": "1023"}, "invalid value for MaximumMessageSize: must be between 1024 and 262144"},
		{"Invalid MessageRetentionPeriod", map[string]string{"MessageRetentionPeriod": "0"}, "invalid value for MessageRetentionPeriod: must be between 60 and 1209600"},
		{"Invalid ReceiveMessageWaitTimeSeconds", map[string]string{"ReceiveMessageWaitTimeSeconds": "21"}, "invalid value for ReceiveMessageWaitTimeSeconds: must be between 0 and 20"},
		{"Invalid VisibilityTimeout", map[string]string{"VisibilityTimeout": "-1"}, "invalid value for VisibilityTimeout: must be between 0 and 43200"},
		{"Invalid FifoQueue", map[string]string{"FifoQueue": "yes"}, "invalid value for FifoQueue: must be 'true' or 'false'"},
		{"Invalid ContentBasedDeduplication", map[string]string{"ContentBasedDeduplication": "no"}, "invalid value for ContentBasedDeduplication: must be 'true' or 'false'"},
		{"Invalid RedrivePolicy JSON", map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn"`}, "invalid value for RedrivePolicy: must be a valid JSON object"},
		{"Missing deadLetterTargetArn", map[string]string{"RedrivePolicy": `{"maxReceiveCount":"5"}`}, "invalid value for RedrivePolicy: deadLetterTargetArn is required"},
		{"Invalid maxReceiveCount string", map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn","maxReceiveCount":"abc"}`}, "invalid value for RedrivePolicy: maxReceiveCount must be an integer between 1 and 1000"},
		{"Invalid maxReceiveCount range", map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn","maxReceiveCount":"0"}`}, "invalid value for RedrivePolicy: maxReceiveCount must be an integer between 1 and 1000"},
		{"Invalid DeduplicationScope", map[string]string{"DeduplicationScope": "invalid"}, "invalid value for DeduplicationScope: must be 'messageGroup' or 'queue'"},
		{"Invalid FifoThroughputLimit", map[string]string{"FifoThroughputLimit": "invalid"}, "invalid value for FifoThroughputLimit: must be 'perQueue' or 'perMessageGroupId'"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAttributes(tc.attributes)
			if tc.expectErr != "" {
				require.Error(t, err)
				assert.Equal(t, tc.expectErr, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUnimplementedHandlers(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		inputBody  string
		httpMethod string
		url        string
	}{
		{"AddPermissionHandler", "AmazonSQS.AddPermission", `{}`, "POST", "/"},
		{"RemovePermissionHandler", "AmazonSQS.RemovePermission", `{}`, "POST", "/"},
		{"ListQueueTagsHandler", "AmazonSQS.ListQueueTags", `{}`, "POST", "/"},
		{"TagQueueHandler", "AmazonSQS.TagQueue", `{}`, "POST", "/"},
		{"UntagQueueHandler", "AmazonSQS.UntagQueue", `{}`, "POST", "/"},
		{"ListDeadLetterSourceQueuesHandler", "AmazonSQS.ListDeadLetterSourceQueues", `{}`, "POST", "/"},
		{"StartMessageMoveTaskHandler", "AmazonSQS.StartMessageMoveTask", `{}`, "POST", "/"},
		{"CancelMessageMoveTaskHandler", "AmazonSQS.CancelMessageMoveTask", `{}`, "POST", "/"},
		{"ListMessageMoveTasksHandler", "AmazonSQS.ListMessageMoveTasks", `{}`, "POST", "/"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest(tc.httpMethod, tc.url, bytes.NewBufferString(tc.inputBody))
			req.Header.Set("X-Amz-Target", tc.target)
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusNotImplemented, rr.Code)
			mockStore.AssertExpectations(t)
		})
	}
}

func TestValidateSendMessageRequest(t *testing.T) {
	app := &App{}
	longString := strings.Repeat("a", 129)
	invalidChars := "a b"
	validString := "valid"
	tooManyAttributes := make(map[string]models.MessageAttributeValue)
	for i := 0; i < 11; i++ {
		tooManyAttributes[fmt.Sprintf("attr%d", i)] = models.MessageAttributeValue{DataType: "String"}
	}

	tests := []struct {
		name      string
		req       *models.SendMessageRequest
		queueName string
		expectErr string
	}{
		{
			name: "MessageDeduplicationId Too Long",
			req: &models.SendMessageRequest{
				QueueUrl:               "q.fifo",
				MessageBody:            "body",
				MessageGroupId:         &validString,
				MessageDeduplicationId: &longString,
			},
			queueName: "q.fifo",
			expectErr: "InvalidParameterValue: MessageDeduplicationId can be up to 128 characters long.",
		},
		{
			name: "Invalid Chars in MessageDeduplicationId",
			req: &models.SendMessageRequest{
				QueueUrl:               "q.fifo",
				MessageBody:            "body",
				MessageGroupId:         &validString,
				MessageDeduplicationId: &invalidChars,
			},
			queueName: "q.fifo",
			expectErr: "InvalidParameterValue: MessageDeduplicationId can only contain alphanumeric characters and punctuation.",
		},
		{
			name: "MessageGroupId Too Long",
			req: &models.SendMessageRequest{
				QueueUrl:       "q.fifo",
				MessageBody:    "body",
				MessageGroupId: &longString,
			},
			queueName: "q.fifo",
			expectErr: "InvalidParameterValue: MessageGroupId can be up to 128 characters long.",
		},
		{
			name: "Invalid Chars in MessageGroupId",
			req: &models.SendMessageRequest{
				QueueUrl:       "q.fifo",
				MessageBody:    "body",
				MessageGroupId: &invalidChars,
			},
			queueName: "q.fifo",
			expectErr: "InvalidParameterValue: MessageGroupId can only contain alphanumeric characters and punctuation.",
		},
		{
			name: "Too Many MessageAttributes",
			req: &models.SendMessageRequest{
				QueueUrl:          "q",
				MessageBody:       "body",
				MessageAttributes: tooManyAttributes,
			},
			queueName: "q",
			expectErr: "InvalidParameterValue: Number of message attributes cannot exceed 10.",
		},
		{
			name: "Invalid MessageAttribute Name",
			req: &models.SendMessageRequest{
				QueueUrl:    "q",
				MessageBody: "body",
				MessageAttributes: map[string]models.MessageAttributeValue{
					"aws.foo": {DataType: "String"},
				},
			},
			queueName: "q",
			expectErr: "InvalidParameterValue: Message attribute name 'aws.foo' is invalid.",
		},
		{
			name: "Empty MessageAttribute Name",
			req: &models.SendMessageRequest{
				QueueUrl:    "q",
				MessageBody: "body",
				MessageAttributes: map[string]models.MessageAttributeValue{
					"": {DataType: "String"},
				},
			},
			queueName: "q",
			expectErr: "InvalidParameterValue: Message attribute name '' is invalid.",
		},
		{
			name: "Missing MessageAttribute DataType",
			req: &models.SendMessageRequest{
				QueueUrl:    "q",
				MessageBody: "body",
				MessageAttributes: map[string]models.MessageAttributeValue{
					"foo": {},
				},
			},
			queueName: "q",
			expectErr: "InvalidParameterValue: DataType of message attribute 'foo' is required.",
		},
		{
			name: "Invalid MessageSystemAttribute Name",
			req: &models.SendMessageRequest{
				QueueUrl:    "q",
				MessageBody: "body",
				MessageSystemAttributes: map[string]models.MessageSystemAttributeValue{
					"foo": {DataType: "String"},
				},
			},
			queueName: "q",
			expectErr: "InvalidParameterValue: 'foo' is not a valid message system attribute.",
		},
		{
			name: "Invalid MessageSystemAttribute DataType",
			req: &models.SendMessageRequest{
				QueueUrl:    "q",
				MessageBody: "body",
				MessageSystemAttributes: map[string]models.MessageSystemAttributeValue{
					"AWSTraceHeader": {DataType: "Number"},
				},
			},
			queueName: "q",
			expectErr: "InvalidParameterValue: DataType of AWSTraceHeader must be String.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := app.validateSendMessageRequest(tc.req, tc.queueName)
			if tc.expectErr != "" {
				require.Error(t, err)
				assert.Equal(t, tc.expectErr, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestChangeMessageVisibilityBatchHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name: "Successful Batch Change",
			inputBody: `{
				"QueueUrl": "http://localhost:8080/queues/my-queue",
				"Entries": [
					{"Id": "msg1", "ReceiptHandle": "handle1", "VisibilityTimeout": 10},
					{"Id": "msg2", "ReceiptHandle": "handle2", "VisibilityTimeout": 20}
				]
			}`,
			mockSetup: func(ms *MockStore) {
				entries := map[string]int{"handle1": 10, "handle2": 20}
				ms.On("ChangeMessageVisibilityBatch", mock.Anything, "my-queue", entries).Return(&models.ChangeMessageVisibilityBatchResponse{
					Successful: []models.ChangeMessageVisibilityBatchResultEntry{
						{Id: "handle1"},
						{Id: "handle2"},
					},
					Failed: []models.BatchResultErrorEntry{},
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"Successful":[{"Id":"msg1"},{"Id":"msg2"}],"Failed":[]}`,
		},
		{
			name:               "Invalid JSON",
			inputBody:          `{]`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidRequest", "message":"Invalid request body"}`,
		},
		{
			name:               "Missing QueueUrl",
			inputBody:          `{"Entries":[]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
		},
		{
			name:               "Empty Entries",
			inputBody:          `{"QueueUrl": "q", "Entries":[]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"EmptyBatchRequest", "message":"The batch request doesn't contain any entries."}`,
		},
		{
			name:               "Too Many Entries",
			inputBody:          `{"QueueUrl": "q", "Entries":[` + strings.Repeat(`{},`, 10) + `{}]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"TooManyEntriesInBatchRequest", "message":"The batch request contains more entries than permissible."}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "q", "Entries":[{"Id": "1", "ReceiptHandle": "rh", "VisibilityTimeout": 1}]}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ChangeMessageVisibilityBatch", mock.Anything, "q", mock.AnythingOfType("map[string]int")).Return(nil, store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		{
			name:      "Store Internal Error",
			inputBody: `{"QueueUrl": "q", "Entries":[{"Id": "1", "ReceiptHandle": "rh", "VisibilityTimeout": 1}]}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ChangeMessageVisibilityBatch", mock.Anything, "q", mock.AnythingOfType("map[string]int")).Return(nil, errors.New("internal error"))
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       `{"__type":"InternalFailure", "message":"Failed to change message visibility batch"}`,
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
			req.Header.Set("X-Amz-Target", "AmazonSQS.ChangeMessageVisibilityBatch")
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

func TestGetQueueUrlHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful GetQueueUrl",
			inputBody: `{"QueueName": "my-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("GetQueueURL", mock.Anything, "my-queue").Return("http://localhost/queues/my-queue", nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"QueueUrl":"http://localhost/queues/my-queue"}`,
		},
		{
			name:               "Missing QueueName",
			inputBody:          `{}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueName."}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueName": "non-existent"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("GetQueueURL", mock.Anything, "non-existent").Return("", store.ErrQueueDoesNotExist)
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
			req.Header.Set("X-Amz-Target", "AmazonSQS.GetQueueUrl")
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

func TestSetQueueAttributesHandler(t *testing.T) {
	// similar to TestGetQueueAttributesHandler
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Set Queue Attributes",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "Attributes": {"VisibilityTimeout": "100"}}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SetQueueAttributes", mock.Anything, "my-queue", map[string]string{"VisibilityTimeout": "100"}).Return(nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       "",
		},
		{
			name:               "Missing QueueUrl",
			inputBody:          `{"Attributes": {"VisibilityTimeout": "100"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
		},
		{
			name:               "Missing Attributes",
			inputBody:          `{"QueueUrl": "http://localhost/queues/my-queue", "Attributes": {}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"SetQueueAttributes request must contain at least one attribute."}`,
		},
		{
			name:      "Store Error",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "Attributes": {"VisibilityTimeout": "100"}}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SetQueueAttributes", mock.Anything, "my-queue", map[string]string{"VisibilityTimeout": "100"}).Return(errors.New("a store error"))
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       `{"__type":"InternalFailure", "message":"Failed to set queue attributes"}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "http://localhost/queues/non-existent", "Attributes": {"VisibilityTimeout": "100"}}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SetQueueAttributes", mock.Anything, "non-existent", map[string]string{"VisibilityTimeout": "100"}).Return(store.ErrQueueDoesNotExist)
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
			req.Header.Set("X-Amz-Target", "AmazonSQS.SetQueueAttributes")
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

func TestGetQueueAttributesHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful - All Attributes",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "AttributeNames": ["All"]}`,
			mockSetup: func(ms *MockStore) {
				ms.On("GetQueueAttributes", mock.Anything, "my-queue").Return(map[string]string{
					"VisibilityTimeout": "100",
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody: `{
				"Attributes": {
					"VisibilityTimeout": "100",
					"QueueArn": "arn:aws:sqs:us-east-1:123456789012:my-queue",
					"ReceiveMessageWaitTimeSeconds": "0"
				}
			}`,
		},
		{
			name:      "Successful - Specific Attributes",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "AttributeNames": ["VisibilityTimeout", "QueueArn"]}`,
			mockSetup: func(ms *MockStore) {
				ms.On("GetQueueAttributes", mock.Anything, "my-queue").Return(map[string]string{
					"VisibilityTimeout":      "100",
					"MessageRetentionPeriod": "120",
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody: `{
				"Attributes": {
					"VisibilityTimeout": "100",
					"QueueArn": "arn:aws:sqs:us-east-1:123456789012:my-queue"
				}
			}`,
		},
		{
			name:               "Queue Does Not Exist",
			inputBody:          `{"QueueUrl": "http://localhost/queues/non-existent", "AttributeNames": ["All"]}`,
			mockSetup: func(ms *MockStore) {
				ms.On("GetQueueAttributes", mock.Anything, "non-existent").Return(nil, store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		{
			name:      "Invalid Attribute Name",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "AttributeNames": ["NonExistentAttribute"]}`,
			mockSetup: func(ms *MockStore) {
				ms.On("GetQueueAttributes", mock.Anything, "my-queue").Return(map[string]string{}, nil)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidAttributeName", "message":"The specified attribute NonExistentAttribute does not exist."}`,
		},
		{
			name:               "Missing QueueUrl",
			inputBody:          `{"AttributeNames": ["All"]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
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
			req.Header.Set("X-Amz-Target", "AmazonSQS.GetQueueAttributes")
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

func TestDeleteMessageBatchHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name: "Successful Batch Deletion",
			inputBody: `{
				"QueueUrl": "http://localhost:8080/queues/my-queue",
				"Entries": [
					{"Id": "msg1", "ReceiptHandle": "handle1"},
					{"Id": "msg2", "ReceiptHandle": "handle2"}
				]
			}`,
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteMessageBatch", mock.Anything, "my-queue", mock.AnythingOfType("[]models.DeleteMessageBatchRequestEntry")).Return(&models.DeleteMessageBatchResponse{
					Successful: []models.DeleteMessageBatchResultEntry{
						{Id: "msg1"},
						{Id: "msg2"},
					},
					Failed: []models.BatchResultErrorEntry{},
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"Successful":[{"Id":"msg1"},{"Id":"msg2"}],"Failed":[]}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "http://localhost/queues/non-existent", "Entries": [{"Id": "1", "ReceiptHandle": "h1"}]}`,
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteMessageBatch", mock.Anything, "non-existent", mock.Anything).Return(nil, store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		{
			name:      "Partial Failure",
			inputBody: `{"QueueUrl": "http://localhost/queues/q", "Entries": [{"Id": "1", "ReceiptHandle": "h1"}]}`,
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteMessageBatch", mock.Anything, "q", mock.Anything).Return(&models.DeleteMessageBatchResponse{
					Successful: []models.DeleteMessageBatchResultEntry{{Id: "ok_id"}},
					Failed:     []models.BatchResultErrorEntry{{Id: "fail_id", Code: "InternalError", Message: "something went wrong", SenderFault: false}},
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"Successful":[{"Id":"ok_id"}],"Failed":[{"Id":"fail_id","Code":"InternalError","Message":"something went wrong","SenderFault":false}]}`,
		},
		{
			name:               "Too Many Entries",
			inputBody:          `{"QueueUrl": "http://localhost/queues/q", "Entries": [` + strings.Repeat(`{"Id":"_","ReceiptHandle":"_"},`, 10) + `{"Id":"_","ReceiptHandle":"_"}]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"TooManyEntriesInBatchRequest", "message":"The batch request contains more entries than permissible."}`,
		},
		{
			name:               "Duplicate Entry Ids",
			inputBody:          `{"QueueUrl": "http://localhost/queues/q", "Entries": [{"Id": "1", "ReceiptHandle": "h1"}, {"Id": "1", "ReceiptHandle": "h2"}]}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"BatchEntryIdsNotDistinct", "message":"Two or more batch entries in the request have the same Id."}`,
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
			req.Header.Set("X-Amz-Target", "AmazonSQS.DeleteMessageBatch")
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
		responseLength     int
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
		{
			name:      "Malformed JSON is ignored",
			inputBody: `{"MaxResults": 1, "QueueNamePrefix": "test"`,
			mockSetup: func(ms *MockStore) {
				// The handler should proceed with default values, so the mock expects 0, "", ""
				ms.On("ListQueues", mock.Anything, 0, "", "").Return([]string{"q1"}, "q1", nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"QueueUrls":["http://localhost:8080/queues/q1"],"NextToken":"q1"}`,
		},
		{
			name:      "Store Internal Error",
			inputBody: `{}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ListQueues", mock.Anything, 0, "", "").Return(nil, "", errors.New("internal error"))
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       `{"__type":"InternalFailure", "message":"Failed to list queues"}`,
		},
		{
			name:      "Over 1000 queues returned",
			inputBody: `{}`, // No MaxResults
			mockSetup: func(ms *MockStore) {
				queues := make([]string, 1001)
				for i := 0; i < 1001; i++ {
					queues[i] = fmt.Sprintf("q%d", i)
				}
				ms.On("ListQueues", mock.Anything, 0, "", "").Return(queues, "next", nil)
			},
			expectedStatusCode: http.StatusOK,
			responseLength:     1000, // Should be truncated to 1000
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

			if tc.responseLength > 0 {
				var resp models.ListQueuesResponse
				err := json.Unmarshal(rr.Body.Bytes(), &resp)
				require.NoError(t, err)
				assert.Len(t, resp.QueueUrls, tc.responseLength)
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

func TestChangeMessageVisibilityHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Change",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "ReceiptHandle": "valid-handle", "VisibilityTimeout": 60}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ChangeMessageVisibility", mock.Anything, "my-queue", "valid-handle", 60).Return(nil).Once()
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       "",
		},
		{
			name:               "Missing QueueUrl",
			inputBody:          `{"ReceiptHandle": "a-handle", "VisibilityTimeout": 60}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
		},
		{
			name:               "Missing ReceiptHandle",
			inputBody:          `{"QueueUrl": "http://localhost/queues/my-queue", "VisibilityTimeout": 60}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a ReceiptHandle."}`,
		},
		{
			name:               "VisibilityTimeout Too Low",
			inputBody:          `{"QueueUrl": "http://localhost/queues/my-queue", "ReceiptHandle": "a-handle", "VisibilityTimeout": -1}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Value for parameter VisibilityTimeout is invalid. Reason: Must be an integer from 0 to 43200."}`,
		},
		{
			name:               "VisibilityTimeout Too High",
			inputBody:          `{"QueueUrl": "http://localhost/queues/my-queue", "ReceiptHandle": "a-handle", "VisibilityTimeout": 43201}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"InvalidParameterValue", "message":"Value for parameter VisibilityTimeout is invalid. Reason: Must be an integer from 0 to 43200."}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "http://localhost/queues/non-existent-queue", "ReceiptHandle": "a-handle", "VisibilityTimeout": 60}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ChangeMessageVisibility", mock.Anything, "non-existent-queue", "a-handle", 60).Return(store.ErrQueueDoesNotExist).Once()
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		{
			name:      "Invalid Receipt Handle",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "ReceiptHandle": "invalid-handle", "VisibilityTimeout": 60}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ChangeMessageVisibility", mock.Anything, "my-queue", "invalid-handle", 60).Return(store.ErrInvalidReceiptHandle).Once()
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"ReceiptHandleIsInvalid", "message":"The specified receipt handle isn't valid."}`,
		},
		{
			name:      "Message Not In-flight",
			inputBody: `{"QueueUrl": "http://localhost/queues/my-queue", "ReceiptHandle": "a-handle", "VisibilityTimeout": 60}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ChangeMessageVisibility", mock.Anything, "my-queue", "a-handle", 60).Return(store.ErrMessageNotInflight).Once()
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MessageNotInflight", "message":"The specified message isn't in flight."}`,
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
			req.Header.Set("X-Amz-Target", "AmazonSQS.ChangeMessageVisibility")
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
