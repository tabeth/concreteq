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
func (m *MockStore) SendMessageBatch(ctx context.Context, queueName string, messages []string) ([]string, error) {
	return nil, nil
}
func (m *MockStore) ReceiveMessage(ctx context.Context, queueName string) (string, string, error) {
	return "", "", nil
}
func (m *MockStore) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	return nil
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
			name:      "Successful Queue Creation",
			inputBody: `{"QueueName": "my-test-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "my-test-queue", mock.Anything, mock.Anything).Return(nil)
			},
			expectedStatusCode: http.StatusCreated,
			expectedBody:       `{"QueueUrl":"http://localhost:8080/queues/my-test-queue"}`,
		},
		{
			name:               "Invalid JSON Body",
			inputBody:          `{"QueueName": "my-test-queue"`, // Malformed JSON
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "Invalid request body",
		},
		{
			name:               "Invalid Queue Name - Too Long",
			inputBody:          `{"QueueName": "` + strings.Repeat("a", 81) + `"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length.",
		},
		{
			name:               "Invalid Queue Name - Invalid Characters",
			inputBody:          `{"QueueName": "my-queue!"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length.",
		},
		{
			name:      "Store Error on Creation",
			inputBody: `{"QueueName": "existing-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "existing-queue", mock.Anything, mock.Anything).Return(assert.AnError)
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       "Failed to create queue",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/queues", bytes.NewBufferString(tc.inputBody))
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") {
					var expectedResp, actualResp models.CreateQueueResponse
					err := json.Unmarshal([]byte(tc.expectedBody), &expectedResp)
					assert.NoError(t, err)
					err = json.Unmarshal(rr.Body.Bytes(), &actualResp)
					assert.NoError(t, err)
					assert.Equal(t, expectedResp, actualResp)
				} else {
					assert.Equal(t, tc.expectedBody, strings.TrimSpace(rr.Body.String()))
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestPurgeQueueHandler(t *testing.T) {
	tests := []struct {
		name               string
		queueName          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Purge",
			queueName: "my-queue",
			mockSetup: func(ms *MockStore) {
				ms.On("PurgeQueue", mock.Anything, "my-queue").Return(nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       "",
		},
		{
			name:      "Queue Not Found",
			queueName: "non-existent-queue",
			mockSetup: func(ms *MockStore) {
				ms.On("PurgeQueue", mock.Anything, "non-existent-queue").Return(store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "QueueDoesNotExist: The specified queue does not exist.",
		},
		{
			name:      "Purge In Progress",
			queueName: "purging-queue",
			mockSetup: func(ms *MockStore) {
				ms.On("PurgeQueue", mock.Anything, "purging-queue").Return(store.ErrPurgeQueueInProgress)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "PurgeQueueInProgress: Indicates that the specified queue previously received a PurgeQueue request within the last 60 seconds.",
		},
		{
			name:      "Store Error",
			queueName: "error-queue",
			mockSetup: func(ms *MockStore) {
				ms.On("PurgeQueue", mock.Anything, "error-queue").Return(assert.AnError)
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       "Failed to purge queue",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("POST", "/queues/"+tc.queueName+"/purge", nil)
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)
			if tc.expectedBody != "" {
				assert.Equal(t, tc.expectedBody, strings.TrimSpace(rr.Body.String()))
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestDeleteQueueHandler(t *testing.T) {
	tests := []struct {
		name               string
		queueName          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Deletion",
			queueName: "my-queue",
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteQueue", mock.Anything, "my-queue").Return(nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       "",
		},
		{
			name:      "Queue Not Found",
			queueName: "non-existent-queue",
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteQueue", mock.Anything, "non-existent-queue").Return(store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "QueueDoesNotExist: The specified queue does not exist.",
		},
		{
			name:      "Store Error",
			queueName: "error-queue",
			mockSetup: func(ms *MockStore) {
				ms.On("DeleteQueue", mock.Anything, "error-queue").Return(assert.AnError)
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       "Failed to delete queue",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("DELETE", "/queues/"+tc.queueName, nil)
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)
			if tc.expectedBody != "" {
				assert.Equal(t, tc.expectedBody, strings.TrimSpace(rr.Body.String()))
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestListQueuesHandler(t *testing.T) {
	tests := []struct {
		name               string
		requestURL         string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:       "Successful Listing - No Params",
			requestURL: "/queues",
			mockSetup: func(ms *MockStore) {
				ms.On("ListQueues", mock.Anything, 0, "", "").Return([]string{"q1", "q2"}, "", nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"QueueUrls":["http://localhost:8080/queues/q1","http://localhost:8080/queues/q2"]}`,
		},
		{
			name:       "Successful Listing - With MaxResults",
			requestURL: "/queues?MaxResults=1",
			mockSetup: func(ms *MockStore) {
				ms.On("ListQueues", mock.Anything, 1, "", "").Return([]string{"q1"}, "q1", nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"QueueUrls":["http://localhost:8080/queues/q1"],"NextToken":"q1"}`,
		},
		{
			name:       "Successful Listing - With Prefix",
			requestURL: "/queues?QueueNamePrefix=test",
			mockSetup: func(ms *MockStore) {
				ms.On("ListQueues", mock.Anything, 0, "", "test").Return([]string{"test-q1"}, "", nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"QueueUrls":["http://localhost:8080/queues/test-q1"]}`,
		},
		{
			name:       "Successful Listing - Pagination",
			requestURL: "/queues?MaxResults=1&NextToken=q1",
			mockSetup: func(ms *MockStore) {
				ms.On("ListQueues", mock.Anything, 1, "q1", "").Return([]string{"q2"}, "", nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"QueueUrls":["http://localhost:8080/queues/q2"]}`,
		},
		{
			name:               "Invalid MaxResults - Non-integer",
			requestURL:         "/queues?MaxResults=abc",
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "Invalid MaxResults value. It must be an integer between 1 and 1000.",
		},
		{
			name:               "Invalid MaxResults - Out of Range",
			requestURL:         "/queues?MaxResults=1001",
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "Invalid MaxResults value. It must be an integer between 1 and 1000.",
		},
		{
			name:       "Store Error",
			requestURL: "/queues",
			mockSetup: func(ms *MockStore) {
				ms.On("ListQueues", mock.Anything, 0, "", "").Return(nil, "", assert.AnError)
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       "Failed to list queues",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := new(MockStore)
			tc.mockSetup(mockStore)

			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			req, _ := http.NewRequest("GET", tc.requestURL, nil)
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") {
					var expectedResp models.ListQueuesResponse
					err := json.Unmarshal([]byte(tc.expectedBody), &expectedResp)
					assert.NoError(t, err)

					var actualResp models.ListQueuesResponse
					err = json.Unmarshal(rr.Body.Bytes(), &actualResp)
					assert.NoError(t, err)
					assert.Equal(t, expectedResp, actualResp)
				} else {
					assert.Equal(t, tc.expectedBody, strings.TrimSpace(rr.Body.String()))
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
			name:      "Successful Send",
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
			name:               "Missing QueueUrl",
			inputBody:          `{"MessageBody": "hello"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "MissingParameter: The request must contain a QueueUrl.",
		},
		{
			name:               "Empty Message Body",
			inputBody:          `{"MessageBody": "", "QueueUrl": "http://localhost:8080/queues/my-queue"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "InvalidParameterValue: The message body must be between 1 and 262144 bytes long.",
		},
		{
			name:               "Message Body Too Large",
			inputBody:          `{"MessageBody": "` + strings.Repeat("a", 257*1024) + `", "QueueUrl": "http://localhost:8080/queues/my-queue"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "InvalidParameterValue: The message body must be between 1 and 262144 bytes long.",
		},
		{
			name:      "MessageGroupId with Standard Queue",
			inputBody: `{"MessageBody": "hello standard", "MessageGroupId": "group1", "QueueUrl": "http://localhost:8080/queues/my-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("SendMessage", mock.Anything, "my-queue", mock.AnythingOfType("*models.SendMessageRequest")).Return(&models.SendMessageResponse{
					MessageId:      "some-uuid",
					MD5OfMessageBody: "md5-of-body",
				}, nil)
			},
			expectedStatusCode: http.StatusOK,
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
				if strings.HasPrefix(tc.expectedBody, "{") {
					var expectedResp, actualResp models.SendMessageResponse
					err := json.Unmarshal([]byte(tc.expectedBody), &expectedResp)
					assert.NoError(t, err, "failed to unmarshal expected response")
					err = json.Unmarshal(rr.Body.Bytes(), &actualResp)
					assert.NoError(t, err, "failed to unmarshal actual response")
					assert.Equal(t, expectedResp, actualResp)
				} else {
					assert.Equal(t, tc.expectedBody, strings.TrimSpace(rr.Body.String()))
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}
