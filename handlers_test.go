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

func (m *MockStore) CreateQueue(ctx context.Context, name string, attributes map[string]string, tags map[string]string) error {
	args := m.Called(ctx, name, attributes, tags)
	return args.Error(0)
}

// Implement the rest of the Store interface methods as needed for tests...
func (m *MockStore) DeleteQueue(ctx context.Context, name string) error { return nil }
func (m *MockStore) ListQueues(ctx context.Context) ([]string, error)   { return nil, nil }
func (m *MockStore) GetQueueAttributes(ctx context.Context, name string) (map[string]string, error) {
	return nil, nil
}
func (m *MockStore) SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error {
	return nil
}
func (m *MockStore) GetQueueURL(ctx context.Context, name string) (string, error) { return "", nil }
func (m *MockStore) PurgeQueue(ctx context.Context, name string) error            { return nil }
func (m *MockStore) SendMessage(ctx context.Context, queueName string, messageBody string) (string, error) {
	return "", nil
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
			name:      "Successful Standard Queue Creation",
			inputBody: `{"QueueName": "my-test-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "my-test-queue", mock.AnythingOfType("map[string]string"), mock.AnythingOfType("map[string]string")).Return(nil)
			},
			expectedStatusCode: http.StatusCreated,
			expectedBody:       `{"QueueUrl":"http://localhost:8080/queues/my-test-queue"}`,
		},
		{
			name:      "Successful FIFO Queue Creation with High Throughput",
			inputBody: `{"QueueName": "my-queue.fifo", "Attributes": {"FifoQueue": "true", "DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}}`,
			mockSetup: func(ms *MockStore) {
				attrs := map[string]string{"FifoQueue": "true", "DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}
				ms.On("CreateQueue", mock.Anything, "my-queue.fifo", attrs, mock.AnythingOfType("map[string]string")).Return(nil)
			},
			expectedStatusCode: http.StatusCreated,
		},
		{
			name:               "Invalid FIFO Queue - Name has suffix but attribute is missing",
			inputBody:          `{"QueueName": "my-queue.fifo"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "Queue name ends in .fifo but FifoQueue attribute is not 'true'",
		},
		{
			name:               "Invalid Standard Queue - Name has no suffix but attribute is true",
			inputBody:          `{"QueueName": "my-queue", "Attributes": {"FifoQueue": "true"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "FifoQueue attribute is 'true' but queue name does not end in .fifo",
		},
		{
			name:               "Invalid Attribute Value - VisibilityTimeout out of range",
			inputBody:          `{"QueueName": "my-queue", "Attributes": {"VisibilityTimeout": "50000"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "invalid value for VisibilityTimeout: must be between 0 and 43200",
		},
		{
			name:      "Queue Already Exists",
			inputBody: `{"QueueName": "existing-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "existing-queue", mock.Anything, mock.Anything).Return(store.ErrQueueAlreadyExists)
			},
			expectedStatusCode: http.StatusConflict,
			expectedBody:       "Queue already exists",
		},
		{
			name:               "Invalid High Throughput - perMessageGroupId without messageGroup scope",
			inputBody:          `{"QueueName": "my-queue.fifo", "Attributes": {"FifoQueue": "true", "DeduplicationScope": "queue", "FifoThroughputLimit": "perMessageGroupId"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "FifoThroughputLimit can be set to perMessageGroupId only when DeduplicationScope is messageGroup",
		},
		{
			name:               "Invalid High Throughput - Attribute on non-FIFO queue",
			inputBody:          `{"QueueName": "my-standard-queue", "Attributes": {"DeduplicationScope": "messageGroup"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "DeduplicationScope is only valid for FIFO queues",
		},
		{
			name:      "Valid Redrive Policy",
			inputBody: `{"QueueName": "my-queue-with-dlq", "Attributes": {"RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:my-dlq\",\"maxReceiveCount\":\"5\"}"}}`,
			mockSetup: func(ms *MockStore) {
				attrs := map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:my-dlq","maxReceiveCount":"5"}`}
				ms.On("CreateQueue", mock.Anything, "my-queue-with-dlq", attrs, mock.Anything).Return(nil)
			},
			expectedStatusCode: http.StatusCreated,
		},
		{
			name:               "Invalid Redrive Policy - Bad JSON",
			inputBody:          `{"QueueName": "my-queue", "Attributes": {"RedrivePolicy": "{\"deadLetterTargetArn\"}"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "invalid value for RedrivePolicy: must be a valid JSON object",
		},
		{
			name:               "Invalid Redrive Policy - Invalid maxReceiveCount",
			inputBody:          `{"QueueName": "my-queue", "Attributes": {"RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:123456789012:my-dlq\",\"maxReceiveCount\":\"2000\"}"}}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "invalid value for RedrivePolicy: maxReceiveCount must be an integer between 1 and 1000",
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
