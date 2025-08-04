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

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockStore is a mock implementation of the Store interface for testing.
type MockStore struct {
	mock.Mock
}

func (m *MockStore) CreateQueue(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

// Implement the rest of the Store interface methods as needed for tests...
// For now, we only need CreateQueue.
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
			name:      "Successful Queue Creation",
			inputBody: `{"QueueName": "my-test-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "my-test-queue").Return(nil)
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
			expectedBody:       "Invalid queue name",
		},
		{
			name:               "Invalid Queue Name - Invalid Characters",
			inputBody:          `{"QueueName": "my-queue!"}`,
			mockSetup:          func(ms *MockStore) {},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "Invalid queue name",
		},
		{
			name:      "Store Error on Creation",
			inputBody: `{"QueueName": "existing-queue"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("CreateQueue", mock.Anything, "existing-queue").Return(assert.AnError)
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
				// For JSON responses, we want to compare the unmarshalled objects
				// to avoid issues with whitespace differences.
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
