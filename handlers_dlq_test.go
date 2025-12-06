package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/store"
)

func TestListDeadLetterSourceQueuesHandler(t *testing.T) {
	tests := []struct {
		name               string
		inputBody          string
		mockSetup          func(*MockStore)
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:      "Successful Listing",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/dlq"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ListDeadLetterSourceQueues", mock.Anything, "dlq").Return([]string{"source-q1", "source-q2"}, nil)
			},
			expectedStatusCode: http.StatusOK,
			expectedBody:       `{"QueueUrls":["http://localhost:8080/queues/source-q1","http://localhost:8080/queues/source-q2"]}`,
		},
		{
			name:      "Queue Does Not Exist",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/non-existent-dlq"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ListDeadLetterSourceQueues", mock.Anything, "non-existent-dlq").Return(nil, store.ErrQueueDoesNotExist)
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"QueueDoesNotExist", "message":"The specified queue does not exist."}`,
		},
		{
			name:      "Missing QueueUrl",
			inputBody: `{}`,
			mockSetup: func(ms *MockStore) {
				// Validation happens before store call
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       `{"__type":"MissingParameter", "message":"The request must contain a QueueUrl."}`,
		},
		{
			name:      "Internal Store Error",
			inputBody: `{"QueueUrl": "http://localhost:8080/queues/dlq"}`,
			mockSetup: func(ms *MockStore) {
				ms.On("ListDeadLetterSourceQueues", mock.Anything, "dlq").Return(nil, fmt.Errorf("internal error"))
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       `{"__type":"InternalFailure", "message":"Failed to list dead letter source queues"}`,
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
			req.Header.Set("X-Amz-Target", "AmazonSQS.ListDeadLetterSourceQueues")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code)

			if tc.expectedBody != "" {
				if strings.HasPrefix(tc.expectedBody, "{") {
					if rr.Code < 300 {
						assert.JSONEq(t, tc.expectedBody, rr.Body.String())
					} else {
						var errResp models.ErrorResponse
						err := json.Unmarshal(rr.Body.Bytes(), &errResp)
						require.NoError(t, err, "failed to unmarshal error response")

						var expectedErrResp models.ErrorResponse
						err = json.Unmarshal([]byte(tc.expectedBody), &expectedErrResp)
						require.NoError(t, err, "failed to unmarshal expected error response")

						assert.Equal(t, expectedErrResp, errResp)
					}
				} else {
					assert.Fail(t, "Received unexpected plain text error response", "Response body: %s", rr.Body.String())
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}
