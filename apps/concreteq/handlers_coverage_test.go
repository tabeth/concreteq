package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tabeth/concreteq/server"
	"github.com/tabeth/concreteq/store"
)

func TestHandlers_Coverage_Permissions(t *testing.T) {
	app, s := setupHandlerTest(t)
	s.CreateQueue(t.Context(), "perm-Q", nil, nil) // Ensures mocked behavior if needed, or simple setup

	// AddPermission Validation Tests
	t.Run("AddPermission - Validation", func(t *testing.T) {
		tests := []struct {
			name   string
			body   string
			status int
		}{
			{"Invalid JSON", `invalid`, http.StatusBadRequest},
			{"No QueueUrl", `{"Label":"l", "AWSAccountIds":["1"],"Actions":["*"]}`, http.StatusBadRequest},
			{"No Label", `{"QueueUrl":"q", "AWSAccountIds":["1"],"Actions":["*"]}`, http.StatusBadRequest},
			{"Invalid Label", `{"QueueUrl":"q", "Label":"invalid!", "AWSAccountIds":["1"],"Actions":["*"]}`, http.StatusBadRequest},
			{"No Accounts", `{"QueueUrl":"q", "Label":"l", "Actions":["*"]}`, http.StatusBadRequest},
			{"No Actions", `{"QueueUrl":"q", "Label":"l", "AWSAccountIds":["1"]}`, http.StatusBadRequest},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte(tc.body)))
				req.Header.Set("X-Amz-Target", "AmazonSQS.AddPermission")
				w := httptest.NewRecorder()
				app.RootSQSHandler(w, req)
				assert.Equal(t, tc.status, w.Code)
			})
		}
	})

	// RemovePermission Validation Tests
	t.Run("RemovePermission - Validation", func(t *testing.T) {
		tests := []struct {
			name   string
			body   string
			status int
		}{
			{"Invalid JSON", `invalid`, http.StatusBadRequest},
			{"No QueueUrl", `{"Label":"l"}`, http.StatusBadRequest},
			{"No Label", `{"QueueUrl":"q"}`, http.StatusBadRequest},
			{"Invalid Label", `{"QueueUrl":"q", "Label":"invalid!"}`, http.StatusBadRequest},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte(tc.body)))
				req.Header.Set("X-Amz-Target", "AmazonSQS.RemovePermission")
				w := httptest.NewRecorder()
				app.RootSQSHandler(w, req)
				assert.Equal(t, tc.status, w.Code)
			})
		}
	})
}

func TestHandlers_Coverage_Tagging(t *testing.T) {
	// We need a mock store to force errors
	mockStore := new(MockStore)
	app := &server.App{Store: mockStore}
	r := chi.NewRouter()
	app.RegisterSQSHandlers(r)

	t.Run("TagQueue - Error Paths", func(t *testing.T) {
		tests := []struct {
			name      string
			setup     func()
			body      string
			wantCode  int
			wantError string // partial match
		}{
			{
				name:      "Invalid JSON",
				setup:     func() {},
				body:      `invalid`,
				wantCode:  http.StatusBadRequest,
				wantError: "InvalidRequest",
			},
			{
				name:      "Missing QueueUrl",
				setup:     func() {},
				body:      `{"Tags":{"a":"b"}}`,
				wantCode:  http.StatusBadRequest,
				wantError: "MissingParameter",
			},
			{
				name:      "Missing Tags",
				setup:     func() {},
				body:      `{"QueueUrl":"q"}`,
				wantCode:  http.StatusBadRequest,
				wantError: "MissingParameter",
			},
			{
				name: "QueueDoesNotExist",
				setup: func() {
					mockStore.On("TagQueue", mock.Anything, "q", mock.Anything).Return(store.ErrQueueDoesNotExist).Once()
				},
				body:      `{"QueueUrl":"http://host/q", "Tags":{"a":"b"}}`,
				wantCode:  http.StatusBadRequest,
				wantError: "QueueDoesNotExist",
			},
			{
				name: "InternalFailure",
				setup: func() {
					mockStore.On("TagQueue", mock.Anything, "q", mock.Anything).Return(assert.AnError).Once()
				},
				body:      `{"QueueUrl":"http://host/q", "Tags":{"a":"b"}}`,
				wantCode:  http.StatusInternalServerError,
				wantError: "InternalFailure",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				tc.setup()
				req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte(tc.body)))
				req.Header.Set("X-Amz-Target", "AmazonSQS.TagQueue")
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				assert.Equal(t, tc.wantCode, w.Code)
				if tc.wantError != "" {
					assert.Contains(t, w.Body.String(), tc.wantError)
				}
			})
		}
	})

	t.Run("UntagQueue - Error Paths", func(t *testing.T) {
		tests := []struct {
			name      string
			setup     func()
			body      string
			wantCode  int
			wantError string
		}{
			{
				name:      "Invalid JSON",
				setup:     func() {},
				body:      `invalid`,
				wantCode:  http.StatusBadRequest,
				wantError: "InvalidRequest",
			},
			{
				name:      "Missing QueueUrl",
				setup:     func() {},
				body:      `{"TagKeys":["a"]}`,
				wantCode:  http.StatusBadRequest,
				wantError: "MissingParameter",
			},
			{
				name:      "Missing TagKeys",
				setup:     func() {},
				body:      `{"QueueUrl":"q"}`,
				wantCode:  http.StatusBadRequest,
				wantError: "MissingParameter",
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				tc.setup()
				req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte(tc.body)))
				req.Header.Set("X-Amz-Target", "AmazonSQS.UntagQueue")
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				assert.Equal(t, tc.wantCode, w.Code)
				if tc.wantError != "" {
					assert.Contains(t, w.Body.String(), tc.wantError)
				}
			})
		}
	})

	t.Run("MessageMoveTask - Validation", func(t *testing.T) {
		t.Run("StartMessageMoveTask", func(t *testing.T) {
			tests := []struct {
				name   string
				body   string
				status int
			}{
				{"Invalid JSON", `invalid`, http.StatusBadRequest},
				{"Missing SourceArn", `{}`, http.StatusBadRequest},
				{"Invalid SourceArn", `{"SourceArn":"invalid"}`, http.StatusBadRequest},
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte(tc.body)))
					req.Header.Set("X-Amz-Target", "AmazonSQS.StartMessageMoveTask")
					w := httptest.NewRecorder()
					app.RootSQSHandler(w, req)
					assert.Equal(t, tc.status, w.Code)
				})
			}
		})

		t.Run("CancelMessageMoveTask", func(t *testing.T) {
			tests := []struct {
				name   string
				body   string
				status int
			}{
				{"Invalid JSON", `invalid`, http.StatusBadRequest},
				{"Missing TaskHandle", `{}`, http.StatusBadRequest},
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte(tc.body)))
					req.Header.Set("X-Amz-Target", "AmazonSQS.CancelMessageMoveTask")
					w := httptest.NewRecorder()
					app.RootSQSHandler(w, req)
					assert.Equal(t, tc.status, w.Code)
				})
			}
		})

		t.Run("ListMessageMoveTasks", func(t *testing.T) {
			tests := []struct {
				name   string
				body   string
				status int
			}{
				{"Invalid JSON", `invalid`, http.StatusBadRequest},
				{"Missing SourceArn", `{}`, http.StatusBadRequest},
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					req := httptest.NewRequest("POST", "/", bytes.NewReader([]byte(tc.body)))
					req.Header.Set("X-Amz-Target", "AmazonSQS.ListMessageMoveTasks")
					w := httptest.NewRecorder()
					app.RootSQSHandler(w, req)
					assert.Equal(t, tc.status, w.Code)
				})
			}
		})
	})
}
