package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApp_UnimplementedHandlers(t *testing.T) {
	unimplementedRoutes := map[string]string{
		"POST /queues/q/permissions":          "AmazonSQS.AddPermission",
		"DELETE /queues/q/permissions/l":      "AmazonSQS.RemovePermission",
		"GET /queues/q/tags":                  "AmazonSQS.ListQueueTags",
		"POST /queues/q/tags":                 "AmazonSQS.TagQueue",
		"DELETE /queues/q/tags":               "AmazonSQS.UntagQueue",
		"GET /dead-letter-source-queues":      "AmazonSQS.ListDeadLetterSourceQueues",
		"POST /message-move-tasks":            "AmazonSQS.StartMessageMoveTask",
		"POST /message-move-tasks/t/cancel":   "AmazonSQS.CancelMessageMoveTask",
		"GET /message-move-tasks":             "AmazonSQS.ListMessageMoveTasks",
	}

	for route, target := range unimplementedRoutes {
		t.Run(target, func(t *testing.T) {
			mockStore := new(MockStore)
			app := &App{Store: mockStore}
			r := chi.NewRouter()
			app.RegisterSQSHandlers(r)

			method, path, _ := strings.Cut(route, " ")

			req, _ := http.NewRequest(method, path, bytes.NewBufferString(`{}`))
			if target != "" {
				req.Header.Set("X-Amz-Target", target)
			}
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusNotImplemented, rr.Code)
			expectedBody := `{"__type":"NotImplemented", "message":"The requested action is not implemented."}`
			require.JSONEq(t, expectedBody, rr.Body.String())

			mockStore.AssertExpectations(t)
		})
	}
}
