package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
)

func TestIntegration_MessageMoveTasks(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	// Setup: Create the source and destination queues required for the tests.
	sourceQueueName := "source-queue"
	destinationQueueName := "destination-queue"
	_, err := app.store.CreateQueue(context.Background(), sourceQueueName, nil, nil)
	require.NoError(t, err)
	_, err = app.store.CreateQueue(context.Background(), destinationQueueName, nil, nil)
	require.NoError(t, err)

	sourceArn := "arn:aws:sqs:us-east-1:123456789012:" + sourceQueueName
	destinationArn := "arn:aws:sqs:us-east-1:123456789012:" + destinationQueueName

	var taskHandle string

	t.Run("StartMessageMoveTask", func(t *testing.T) {
		reqBody := models.StartMessageMoveTaskRequest{
			SourceArn:      sourceArn,
			DestinationArn: destinationArn,
		}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/message-move-tasks", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var respData models.StartMessageMoveTaskResponse
		err = json.NewDecoder(resp.Body).Decode(&respData)
		assert.NoError(t, err)
		assert.NotEmpty(t, respData.TaskHandle)
		taskHandle = respData.TaskHandle
	})

	t.Run("ListMessageMoveTasks", func(t *testing.T) {
		reqBody := models.ListMessageMoveTasksRequest{
			SourceArn: sourceArn,
		}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/message-move-tasks", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var respData models.ListMessageMoveTasksResponse
		err = json.NewDecoder(resp.Body).Decode(&respData)
		assert.NoError(t, err)
		require.NotEmpty(t, respData.Results)
		assert.Equal(t, taskHandle, respData.Results[0].TaskHandle)
		assert.Equal(t, sourceArn, respData.Results[0].SourceArn)
		assert.Equal(t, "RUNNING", respData.Results[0].Status)
	})

	t.Run("CancelMessageMoveTask", func(t *testing.T) {
		// Use the correct URL path as per handlers.go registration:
		// r.Post("/message-move-tasks/{taskHandle}/cancel", app.CancelMessageMoveTaskHandler)
		// But wait, the handler reads the body for TaskHandle too?
		// Let's check handlers.go.
		// `CancelMessageMoveTaskHandler` decodes `req.TaskHandle`.
		// The route is registered as `/message-move-tasks/{taskHandle}/cancel`.
		// This implies the TaskHandle should be in the URL path, OR the handler ignores the path param and reads body?
		// Handler uses `json.NewDecoder(r.Body).Decode(&req)`.
		// So it expects a JSON body. The path param in the router might be just for URL structure or unused if handler doesn't read it.
		// However, in standard Chi, if it's in URL, you usually use `chi.URLParam`.
		// Let's verify `handlers.go`.
		// The registration: `r.Post("/message-move-tasks/{taskHandle}/cancel", app.CancelMessageMoveTaskHandler)`
		// The handler: `var req models.CancelMessageMoveTaskRequest ... Decode(&req) ... req.TaskHandle`.
		// It seems the handler relies on the body. So the URL path param `{taskHandle}` is effectively ignored by the handler logic shown,
		// but the router requires it to match.
		// So I must include *some* string in the URL, and the actual handle in the body.

		reqBody := models.CancelMessageMoveTaskRequest{
			TaskHandle: taskHandle,
		}
		body, _ := json.Marshal(reqBody)
		// Construct URL with the task handle in path
		url := fmt.Sprintf("%s/message-move-tasks/%s/cancel", app.baseURL, taskHandle)

		req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var respData models.CancelMessageMoveTaskResponse
		err = json.NewDecoder(resp.Body).Decode(&respData)
		assert.NoError(t, err)
		assert.Equal(t, taskHandle, respData.TaskHandle)
		assert.Equal(t, "CANCELLED", respData.Status)
	})

	t.Run("ListMessageMoveTasks_AfterCancel", func(t *testing.T) {
		reqBody := models.ListMessageMoveTasksRequest{
			SourceArn: sourceArn,
		}
		body, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/message-move-tasks", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var respData models.ListMessageMoveTasksResponse
		err = json.NewDecoder(resp.Body).Decode(&respData)
		assert.NoError(t, err)
		require.NotEmpty(t, respData.Results)
		found := false
		for _, task := range respData.Results {
			if task.TaskHandle == taskHandle {
				assert.Equal(t, "CANCELLED", task.Status)
				found = true
			}
		}
		assert.True(t, found)
	})
}
