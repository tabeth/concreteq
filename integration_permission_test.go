package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
)

func TestAddPermission(t *testing.T) {
	app, teardown := setupIntegrationTest(t)
	defer teardown()

	queueName := "test-permission-queue"

	// Create Queue
	createBody := `{"QueueName": "` + queueName + `"}`
	req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBufferString(createBody))
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	queueURL := app.baseURL + "/queues/" + queueName

	t.Run("AddPermission Success", func(t *testing.T) {
		reqBody := models.AddPermissionRequest{
			QueueUrl:      queueURL,
			Label:         "test-label",
			AWSAccountIds: []string{"123456789012"},
			Actions:       []string{"SendMessage", "ReceiveMessage"},
		}

		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.AddPermission")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify Policy via GetQueueAttributes
		getAttrsReq := models.GetQueueAttributesRequest{
			QueueUrl:       queueURL,
			AttributeNames: []string{"Policy"},
		}
		getAttrsBytes, _ := json.Marshal(getAttrsReq)
		req, _ = http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(getAttrsBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.GetQueueAttributes")
		getAttrsResp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer getAttrsResp.Body.Close()

		require.Equal(t, http.StatusOK, getAttrsResp.StatusCode)

		var result models.GetQueueAttributesResponse
		json.NewDecoder(getAttrsResp.Body).Decode(&result)

		policyStr, ok := result.Attributes["Policy"]
		require.True(t, ok, "Policy attribute should exist")

		var policy struct {
			Statement []struct {
				Sid       string
				Principal map[string]interface{}
				Action    interface{}
				Effect    string
			}
		}
		err = json.Unmarshal([]byte(policyStr), &policy)
		require.NoError(t, err)

		found := false
		for _, stmt := range policy.Statement {
			if stmt.Sid == "test-label" {
				found = true
				assert.Equal(t, "Allow", stmt.Effect)

				awsPrincipal, ok := stmt.Principal["AWS"]
				require.True(t, ok)

				awsList, ok := awsPrincipal.([]interface{})
				if !ok {
					t.Fatalf("Expected AWS Principal to be a list, got %T", awsPrincipal)
				}
				assert.Contains(t, awsList, "123456789012")

				actionList, ok := stmt.Action.([]interface{})
				if !ok {
					t.Fatalf("Expected Action to be a list, got %T", stmt.Action)
				}
				assert.Len(t, actionList, 2)
				assert.Contains(t, actionList, "SendMessage")
				assert.Contains(t, actionList, "ReceiveMessage")
			}
		}
		assert.True(t, found, "Policy statement with label 'test-label' not found")
	})

	t.Run("AddPermission Overwrite", func(t *testing.T) {
		// Overwrite the same label with different permissions
		reqBody := models.AddPermissionRequest{
			QueueUrl:      queueURL,
			Label:         "test-label",
			AWSAccountIds: []string{"987654321098"},
			Actions:       []string{"DeleteMessage"},
		}

		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.AddPermission")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify Policy
		getAttrsReq := models.GetQueueAttributesRequest{
			QueueUrl:       queueURL,
			AttributeNames: []string{"Policy"},
		}
		getAttrsBytes, _ := json.Marshal(getAttrsReq)
		req, _ = http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(getAttrsBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.GetQueueAttributes")
		getAttrsResp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer getAttrsResp.Body.Close()

		var result models.GetQueueAttributesResponse
		json.NewDecoder(getAttrsResp.Body).Decode(&result)

		policyStr := result.Attributes["Policy"]
		var policy struct {
			Statement []struct {
				Sid       string
				Principal map[string]interface{}
				Action    interface{}
			}
		}
		json.Unmarshal([]byte(policyStr), &policy)

		found := false
		count := 0
		for _, stmt := range policy.Statement {
			if stmt.Sid == "test-label" {
				found = true
				count++

				awsPrincipal := stmt.Principal["AWS"].([]interface{})
				assert.Contains(t, awsPrincipal, "987654321098")
				assert.NotContains(t, awsPrincipal, "123456789012")

				actionList := stmt.Action.([]interface{})
				assert.Contains(t, actionList, "DeleteMessage")
				assert.NotContains(t, actionList, "SendMessage")
			}
		}
		assert.True(t, found)
		assert.Equal(t, 1, count, "Should only have one statement with this label")
	})

	t.Run("AddPermission Invalid Account ID", func(t *testing.T) {
		reqBody := models.AddPermissionRequest{
			QueueUrl:      queueURL,
			Label:         "bad-account",
			AWSAccountIds: []string{"bad-id"}, // Not 12 digits
			Actions:       []string{"SendMessage"},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.AddPermission")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("AddPermission Missing Queue", func(t *testing.T) {
		reqBody := models.AddPermissionRequest{
			QueueUrl:      app.baseURL + "/queues/non-existent-queue",
			Label:         "label",
			AWSAccountIds: []string{"123456789012"},
			Actions:       []string{"SendMessage"},
		}
		bodyBytes, _ := json.Marshal(reqBody)
		req, _ := http.NewRequest("POST", app.baseURL+"/", bytes.NewBuffer(bodyBytes))
		req.Header.Set("X-Amz-Target", "AmazonSQS.AddPermission")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}
