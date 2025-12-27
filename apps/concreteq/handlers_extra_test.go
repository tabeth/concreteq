package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/server"
	"github.com/tabeth/concreteq/store"
)

func TestTagQueueHandler(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	t.Run("Success", func(t *testing.T) {
		req := models.TagQueueRequest{
			QueueUrl: "http://localhost:8080/queues/test-q",
			Tags:     map[string]string{"Key1": "Val1"},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/tag", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		ms.On("TagQueue", mock.Anything, "test-q", req.Tags).Return(nil).Once()

		app.TagQueueHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
		ms.AssertExpectations(t)
	})
}

func TestUntagQueueHandler(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	t.Run("Success", func(t *testing.T) {
		req := models.UntagQueueRequest{
			QueueUrl: "q",
			TagKeys:  []string{"K1"},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/untag", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		ms.On("UntagQueue", mock.Anything, "q", req.TagKeys).Return(nil).Once()

		app.UntagQueueHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestListQueueTagsHandler(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	t.Run("Success", func(t *testing.T) {
		req := models.ListQueueTagsRequest{QueueUrl: "q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/list-tags", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		tags := map[string]string{"K1": "V1"}
		ms.On("ListQueueTags", mock.Anything, "q").Return(tags, nil).Once()

		app.ListQueueTagsHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestMessageMoveTaskHandlers(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}
	validArn := "arn:aws:sqs:us-east-1:123456789012:my-queue"

	t.Run("Start Success", func(t *testing.T) {
		req := models.StartMessageMoveTaskRequest{
			SourceArn:      validArn,
			DestinationArn: "arn:aws:sqs:us-east-1:123456789012:my-dlq",
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/start-move", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		ms.On("StartMessageMoveTask", mock.Anything, req.SourceArn, req.DestinationArn).Return("handle1", nil).Once()

		app.StartMessageMoveTaskHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("List Success", func(t *testing.T) {
		req := models.ListMessageMoveTasksRequest{SourceArn: validArn}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/list-moves", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		tasks := []models.ListMessageMoveTasksResultEntry{{TaskHandle: "h1"}}
		ms.On("ListMessageMoveTasks", mock.Anything, validArn).Return(tasks, nil).Once()

		app.ListMessageMoveTasksHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestListDeadLetterSourceQueuesHandler_Extra(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	t.Run("Success", func(t *testing.T) {
		req := models.ListDeadLetterSourceQueuesRequest{QueueUrl: "q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/dlq-sources", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		// MaxResults defaults to 1000
		ms.On("ListDeadLetterSourceQueues", mock.Anything, "q", 1000, "").Return([]string{"s1"}, "", nil).Once()

		app.ListDeadLetterSourceQueuesHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestHandlerErrorPathsExtra(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	t.Run("SetQueueAttributes_InvalidRedrivePolicy", func(t *testing.T) {
		req := models.SetQueueAttributesRequest{
			QueueUrl:   "q",
			Attributes: map[string]string{"RedrivePolicy": "invalid-json"},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/set-attrs", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SetQueueAttributes_MissingMaxReceiveCount", func(t *testing.T) {
		req := models.SetQueueAttributesRequest{
			QueueUrl:   "q",
			Attributes: map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn"}`},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/set-attrs", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("MalformedJSON", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/purge", bytes.NewBuffer([]byte("{invalid")))
		w := httptest.NewRecorder()
		app.PurgeQueueHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		app.CreateQueueHandler(w, httptest.NewRequest("POST", "/create", bytes.NewBuffer([]byte("{"))))
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		app.DeleteQueueHandler(w, httptest.NewRequest("POST", "/delete", bytes.NewBuffer([]byte("{"))))
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		app.CancelMessageMoveTaskHandler(w, httptest.NewRequest("POST", "/cancel", bytes.NewBuffer([]byte("{"))))
		assert.Equal(t, http.StatusBadRequest, w.Code)

		w = httptest.NewRecorder()
		app.UntagQueueHandler(w, httptest.NewRequest("POST", "/untag", bytes.NewBuffer([]byte("{"))))
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestChangeMessageVisibilityHandler_Extra(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	t.Run("QueueDoesNotExist", func(t *testing.T) {
		req := models.ChangeMessageVisibilityRequest{
			QueueUrl: "q", ReceiptHandle: "h", VisibilityTimeout: models.Ptr(60),
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/change-vis", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		ms.On("ChangeMessageVisibility", mock.Anything, "q", "h", 60).Return(store.ErrQueueDoesNotExist).Once()
		app.ChangeMessageVisibilityHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestSqsErrorExtra(t *testing.T) {
	err := &server.SqsError{Type: "Test", Message: "Msg"}
	assert.Equal(t, "Msg", err.Error())
}

func TestMoreHandlerErrorPaths(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	t.Run("SendMessage_MissingBody", func(t *testing.T) {
		req := models.SendMessageRequest{QueueUrl: "q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SendMessage_InvalidBodyChar", func(t *testing.T) {
		req := models.SendMessageRequest{QueueUrl: "q", MessageBody: "\x00"} // Null char is invalid
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SendMessage_Fifo_MissingGroupId", func(t *testing.T) {
		req := models.SendMessageRequest{QueueUrl: "q.fifo", MessageBody: "body"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ReceiveMessage_InvalidParams", func(t *testing.T) {
		req := models.ReceiveMessageRequest{QueueUrl: "q", VisibilityTimeout: models.Ptr(-1)}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ReceiveMessageHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("DeleteMessage_MissingHandle", func(t *testing.T) {
		req := models.DeleteMessageRequest{QueueUrl: "q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.DeleteMessageHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SendMessageBatch_DuplicateId", func(t *testing.T) {
		req := models.SendMessageBatchRequest{
			QueueUrl: "q",
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "1", MessageBody: "b1"},
				{Id: "1", MessageBody: "b2"},
			},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("PurgeQueue_InProgress", func(t *testing.T) {
		req := models.PurgeQueueRequest{QueueUrl: "q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		ms.On("PurgeQueue", mock.Anything, "q").Return(store.ErrPurgeQueueInProgress).Once()
		app.PurgeQueueHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("CreateQueue_InvalidName", func(t *testing.T) {
		req := models.CreateQueueRequest{QueueName: "invalid!"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.CreateQueueHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SetQueueAttributes_InvalidVisibility", func(t *testing.T) {
		req := models.SetQueueAttributesRequest{
			QueueUrl:   "q",
			Attributes: map[string]string{"VisibilityTimeout": "999999"},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SetQueueAttributes_InvalidRedriveAllowPolicy", func(t *testing.T) {
		req := models.SetQueueAttributesRequest{
			QueueUrl:   "q",
			Attributes: map[string]string{"RedriveAllowPolicy": `{"redrivePermission":"invalid"}`},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SetQueueAttributes_InvalidKmsMasterKeyId", func(t *testing.T) {
		req := models.SetQueueAttributesRequest{
			QueueUrl:   "q",
			Attributes: map[string]string{"KmsMasterKeyId": " "},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SendMessageBatch_InvalidEntryId", func(t *testing.T) {
		req := models.SendMessageBatchRequest{
			QueueUrl: "q",
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "invalid @ id", MessageBody: "b1"},
			},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SendMessageBatch_TooManyEntries", func(t *testing.T) {
		entries := make([]models.SendMessageBatchRequestEntry, 11)
		for i := 0; i < 11; i++ {
			entries[i] = models.SendMessageBatchRequestEntry{Id: "id", MessageBody: "b"}
		}
		req := models.SendMessageBatchRequest{QueueUrl: "q", Entries: entries}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SendMessageBatch_InvalidDelay", func(t *testing.T) {
		req := models.SendMessageBatchRequest{
			QueueUrl: "q",
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "e1", MessageBody: "b1", DelaySeconds: models.Ptr(1000)},
			},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()

		// The store should return a partial failure for invalid delay
		resp := &models.SendMessageBatchResponse{
			Successful: []models.SendMessageBatchResultEntry{},
			Failed: []models.BatchResultErrorEntry{
				{Id: "e1", Code: "InvalidParameterValue", Message: "DelaySeconds must be 0-900", SenderFault: true},
			},
		}
		ms.On("SendMessageBatch", mock.Anything, "q", mock.Anything).Return(resp, nil).Once()

		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code) // SQS returns 200 for partial failures
	})

	t.Run("SendMessageBatch_Fifo_Delay", func(t *testing.T) {
		req := models.SendMessageBatchRequest{
			QueueUrl: "q.fifo",
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "e1", MessageBody: "b1", DelaySeconds: models.Ptr(10), MessageGroupId: models.Ptr("g1")},
			},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code) // Per-entry failure returns 200
		var resp models.SendMessageBatchResponse
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Len(t, resp.Failed, 1)
	})

	t.Run("SendMessageBatch_NonFifo_DedupId", func(t *testing.T) {
		req := models.SendMessageBatchRequest{
			QueueUrl: "q",
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "e1", MessageBody: "b1", MessageDeduplicationId: models.Ptr("d1")},
			},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code) // Per-entry failure returns 200
		var resp models.SendMessageBatchResponse
		json.Unmarshal(w.Body.Bytes(), &resp)
		assert.Len(t, resp.Failed, 1)
	})

	t.Run("SendMessageBatch_Empty", func(t *testing.T) {
		req := models.SendMessageBatchRequest{QueueUrl: "q", Entries: []models.SendMessageBatchRequestEntry{}}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ListMessageMoveTasks_MissingSourceArn", func(t *testing.T) {
		req := models.ListMessageMoveTasksRequest{}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ListMessageMoveTasksHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("CancelMessageMoveTask_NotFound", func(t *testing.T) {
		ms.On("CancelMessageMoveTask", mock.Anything, "invalid-handle").Return(errors.New("ResourceNotFoundException")).Once()
		req := models.CancelMessageMoveTaskRequest{TaskHandle: "invalid-handle"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.CancelMessageMoveTaskHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("UntagQueue_NotFound", func(t *testing.T) {
		ms.On("UntagQueue", mock.Anything, "non-existent", mock.Anything).Return(store.ErrQueueDoesNotExist).Once()
		req := models.UntagQueueRequest{QueueUrl: "non-existent", TagKeys: []string{"K1"}}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.UntagQueueHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("PurgeQueue_InProgress", func(t *testing.T) {
		ms.On("PurgeQueue", mock.Anything, "q").Return(store.ErrPurgeQueueInProgress).Once()
		req := models.PurgeQueueRequest{QueueUrl: "q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.PurgeQueueHandler(w, r)
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
	})

	t.Run("DeleteQueue_NotFound", func(t *testing.T) {
		ms.On("DeleteQueue", mock.Anything, "non-existent").Return(store.ErrQueueDoesNotExist).Once()
		req := models.DeleteQueueRequest{QueueUrl: "non-existent"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.DeleteQueueHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("Store_InternalFailure", func(t *testing.T) {
		ms.On("SendMessage", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("fdb error")).Once()
		req := models.SendMessageRequest{QueueUrl: "q", MessageBody: "b"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageHandler(w, r)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("UntagQueue_MissingKeys", func(t *testing.T) {
		req := models.UntagQueueRequest{QueueUrl: "q", TagKeys: []string{}}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.UntagQueueHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ListMessageMoveTasks_InternalError", func(t *testing.T) {
		ms.On("ListMessageMoveTasks", mock.Anything, mock.Anything).Return(nil, errors.New("fdb error")).Once()
		req := models.ListMessageMoveTasksRequest{SourceArn: "arn:aws:sqs:us-east-1:123456789012:q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ListMessageMoveTasksHandler(w, r)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("ListDeadLetterSourceQueues_MissingUrl", func(t *testing.T) {
		req := models.ListDeadLetterSourceQueuesRequest{}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ListDeadLetterSourceQueuesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ChangeMessageVisibility_MissingFields", func(t *testing.T) {
		req := models.ChangeMessageVisibilityRequest{QueueUrl: "q"} // Missing ReceiptHandle and VisibilityTimeout
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ChangeMessageVisibilityHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ListDeadLetterSourceQueues_InvalidMaxResults", func(t *testing.T) {
		req := models.ListDeadLetterSourceQueuesRequest{QueueUrl: "q", MaxResults: models.Ptr(0)}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ListDeadLetterSourceQueuesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		req.MaxResults = models.Ptr(1001)
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.ListDeadLetterSourceQueuesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("AddPermission_InvalidAccountId", func(t *testing.T) {
		req := models.AddPermissionRequest{
			QueueUrl:      "q",
			Label:         "L1",
			AWSAccountIds: []string{"invalid"},
			Actions:       []string{"SendMessage"},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.AddPermissionHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ChangeMessageVisibility_InvalidTimeout", func(t *testing.T) {
		req := models.ChangeMessageVisibilityRequest{
			QueueUrl:          "q",
			ReceiptHandle:     "h",
			VisibilityTimeout: models.Ptr(43201),
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ChangeMessageVisibilityHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("DeleteQueue_MissingUrl", func(t *testing.T) {
		req := models.DeleteQueueRequest{}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.DeleteQueueHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ListQueues_DecodingError", func(t *testing.T) {
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer([]byte("{")))
		w := httptest.NewRecorder()
		app.ListQueuesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ChangeMessageVisibility_StoreErrors", func(t *testing.T) {
		ms.On("ChangeMessageVisibility", mock.Anything, "q", "h1", 30).Return(store.ErrInvalidReceiptHandle).Once()
		ms.On("ChangeMessageVisibility", mock.Anything, "q", "h2", 30).Return(store.ErrMessageNotInflight).Once()

		req := models.ChangeMessageVisibilityRequest{QueueUrl: "q", ReceiptHandle: "h1", VisibilityTimeout: models.Ptr(30)}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ChangeMessageVisibilityHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		req.ReceiptHandle = "h2"
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.ChangeMessageVisibilityHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("SendMessageBatch_DuplicateIds_In_Handler", func(t *testing.T) {
		req := models.SendMessageBatchRequest{
			QueueUrl: "q",
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "1", MessageBody: "b1"},
				{Id: "1", MessageBody: "b2"}, // Duplicate ID
			},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		// Store should NOT be called
	})

	t.Run("ListQueueTags_StoreError", func(t *testing.T) {
		ms.On("ListQueueTags", mock.Anything, "q").Return(nil, errors.New("db error")).Once()
		req := models.ListQueueTagsRequest{QueueUrl: "q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ListQueueTagsHandler(w, r)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("GetQueueUrl_StoreError", func(t *testing.T) {
		ms := new(MockStore)
		app := &server.App{Store: ms}
		ms.On("GetQueueURL", mock.Anything, "q").Return("", errors.New("db error")).Once()
		req := models.GetQueueURLRequest{QueueName: "q"}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.GetQueueUrlHandler(w, r)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("SetQueueAttributes_RedrivePolicy_Validation", func(t *testing.T) {
		// Mock GetQueueAttributes for DLQ validation
		ms := new(MockStore)
		app := &server.App{Store: ms}

		// 1. Invalid JSON structure
		req := models.SetQueueAttributesRequest{
			QueueUrl:   "q",
			Attributes: map[string]string{"RedrivePolicy": "{invalid json"},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		// 2. Missing deadLetterTargetArn
		req.Attributes = map[string]string{"RedrivePolicy": `{"maxReceiveCount": 5}`}
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		// 3. MaxReceiveCount not integer
		req.Attributes = map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dlq", "maxReceiveCount": "invalid"}`}
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		// 4. DLQ does not exist
		dlqArn := "arn:aws:sqs:us-east-1:123456789012:dlq"
		req.Attributes = map[string]string{"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s", "maxReceiveCount": 5}`, dlqArn)}
		ms.On("GetQueueAttributes", mock.Anything, "dlq").Return(nil, store.ErrQueueDoesNotExist).Once()
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)

		// 5. DLQ exists but denies (Mocking RedriveAllowPolicy behavior if checkRedrivePolicy implements it)
		// For coverage, we just need to hit the error path if CheckRedrivePolicy logic involves store lookup
	})

	t.Run("CreateQueue_RedrivePolicy_FIFO_Mismatch", func(t *testing.T) {
		ms := new(MockStore)
		app := &server.App{Store: ms}

		// Source is FIFO, DLQ is Standard
		dlqArn := "arn:aws:sqs:us-east-1:123456789012:dlq" // Standard
		req := models.CreateQueueRequest{
			QueueName: "source.fifo",
			Attributes: map[string]string{
				"FifoQueue":     "true",
				"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s", "maxReceiveCount": 5}`, dlqArn),
			},
		}

		// Mock DLQ existence (standard)
		ms.On("GetQueueAttributes", mock.Anything, "dlq").Return(map[string]string{}, nil).Once()

		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.CreateQueueHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "must also be a FIFO queue")
	})

	t.Run("SendMessageBatch_Limits", func(t *testing.T) {
		app := &server.App{} // No store needed for early validation

		// 1. Empty Batch
		req := models.SendMessageBatchRequest{QueueUrl: "q", Entries: []models.SendMessageBatchRequestEntry{}}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "EmptyBatchRequest")

		// 2. Too Many Entries
		entries := make([]models.SendMessageBatchRequestEntry, 11)
		for i := 0; i < 11; i++ {
			entries[i] = models.SendMessageBatchRequestEntry{Id: fmt.Sprintf("id%d", i), MessageBody: "b"}
		}
		req = models.SendMessageBatchRequest{QueueUrl: "q", Entries: entries}
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "TooManyEntriesInBatchRequest")

		// 3. Payload Too Large (Simulate by mocking MaxBytesReader or just large body)
		// Actually, we can just send a large body
		largeBody := make([]byte, 262145) // > 256KB
		req = models.SendMessageBatchRequest{QueueUrl: "q", Entries: []models.SendMessageBatchRequestEntry{{Id: "1", MessageBody: string(largeBody)}}}
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		// MaxBytesReader should catch this during decode if body is too large, OR our manual check
		// The manual check in SendMessageBatchHandler checks totalSize > 256KB
		// But validIdRegex might fail first if we don't set ID? ID is set.
		// Let's use a smaller body but enough to trigger totalSize check if MaxBytesReader allows it.
		// But MaxBytesReader is limited to 262144. So we expect "BatchRequestTooLong" from decoder error.
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("ChangeMessageVisibilityBatch_Limits", func(t *testing.T) {
		app := &server.App{}

		// 1. Empty
		req := models.ChangeMessageVisibilityBatchRequest{QueueUrl: "q", Entries: []models.ChangeMessageVisibilityBatchRequestEntry{}}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ChangeMessageVisibilityBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "EmptyBatchRequest")

		// 2. Too Many
		entries := make([]models.ChangeMessageVisibilityBatchRequestEntry, 11)
		for i := 0; i < 11; i++ {
			entries[i] = models.ChangeMessageVisibilityBatchRequestEntry{Id: fmt.Sprintf("id%d", i), ReceiptHandle: "h", VisibilityTimeout: models.Ptr(30)}
		}
		req = models.ChangeMessageVisibilityBatchRequest{QueueUrl: "q", Entries: entries}
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.ChangeMessageVisibilityBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "TooManyEntriesInBatchRequest")

		// 3. Duplicate IDs
		req = models.ChangeMessageVisibilityBatchRequest{
			QueueUrl: "q",
			Entries: []models.ChangeMessageVisibilityBatchRequestEntry{
				{Id: "dup", ReceiptHandle: "h1", VisibilityTimeout: models.Ptr(30)},
				{Id: "dup", ReceiptHandle: "h2", VisibilityTimeout: models.Ptr(30)},
			},
		}
		body, _ = json.Marshal(req)
		r = httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w = httptest.NewRecorder()
		app.ChangeMessageVisibilityBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "BatchEntryIdsNotDistinct")
	})

	t.Run("GetQueueUrl_MissingName", func(t *testing.T) {
		app := &server.App{}
		req := models.GetQueueURLRequest{QueueName: ""}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.GetQueueUrlHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "MissingParameter")
	})

	t.Run("SetQueueAttributes_RedrivePolicy_Deny", func(t *testing.T) {
		ms := new(MockStore)
		app := &server.App{Store: ms}

		dlqArn := "arn:aws:sqs:us-east-1:123456789012:dlq"
		req := models.SetQueueAttributesRequest{
			QueueUrl: "q",
			Attributes: map[string]string{
				"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s", "maxReceiveCount": 5}`, dlqArn),
			},
		}

		// Mock DLQ check returning "denyAll"
		ms.On("GetQueueAttributes", mock.Anything, "dlq").Return(map[string]string{
			"RedriveAllowPolicy": `{"redrivePermission":"denyAll"}`,
		}, nil).Once()

		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "does not allow this queue")
	})

	t.Run("SetQueueAttributes_RedrivePolicy_ByQueue_Allowed", func(t *testing.T) {
		ms := new(MockStore)
		app := &server.App{Store: ms}

		dlqArn := "arn:aws:sqs:us-east-1:123456789012:dlq"
		req := models.SetQueueAttributesRequest{
			QueueUrl: "http://host/queues/src",
			Attributes: map[string]string{
				"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s", "maxReceiveCount": 5}`, dlqArn),
			},
		}

		// Mock DLQ check returning "byQueue" allowing "src"
		ms.On("GetQueueAttributes", mock.Anything, "dlq").Return(map[string]string{
			"RedriveAllowPolicy": `{"redrivePermission":"byQueue", "sourceQueueArns":["arn:aws:sqs:us-east-1:123456789012:src"]}`,
		}, nil).Once()

		ms.On("SetQueueAttributes", mock.Anything, "src", mock.Anything).Return(nil).Once()

		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("SetQueueAttributes_RedrivePolicy_ByQueue_Denied", func(t *testing.T) {
		ms := new(MockStore)
		app := &server.App{Store: ms}

		dlqArn := "arn:aws:sqs:us-east-1:123456789012:dlq"
		req := models.SetQueueAttributesRequest{
			QueueUrl: "http://host/queues/other",
			Attributes: map[string]string{
				"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s", "maxReceiveCount": 5}`, dlqArn),
			},
		}

		// Mock DLQ check returning "byQueue" allowing "src" only
		ms.On("GetQueueAttributes", mock.Anything, "dlq").Return(map[string]string{
			"RedriveAllowPolicy": `{"redrivePermission":"byQueue", "sourceQueueArns":["arn:aws:sqs:us-east-1:123456789012:src"]}`,
		}, nil).Once()

		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "does not allow this queue")
	})
}

func TestHandler_BatchOperations_StoreErrors(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	t.Run("SendMessageBatch_InternalError", func(t *testing.T) {
		ms.On("SendMessageBatch", mock.Anything, "q", mock.Anything).Return(nil, errors.New("fdb error")).Once()

		req := models.SendMessageBatchRequest{
			QueueUrl: "http://h/q",
			Entries:  []models.SendMessageBatchRequestEntry{{Id: "1", MessageBody: "b"}},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.SendMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("DeleteMessageBatch_QueueDoesNotExist", func(t *testing.T) {
		ms.On("DeleteMessageBatch", mock.Anything, "q", mock.Anything).Return(nil, store.ErrQueueDoesNotExist).Once()

		req := models.DeleteMessageBatchRequest{
			QueueUrl: "http://h/q",
			Entries:  []models.DeleteMessageBatchRequestEntry{{Id: "1", ReceiptHandle: "h"}},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.DeleteMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "QueueDoesNotExist")
	})

	t.Run("DeleteMessageBatch_InternalError", func(t *testing.T) {
		ms.On("DeleteMessageBatch", mock.Anything, "q", mock.Anything).Return(nil, errors.New("fdb error")).Once()

		req := models.DeleteMessageBatchRequest{
			QueueUrl: "http://h/q",
			Entries:  []models.DeleteMessageBatchRequestEntry{{Id: "1", ReceiptHandle: "h"}},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.DeleteMessageBatchHandler(w, r)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	t.Run("ChangeMessageVisibilityBatch_InternalError", func(t *testing.T) {
		ms.On("ChangeMessageVisibilityBatch", mock.Anything, "q", mock.Anything).Return(&models.ChangeMessageVisibilityBatchResponse{}, errors.New("fdb error")).Once()

		req := models.ChangeMessageVisibilityBatchRequest{
			QueueUrl: "http://h/q",
			Entries:  []models.ChangeMessageVisibilityBatchRequestEntry{{Id: "1", ReceiptHandle: "h", VisibilityTimeout: models.Ptr(10)}},
		}
		body, _ := json.Marshal(req)
		r := httptest.NewRequest("POST", "/", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		app.ChangeMessageVisibilityBatchHandler(w, r)
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}

func TestHandler_StoreErrors_Comprehensive(t *testing.T) {
	ms := new(MockStore)
	app := &server.App{Store: ms}

	// Helper to create request
	newReq := func(method, bodyStr string) *http.Request {
		return httptest.NewRequest(method, "/", bytes.NewBuffer([]byte(bodyStr)))
	}

	// 1. CreateQueue - Store Error
	t.Run("CreateQueue_StoreError", func(t *testing.T) {
		ms.On("CreateQueue", mock.Anything, "q", mock.Anything, mock.Anything).Return(nil, errors.New("fdb error")).Once()
		req := models.CreateQueueRequest{QueueName: "q"}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.CreateQueueHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 2. DeleteQueue - Store Error
	t.Run("DeleteQueue_StoreError", func(t *testing.T) {
		ms.On("DeleteQueue", mock.Anything, "q").Return(errors.New("fdb error")).Once()
		req := models.DeleteQueueRequest{QueueUrl: "http://h/q"}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.DeleteQueueHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 3. PurgeQueue - Store Error
	t.Run("PurgeQueue_StoreError", func(t *testing.T) {
		ms.On("PurgeQueue", mock.Anything, "q").Return(errors.New("fdb error")).Once()
		req := models.PurgeQueueRequest{QueueUrl: "http://h/q"}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.PurgeQueueHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 4. SetQueueAttributes - Store Error
	t.Run("SetQueueAttributes_StoreError", func(t *testing.T) {
		// Mock DLQ check if RedrivePolicy is present? No, let's just make simple request.
		// Handlers check for RedrivePolicy key. If not present, skips check.
		ms.On("SetQueueAttributes", mock.Anything, "q", mock.Anything).Return(errors.New("fdb error")).Once()
		req := models.SetQueueAttributesRequest{QueueUrl: "http://h/q", Attributes: map[string]string{"VisibilityTimeout": "30"}}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.SetQueueAttributesHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 5. GetQueueAttributes - Store Error
	t.Run("GetQueueAttributes_StoreError", func(t *testing.T) {
		ms.On("GetQueueAttributes", mock.Anything, "q").Return(nil, errors.New("fdb error")).Once()
		req := models.GetQueueAttributesRequest{QueueUrl: "http://h/q"}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.GetQueueAttributesHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 6. ListQueues - Store Error
	t.Run("ListQueues_StoreError", func(t *testing.T) {
		ms.On("ListQueues", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, "", errors.New("fdb error")).Once()
		req := models.ListQueuesRequest{}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.ListQueuesHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 7. AddPermission - Store Error
	t.Run("AddPermission_StoreError", func(t *testing.T) {
		ms.On("AddPermission", mock.Anything, "q", "L", mock.Anything, mock.Anything).Return(errors.New("fdb error")).Once()
		req := models.AddPermissionRequest{QueueUrl: "http://h/q", Label: "L", AWSAccountIds: []string{"123456789012"}, Actions: []string{"*"}}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.AddPermissionHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 8. RemovePermission - Store Error
	t.Run("RemovePermission_StoreError", func(t *testing.T) {
		ms.On("RemovePermission", mock.Anything, "q", "L").Return(errors.New("fdb error")).Once()
		req := models.RemovePermissionRequest{QueueUrl: "http://h/q", Label: "L"}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.RemovePermissionHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 9. TagQueue - Store Error
	t.Run("TagQueue_StoreError", func(t *testing.T) {
		ms.On("TagQueue", mock.Anything, "q", mock.Anything).Return(errors.New("fdb error")).Once()
		req := models.TagQueueRequest{QueueUrl: "http://h/q", Tags: map[string]string{"K": "V"}}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.TagQueueHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 10. UntagQueue - Store Error
	t.Run("UntagQueue_StoreError", func(t *testing.T) {
		ms.On("UntagQueue", mock.Anything, "q", mock.Anything).Return(errors.New("fdb error")).Once()
		req := models.UntagQueueRequest{QueueUrl: "http://h/q", TagKeys: []string{"K"}}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.UntagQueueHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})

	// 11. ListQueueTags - Store Error
	t.Run("ListQueueTags_StoreError", func(t *testing.T) {
		ms.On("ListQueueTags", mock.Anything, "q").Return(nil, errors.New("fdb error")).Once()
		req := models.ListQueueTagsRequest{QueueUrl: "http://h/q"}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		app.ListQueueTagsHandler(w, newReq("POST", string(body)))
		assert.Equal(t, http.StatusInternalServerError, w.Code)
	})
}
