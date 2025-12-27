package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
)

func TestDispatcher_DeliverSQS(t *testing.T) {
	// 1. Mock ConcreteQ Server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify Headers
		if r.Header.Get("X-Amz-Target") != "AmazonSQS.SendMessage" {
			t.Errorf("Expected X-Amz-Target AmazonSQS.SendMessage, got %s", r.Header.Get("X-Amz-Target"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Header.Get("Content-Type") != "application/x-amz-json-1.0" {
			t.Errorf("Expected Content-Type application/x-amz-json-1.0, got %s", r.Header.Get("Content-Type"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Verify Body
		var payload struct {
			QueueUrl          string
			MessageBody       string
			MessageAttributes map[string]struct {
				DataType    string
				StringValue string
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("Failed to decode body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if payload.MessageBody != "sqs-msg" {
			t.Errorf("Expected MessageBody 'sqs-msg', got %s", payload.MessageBody)
		}
		if payload.MessageAttributes["my-attr"].StringValue != "attr-val" {
			t.Errorf("Expected attr val 'attr-val', got %v", payload.MessageAttributes["my-attr"])
		}

		// Success Response
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"MessageId": "sqs-id"}`))
	}))
	defer ts.Close()

	// 2. Setup Dispatcher
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, arn string) (*models.Subscription, error) {
			return &models.Subscription{
				SubscriptionArn: "sub-sqs",
				TopicArn:        "topic-sqs",
				Protocol:        "sqs",
				Endpoint:        ts.URL + "/queue/my-queue", // Real SQS URL format
			}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, messageID string) (*models.Message, error) {
			return &models.Message{
				TopicArn: "topic-sqs",
				Message:  "sqs-msg",
				MessageAttributes: map[string]string{
					"my-attr": "attr-val",
				},
			}, nil
		},
		DeleteDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask) error {
			return nil
		},
	}

	d := NewDispatcher(mockStore, 1)
	ctx := context.Background()

	task := &models.DeliveryTask{
		TaskID:          "task-1",
		SubscriptionArn: "sub-sqs",
		MessageID:       "msg-1",
	}

	// 3. Manual Invocation of delivering a task (private method handling via Start/Worker is partial)
	// We can test the private deliverTask method via reflection or just trust the worker loop?
	// The problem is `deliverTask` is private.
	// But `Start` creates a goroutine that consumes from `taskQueue`.
	// We can push to `taskQueue` channel if it was exposed? It is not.
	// `Poll` calls `PollDeliveryTasks` from store.

	// Let's modify MockStore to return our task once.
	taskReturned := false
	mockStore.PollDeliveryTasksFunc = func(ctx context.Context, limit int) ([]*models.DeliveryTask, error) {
		if !taskReturned {
			taskReturned = true
			return []*models.DeliveryTask{task}, nil
		}
		return nil, nil // Return empty subsequently
	}

	// Track if delete was called
	deleteCalled := make(chan bool, 1)
	mockStore.DeleteDeliveryTaskFunc = func(ctx context.Context, task *models.DeliveryTask) error {
		deleteCalled <- true
		return nil
	}

	d.Start(ctx)
	defer d.Stop()

	// Wait for success
	select {
	case <-deleteCalled:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for task delivery")
	}
}
