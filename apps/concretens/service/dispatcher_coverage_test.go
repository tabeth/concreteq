package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
)

func TestDispatcher_DeliveryPolicy_Custom(t *testing.T) {
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, arn string) (*models.Subscription, error) {
			policy := `{"http":{"defaultHealthyRetryPolicy":{"numRetries":3,"minDelayTarget":10}}}`
			return &models.Subscription{
				SubscriptionArn: arn,
				Protocol:        "http",
				Endpoint:        "http://fail.endpoint",
				Attributes:      map[string]string{"DeliveryPolicy": policy},
			}, nil
		},
		GetMessageFunc: func(ctx context.Context, topic, id string) (*models.Message, error) {
			return &models.Message{MessageID: id, TopicArn: topic, Message: "body"}, nil
		},
		RescheduleDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
			// Verify backoff is >= 10s
			delay := nextVisible.Sub(time.Now())
			if delay < 9*time.Second {
				t.Errorf("Expected delay >= 10s, got %v", delay)
			}
			return nil
		},
	}

	d := NewDispatcher(mockStore, 1, "")
	d.httpClient.Transport = &MockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("network error")
		},
	}

	task := &models.DeliveryTask{TaskID: "t1", RetryCount: 0}
	d.deliverTask(context.Background(), task)
}

func TestDispatcher_RedrivePolicy_ExternalDLQ(t *testing.T) {
	deletedTask := false
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, arn string) (*models.Subscription, error) {
			redrive := `{"deadLetterTargetArn":"arn:concreteq:region:acc:MyDLQ"}`
			return &models.Subscription{
				SubscriptionArn: arn,
				Protocol:        "http",
				Endpoint:        "http://fail.endpoint",
				Attributes:      map[string]string{"RedrivePolicy": redrive},
			}, nil
		},
		GetMessageFunc: func(ctx context.Context, topic, id string) (*models.Message, error) {
			return &models.Message{MessageID: id, TopicArn: topic, Message: "body"}, nil
		},
		DeleteDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask) error {
			deletedTask = true
			if task.TaskID != "t1" {
				t.Errorf("Deleted wrong task")
			}
			return nil
		},
	}

	d := NewDispatcher(mockStore, 1, "http://base")

	// Mock HTTP to fail for main endpoint, BUT SUCCEED for DLQ
	d.httpClient.Transport = &MockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			if req.URL.String() == "http://fail.endpoint" {
				return nil, errors.New("fail main")
			}
			// DLQ Endpoint constructed: base + /queue/ + name
			if req.URL.String() == "http://base/queue/MyDLQ" {
				// SQS Respond success
				return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
			}
			t.Errorf("Unexpected request to %s", req.URL.String())
			return nil, errors.New("unexpected")
		},
	}

	// RetryCount = 5 (Max)
	task := &models.DeliveryTask{TaskID: "t1", RetryCount: 5}
	d.deliverTask(context.Background(), task)

	if !deletedTask {
		t.Error("Expected task to be deleted after successful DLQ redrive")
	}
}

func TestDispatcher_RedrivePolicy_InternalDLQ_Fallback(t *testing.T) {
	movedToDLQ := false
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, arn string) (*models.Subscription, error) {
			// No Redrive Policy -> Internal
			return &models.Subscription{
				SubscriptionArn: arn,
				Protocol:        "http",
				Endpoint:        "http://fail.endpoint",
			}, nil
		},
		GetMessageFunc: func(ctx context.Context, topic, id string) (*models.Message, error) {
			return &models.Message{MessageID: id, TopicArn: topic, Message: "body"}, nil
		},
		MoveToDLQFunc: func(ctx context.Context, task *models.DeliveryTask) error {
			movedToDLQ = true
			return nil
		},
	}

	d := NewDispatcher(mockStore, 1, "")
	d.httpClient.Transport = &MockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("fail")
		},
	}

	task := &models.DeliveryTask{TaskID: "t1", RetryCount: 5}
	d.deliverTask(context.Background(), task)

	if !movedToDLQ {
		t.Error("Expected task to be moved to internal DLQ")
	}
}
