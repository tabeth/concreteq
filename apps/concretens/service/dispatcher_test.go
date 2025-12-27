package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
)

func TestDispatcher_DeliverHTTP(t *testing.T) {
	// 1. Setup Mock Server
	received := make(chan string, 1) // Changed to string to simplify
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		json.NewDecoder(r.Body).Decode(&payload)

		if payload["Type"] == "SubscriptionConfirmation" {
			// receivedToken = payload["Token"].(string)
		} else if payload["Type"] == "Notification" {
			received <- payload["Message"].(string)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	// 2. Setup Mock Store
	ctx := context.Background()
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{
				SubscriptionArn: subArn,
				TopicArn:        "arn:topic:test",
				Protocol:        "http",
				Endpoint:        mockServer.URL,
				Status:          "Active", // Assume active for this test
			}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return &models.Message{
				TopicArn:  topicArn,
				MessageID: msgID,
				Message:   "hello-world",
			}, nil
		},
		DeleteDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask) error {
			return nil // Success
		},
	}

	// 3. Run Logic
	d := NewDispatcher(mockStore, 1)

	task := &models.DeliveryTask{
		TaskID:          "task-1",
		SubscriptionArn: "arn:sub:1",
		MessageID:       "msg-1",
	}

	d.deliverTask(ctx, task)

	// 4. Verify Delivery
	select {
	case msgBody := <-received:
		if msgBody != "hello-world" {
			t.Errorf("Expected message 'hello-world', got %s", msgBody)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message delivery")
	}
}

func TestDispatcher_DeliverTask_SubNotFound(t *testing.T) {
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return nil, errors.New("sub not found")
		},
	}
	d := NewDispatcher(mockStore, 1)

	task := &models.DeliveryTask{
		TaskID:          "task-1",
		SubscriptionArn: "invalid-sub",
		MessageID:       "msg-id",
	}

	// Should not crash, just log error
	d.deliverTask(context.Background(), task)
}

func TestDispatcher_DeliverTask_MsgNotFound(t *testing.T) {
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{
				SubscriptionArn: subArn,
				TopicArn:        "arn:topic",
				Protocol:        "http",
				Endpoint:        "http://foo",
			}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return nil, errors.New("msg not found")
		},
	}
	d := NewDispatcher(mockStore, 1)

	task := &models.DeliveryTask{
		TaskID:          "task-1",
		SubscriptionArn: "arn:sub:1",
		MessageID:       "missing-msg-id",
	}

	// Should not crash, just log error
	d.deliverTask(context.Background(), task)
}

func TestDispatcher_DeliverHTTP_Failure_Retry(t *testing.T) {
	// Mock server always fails
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockServer.Close()

	rescheduled := false
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{
				SubscriptionArn: subArn,
				TopicArn:        "arn:topic",
				Protocol:        "http",
				Endpoint:        mockServer.URL,
			}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return &models.Message{Message: "foo", MessageID: msgID}, nil
		},
		RescheduleDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
			rescheduled = true
			if task.RetryCount != 0 {
				t.Errorf("Expected RetryCount 0, got %d", task.RetryCount)
			}
			return nil
		},
	}

	d := NewDispatcher(mockStore, 1)
	task := &models.DeliveryTask{TaskID: "t1", RetryCount: 0}

	d.deliverTask(context.Background(), task)

	if !rescheduled {
		t.Error("Task was not rescheduled on failure")
	}
}

func TestDispatcher_UnsupportedProtocol(t *testing.T) {
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{
				SubscriptionArn: subArn,
				TopicArn:        "arn:topic",
				Protocol:        "email", // Unsupported
				Endpoint:        "me@example.com",
			}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return &models.Message{Message: "foo", MessageID: msgID}, nil
		},
		DeleteDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask) error {
			// In implementation, unsupported protocols trigger task deletion? Check dispatcher.go:97
			// Yes, for now it does nothing but log, error is nil.
			// Dispatcher implementation:
			// case default: log...
			// But error `err` is nil by default?
			// wait, if default case is hit, `err` is nil.
			// Then it proceeds to step 4: Delete Task on success.
			// So yes, unsupported task IS deleted.
			return nil
		},
	}
	d := NewDispatcher(mockStore, 1)

	task := &models.DeliveryTask{
		TaskID:          "task-1",
		SubscriptionArn: "sub-1",
		MessageID:       "msg-1",
	}

	d.deliverTask(context.Background(), task)
}

// MockDispatcherStore
type MockDispatcherStore struct {
	PollDeliveryTasksFunc      func(ctx context.Context, limit int) ([]*models.DeliveryTask, error)
	DeleteDeliveryTaskFunc     func(ctx context.Context, task *models.DeliveryTask) error
	GetSubscriptionFunc        func(ctx context.Context, subscriptionArn string) (*models.Subscription, error)
	GetMessageFunc             func(ctx context.Context, topicArn, messageID string) (*models.Message, error)
	RescheduleDeliveryTaskFunc func(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error
	MoveToDLQFunc              func(ctx context.Context, task *models.DeliveryTask) error
}

func (m *MockDispatcherStore) PollDeliveryTasks(ctx context.Context, limit int) ([]*models.DeliveryTask, error) {
	if m.PollDeliveryTasksFunc != nil {
		return m.PollDeliveryTasksFunc(ctx, limit)
	}
	return nil, nil
}
func (m *MockDispatcherStore) DeleteDeliveryTask(ctx context.Context, task *models.DeliveryTask) error {
	if m.DeleteDeliveryTaskFunc != nil {
		return m.DeleteDeliveryTaskFunc(ctx, task)
	}
	return nil
}
func (m *MockDispatcherStore) GetSubscription(ctx context.Context, subscriptionArn string) (*models.Subscription, error) {
	if m.GetSubscriptionFunc != nil {
		return m.GetSubscriptionFunc(ctx, subscriptionArn)
	}
	return nil, nil
}
func (m *MockDispatcherStore) GetMessage(ctx context.Context, topicArn, messageID string) (*models.Message, error) {
	if m.GetMessageFunc != nil {
		return m.GetMessageFunc(ctx, topicArn, messageID)
	}
	return nil, nil
}
func (m *MockDispatcherStore) RescheduleDeliveryTask(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
	if m.RescheduleDeliveryTaskFunc != nil {
		return m.RescheduleDeliveryTaskFunc(ctx, task, nextVisible)
	}
	task.RetryCount++
	task.VisibleAfter = nextVisible
	return nil
}
func (m *MockDispatcherStore) MoveToDLQ(ctx context.Context, task *models.DeliveryTask) error {
	if m.MoveToDLQFunc != nil {
		return m.MoveToDLQFunc(ctx, task)
	}
	return nil
}

func TestDispatcher_PollError(t *testing.T) {
	mockStore := &MockDispatcherStore{
		PollDeliveryTasksFunc: func(ctx context.Context, limit int) ([]*models.DeliveryTask, error) {
			return nil, errors.New("poll error")
		},
	}
	d := NewDispatcher(mockStore, 1)

	// processTasks should handle error (log it) and return
	d.processTasks()
	// Test passes if no panic
}

func TestDispatcher_DeleteTask_Error(t *testing.T) {
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{Protocol: "http", Endpoint: "http://example.com"}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return &models.Message{Message: "foo", MessageID: msgID}, nil
		},
		DeleteDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask) error {
			return errors.New("delete error")
		},
	}
	// Inject a transport that always succeeds to avoid HTTP error
	d := NewDispatcher(mockStore, 1)
	d.httpClient.Transport = &MockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		},
	}

	task := &models.DeliveryTask{TaskID: "t1"}
	// Should log error but not crash
	d.deliverTask(context.Background(), task)
}

func TestDispatcher_DeliverHTTP_NetworkError(t *testing.T) {
	// 1. Mock Transport to return error
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{Protocol: "http", Endpoint: "http://bad-url"}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return &models.Message{Message: "foo", MessageID: msgID}, nil
		},
		RescheduleDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
			return nil // Expected retry
		},
	}
	d := NewDispatcher(mockStore, 1)
	d.httpClient.Transport = &MockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("network error")
		},
	}

	task := &models.DeliveryTask{TaskID: "t1"}
	d.deliverTask(context.Background(), task)
}

func TestDispatcher_DeliverSQS_NetworkError(t *testing.T) {
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{Protocol: "sqs", Endpoint: "http://sqs-url"}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return &models.Message{Message: "foo", MessageID: msgID}, nil
		},
		RescheduleDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
			return nil // Expected retry
		},
	}
	d := NewDispatcher(mockStore, 1)
	d.httpClient.Transport = &MockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("network error")
		},
	}

	task := &models.DeliveryTask{TaskID: "t1"}
	d.deliverTask(context.Background(), task)
}

type MockTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *MockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

func TestDispatcher_DeliverHTTP_Confirmation(t *testing.T) {
	received := make(chan string, 1)
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		json.NewDecoder(r.Body).Decode(&payload)

		if payload["Type"] == "SubscriptionConfirmation" {
			received <- payload["Token"].(string)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{
				TopicArn:          "arn:topic",
				Protocol:          "http",
				Endpoint:          mockServer.URL,
				Status:            "PendingConfirmation",
				ConfirmationToken: "token-123",
			}, nil
		},
		RescheduleDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
			return nil
		},
		DeleteDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask) error {
			return nil
		},
	}
	d := NewDispatcher(mockStore, 1)

	// Special MessageID triggers confirmation logic
	task := &models.DeliveryTask{
		TaskID:    "t1",
		MessageID: "CONFIRMATION_REQUEST",
	}

	d.deliverTask(context.Background(), task)

	select {
	case token := <-received:
		if token != "token-123" {
			t.Errorf("Expected token token-123, got %s", token)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for confirmation")
	}
}

func TestDispatcher_DeliverSQS_Success(t *testing.T) {
	// Mock Transport for SQS success
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{Protocol: "sqs", Endpoint: "http://sqs.url"}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return &models.Message{
				Message:   "sqs-msg",
				MessageID: msgID,
				MessageAttributes: map[string]string{
					"attr1": "val1",
				},
			}, nil
		},
		DeleteDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask) error {
			return nil
		},
	}
	d := NewDispatcher(mockStore, 1)
	d.httpClient.Transport = &MockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			// Verify headers or body if needed
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		},
	}

	task := &models.DeliveryTask{TaskID: "t1", MessageID: "m1"}
	d.deliverTask(context.Background(), task)
}

func TestDispatcher_DeliverSQS_Failure_500(t *testing.T) {
	mockStore := &MockDispatcherStore{
		GetSubscriptionFunc: func(ctx context.Context, subArn string) (*models.Subscription, error) {
			return &models.Subscription{Protocol: "sqs", Endpoint: "http://sqs.url"}, nil
		},
		GetMessageFunc: func(ctx context.Context, topicArn, msgID string) (*models.Message, error) {
			return &models.Message{Message: "sqs-msg", MessageID: msgID}, nil
		},
		RescheduleDeliveryTaskFunc: func(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
			return nil // Expected retry
		},
	}
	d := NewDispatcher(mockStore, 1)
	d.httpClient.Transport = &MockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 500,
				Status:     "500 Internal Server Error",
				Body:       io.NopCloser(bytes.NewBufferString("SQS Error")),
			}, nil
		},
	}

	task := &models.DeliveryTask{TaskID: "t1", MessageID: "m1"}
	d.deliverTask(context.Background(), task)
}
