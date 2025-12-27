package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
)

func TestCreateTopicHandler(t *testing.T) {
	mockStore := &MockStore{
		CreateTopicFunc: func(ctx context.Context, name string, attrs map[string]string) (*models.Topic, error) {
			return &models.Topic{
				TopicArn:   "arn:concretens:topic:" + name,
				Name:       name,
				Attributes: attrs,
			}, nil
		},
	}
	s := NewServer(mockStore)

	payload := `{"Name": "test-topic", "Attributes": {"DisplayName": "Test Topic"}}`
	req := httptest.NewRequest("POST", "/createTopic", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()

	s.CreateTopicHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}

	var topic models.Topic
	json.NewDecoder(resp.Body).Decode(&topic)
	if topic.Name != "test-topic" {
		t.Errorf("Expected topic name test-topic, got %s", topic.Name)
	}
}

func TestSubscribeHandler(t *testing.T) {
	mockStore := &MockStore{
		SubscribeFunc: func(ctx context.Context, sub *models.Subscription) (*models.Subscription, error) {
			sub.SubscriptionArn = sub.TopicArn + ":sub-id"
			return sub, nil
		},
	}
	s := NewServer(mockStore)

	payload := `{"TopicArn": "arn:concretens:topic:test-topic", "Protocol": "http", "Endpoint": "http://example.com"}`
	req := httptest.NewRequest("POST", "/subscribe", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()

	s.SubscribeHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}
}

func TestPublishHandler(t *testing.T) {
	mockStore := &MockStore{
		PublishMessageFunc: func(ctx context.Context, msg *models.Message) error {
			msg.MessageID = "msg-id"
			return nil
		},
	}
	s := NewServer(mockStore)

	payload := `{"TopicArn": "arn:concretens:topic:test-topic", "Message": "Hello"}`
	req := httptest.NewRequest("POST", "/publish", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()

	s.PublishHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}
	var pubResp models.PublishResponse
	json.NewDecoder(resp.Body).Decode(&pubResp)
	if pubResp.MessageId == "" {
		t.Error("Expected MessageId in response")
	}
}

func TestDeleteTopicHandler(t *testing.T) {
	mockStore := &MockStore{
		DeleteTopicFunc: func(ctx context.Context, arn string) error {
			return nil
		},
	}
	s := NewServer(mockStore)
	payload := `{"topicArn": "arn:test"}`
	req := httptest.NewRequest("POST", "/deleteTopic", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	s.DeleteTopicHandler(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("Expected OK, got %v", w.Result().Status)
	}
}

func TestDeleteSubscriptionHandler(t *testing.T) {
	mockStore := &MockStore{
		DeleteSubscriptionFunc: func(ctx context.Context, arn string) error {
			return nil
		},
	}
	s := NewServer(mockStore)
	payload := `{"subscriptionArn": "arn:sub"}`
	req := httptest.NewRequest("POST", "/deleteSubscription", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	s.DeleteSubscriptionHandler(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("Expected OK, got %v", w.Result().Status)
	}
}

func TestConfirmSubscriptionHandler(t *testing.T) {
	mockStore := &MockStore{
		ConfirmSubscriptionFunc: func(ctx context.Context, topicArn, token string) (*models.Subscription, error) {
			return &models.Subscription{Status: "Active", TopicArn: topicArn}, nil
		},
	}
	s := NewServer(mockStore)

	// Case 1: Success
	req := httptest.NewRequest("GET", "/confirmSubscription?topicArn=arn:t&token=valid", nil)
	w := httptest.NewRecorder()
	s.ConfirmSubscriptionHandler(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("Expected OK, got %v", w.Result().Status)
	}

	// Case 2: Missing Params
	req = httptest.NewRequest("GET", "/confirmSubscription", nil)
	w = httptest.NewRecorder()
	s.ConfirmSubscriptionHandler(w, req)
	if w.Result().StatusCode != http.StatusBadRequest {
		t.Errorf("Expected Bad Request for missing params")
	}

	// Case 3: Store Error
	errStore := &MockStore{
		ConfirmSubscriptionFunc: func(ctx context.Context, topicArn, token string) (*models.Subscription, error) {
			return nil, context.DeadlineExceeded
		},
	}
	sErr := NewServer(errStore)
	req = httptest.NewRequest("GET", "/confirmSubscription?topicArn=arn:t&token=invalid", nil)
	w = httptest.NewRecorder()
	sErr.ConfirmSubscriptionHandler(w, req)
	if w.Result().StatusCode != http.StatusBadRequest { // Handler returns 400 on error currently
		t.Errorf("Expected Bad Request on error, got %v", w.Result().StatusCode)
	}
}

func TestListTopicsHandler(t *testing.T) {
	mockStore := &MockStore{
		ListTopicsFunc: func(ctx context.Context) ([]*models.Topic, error) {
			return []*models.Topic{{Name: "t1"}}, nil
		},
	}
	s := NewServer(mockStore)
	req := httptest.NewRequest("GET", "/listTopics", nil)
	w := httptest.NewRecorder()
	s.ListTopicsHandler(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("Expected OK, got %v", w.Result().Status)
	}
}

func TestListSubscriptionsHandler(t *testing.T) {
	mockStore := &MockStore{
		ListSubscriptionsFunc: func(ctx context.Context) ([]*models.Subscription, error) {
			return []*models.Subscription{{Endpoint: "e1"}}, nil
		},
	}
	s := NewServer(mockStore)
	req := httptest.NewRequest("GET", "/listSubscriptions", nil)
	w := httptest.NewRecorder()
	s.ListSubscriptionsHandler(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Errorf("Expected OK, got %v", w.Result().Status)
	}
}

func TestHandlers_DeepCoverage(t *testing.T) {
	// 1. Method Not Allowed
	s := NewServer(&MockStore{})
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	endpoints := []struct {
		Path    string
		Method  string
		Handler func(http.ResponseWriter, *http.Request)
	}{
		{"/createTopic", "POST", s.CreateTopicHandler},
		{"/subscribe", "POST", s.SubscribeHandler},
		{"/publish", "POST", s.PublishHandler},
		{"/deleteTopic", "POST", s.DeleteTopicHandler},
		{"/deleteSubscription", "POST", s.DeleteSubscriptionHandler},
		{"/listTopics", "GET", s.ListTopicsHandler},
		{"/listSubscriptions", "GET", s.ListSubscriptionsHandler},
		{"/confirmSubscription", "GET", s.ConfirmSubscriptionHandler},
	}

	for _, ep := range endpoints {
		for _, m := range methods {
			if m == ep.Method {
				continue
			}
			req := httptest.NewRequest(m, ep.Path, nil)
			w := httptest.NewRecorder()
			ep.Handler(w, req)
			if w.Result().StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("%s %s expected 405, got %d", m, ep.Path, w.Result().StatusCode)
			}
		}
	}

	// 2. Bad Request (Invalid JSON)
	postEndpoints := []struct {
		Path    string
		Handler func(http.ResponseWriter, *http.Request)
	}{
		{"/createTopic", s.CreateTopicHandler},
		{"/subscribe", s.SubscribeHandler},
		{"/publish", s.PublishHandler},
		{"/deleteTopic", s.DeleteTopicHandler},
		{"/deleteSubscription", s.DeleteSubscriptionHandler},
	}

	for _, ep := range postEndpoints {
		req := httptest.NewRequest("POST", ep.Path, bytes.NewBufferString("{invalidjson"))
		w := httptest.NewRecorder()
		ep.Handler(w, req)
		if w.Result().StatusCode != http.StatusBadRequest {
			t.Errorf("Invalid JSON to %s expected 400, got %d", ep.Path, w.Result().StatusCode)
		}
	}

	// 3. Store Errors
	errStore := &MockStore{
		CreateTopicFunc: func(ctx context.Context, name string, attrs map[string]string) (*models.Topic, error) {
			return nil, context.DeadlineExceeded
		},
		SubscribeFunc: func(ctx context.Context, sub *models.Subscription) (*models.Subscription, error) {
			return nil, context.DeadlineExceeded
		},
		PublishMessageFunc:     func(ctx context.Context, msg *models.Message) error { return context.DeadlineExceeded },
		DeleteTopicFunc:        func(ctx context.Context, arn string) error { return context.DeadlineExceeded },
		DeleteSubscriptionFunc: func(ctx context.Context, arn string) error { return context.DeadlineExceeded },
		ListTopicsFunc:         func(ctx context.Context) ([]*models.Topic, error) { return nil, context.DeadlineExceeded },
		ListSubscriptionsFunc:  func(ctx context.Context) ([]*models.Subscription, error) { return nil, context.DeadlineExceeded },
	}
	sErr := NewServer(errStore)

	validJSON := map[string]string{
		"/createTopic":        `{"Name": "t"}`,
		"/subscribe":          `{"TopicArn": "a"}`,
		"/publish":            `{"TopicArn": "a"}`,
		"/deleteTopic":        `{"topicArn": "a"}`,
		"/deleteSubscription": `{"subscriptionArn": "a"}`,
	}

	for path, jsonStr := range validJSON {
		req := httptest.NewRequest("POST", path, bytes.NewBufferString(jsonStr))
		w := httptest.NewRecorder()
		if path == "/createTopic" {
			sErr.CreateTopicHandler(w, req)
		}
		if path == "/subscribe" {
			sErr.SubscribeHandler(w, req)
		}
		if path == "/publish" {
			sErr.PublishHandler(w, req)
		}
		if path == "/deleteTopic" {
			sErr.DeleteTopicHandler(w, req)
		}
		if path == "/deleteSubscription" {
			sErr.DeleteSubscriptionHandler(w, req)
		}

		if w.Result().StatusCode != http.StatusInternalServerError {
			t.Errorf("Store error on %s expected 500, got %d", path, w.Result().StatusCode)
		}
	}

	// List errors
	req := httptest.NewRequest("GET", "/listTopics", nil)
	w := httptest.NewRecorder()
	sErr.ListTopicsHandler(w, req)
	if w.Result().StatusCode != http.StatusInternalServerError {
		t.Errorf("Store error on /listTopics expected 500")
	}

	req = httptest.NewRequest("GET", "/listSubscriptions", nil)
	w = httptest.NewRecorder()
	sErr.ListSubscriptionsHandler(w, req)
	if w.Result().StatusCode != http.StatusInternalServerError {
		t.Errorf("Store error on /listSubscriptions expected 500")
	}
}

// MockStore
type MockStore struct {
	CreateTopicFunc         func(ctx context.Context, name string, attrs map[string]string) (*models.Topic, error)
	GetTopicFunc            func(ctx context.Context, topicArn string) (*models.Topic, error)
	GetMessageFunc          func(ctx context.Context, topicArn, messageID string) (*models.Message, error)
	SubscribeFunc           func(ctx context.Context, sub *models.Subscription) (*models.Subscription, error)
	PublishMessageFunc      func(ctx context.Context, msg *models.Message) error
	DeleteTopicFunc         func(ctx context.Context, arn string) error
	DeleteSubscriptionFunc  func(ctx context.Context, arn string) error
	ListTopicsFunc          func(ctx context.Context) ([]*models.Topic, error)
	ListSubscriptionsFunc   func(ctx context.Context) ([]*models.Subscription, error)
	ConfirmSubscriptionFunc func(ctx context.Context, topicArn, token string) (*models.Subscription, error)
}

func (m *MockStore) ConfirmSubscription(ctx context.Context, topicArn, token string) (*models.Subscription, error) {
	if m.ConfirmSubscriptionFunc != nil {
		return m.ConfirmSubscriptionFunc(ctx, topicArn, token)
	}
	return nil, nil
}
func (m *MockStore) CreateTopic(ctx context.Context, name string, attrs map[string]string) (*models.Topic, error) {
	if m.CreateTopicFunc != nil {
		return m.CreateTopicFunc(ctx, name, attrs)
	}
	return nil, nil
}
func (m *MockStore) GetTopic(ctx context.Context, topicArn string) (*models.Topic, error) {
	if m.GetTopicFunc != nil {
		return m.GetTopicFunc(ctx, topicArn)
	}
	return nil, nil
}
func (m *MockStore) Subscribe(ctx context.Context, sub *models.Subscription) (*models.Subscription, error) {
	if m.SubscribeFunc != nil {
		return m.SubscribeFunc(ctx, sub)
	}
	return nil, nil
}
func (m *MockStore) PublishMessage(ctx context.Context, msg *models.Message) error {
	if m.PublishMessageFunc != nil {
		return m.PublishMessageFunc(ctx, msg)
	}
	return nil
}
func (m *MockStore) GetMessage(ctx context.Context, topicArn, messageID string) (*models.Message, error) {
	if m.GetMessageFunc != nil {
		return m.GetMessageFunc(ctx, topicArn, messageID)
	}
	return nil, nil
}
func (m *MockStore) DeleteTopic(ctx context.Context, arn string) error {
	if m.DeleteTopicFunc != nil {
		return m.DeleteTopicFunc(ctx, arn)
	}
	return nil
}
func (m *MockStore) DeleteSubscription(ctx context.Context, arn string) error {
	if m.DeleteSubscriptionFunc != nil {
		return m.DeleteSubscriptionFunc(ctx, arn)
	}
	return nil
}
func (m *MockStore) ListTopics(ctx context.Context) ([]*models.Topic, error) {
	if m.ListTopicsFunc != nil {
		return m.ListTopicsFunc(ctx)
	}
	return nil, nil
}
func (m *MockStore) ListSubscriptions(ctx context.Context) ([]*models.Subscription, error) {
	if m.ListSubscriptionsFunc != nil {
		return m.ListSubscriptionsFunc(ctx)
	}
	return nil, nil
}
