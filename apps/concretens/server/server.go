package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
)

type Store interface {
	CreateTopic(ctx context.Context, name string, attributes map[string]string) (*models.Topic, error)
	GetMessage(ctx context.Context, topicArn, messageID string) (*models.Message, error)
	ConfirmSubscription(ctx context.Context, topicArn, token string) (*models.Subscription, error)
	Subscribe(ctx context.Context, sub *models.Subscription) (*models.Subscription, error)
	PublishMessage(ctx context.Context, msg *models.Message) error
	DeleteTopic(ctx context.Context, topicArn string) error
	DeleteSubscription(ctx context.Context, subscriptionArn string) error
	ListTopics(ctx context.Context) ([]*models.Topic, error)
	ListSubscriptions(ctx context.Context) ([]*models.Subscription, error)
	GetTopicAttributes(ctx context.Context, topicArn string) (map[string]string, error)
	SetTopicAttributes(ctx context.Context, topicArn string, attributes map[string]string) error
	GetSubscriptionAttributes(ctx context.Context, subArn string) (map[string]string, error)
	SetSubscriptionAttributes(ctx context.Context, subArn string, attributes map[string]string) error
}

type Server struct {
	store Store
}

func NewServer(store Store) *Server {
	return &Server{store: store}
}

func (s *Server) CreateTopicHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name       string            `json:"Name"`
		Attributes map[string]string `json:"Attributes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	topic, err := s.store.CreateTopic(r.Context(), req.Name, req.Attributes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(topic)
}

func (s *Server) SubscribeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.Subscription
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	sub, err := s.store.Subscribe(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sub)
}

func (s *Server) PublishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	msg := &models.Message{
		TopicArn:               req.TopicArn,
		Message:                req.Message,
		Subject:                req.Subject,
		MessageAttributes:      req.MessageAttributes,
		MessageGroupId:         req.MessageGroupId,
		MessageDeduplicationId: req.MessageDeduplicationId,
	}

	if err := s.store.PublishMessage(r.Context(), msg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := models.PublishResponse{
		MessageId: msg.MessageID,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) DeleteTopicHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TopicArn string `json:"topicArn"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := s.store.DeleteTopic(r.Context(), req.TopicArn); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) DeleteSubscriptionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		SubscriptionArn string `json:"subscriptionArn"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := s.store.DeleteSubscription(r.Context(), req.SubscriptionArn); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) ConfirmSubscriptionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topicArn := r.URL.Query().Get("topicArn")
	token := r.URL.Query().Get("token")

	if topicArn == "" || token == "" {
		http.Error(w, "Missing topicArn or token", http.StatusBadRequest)
		return
	}

	// Call Store
	_, err := s.store.ConfirmSubscription(r.Context(), topicArn, token)
	if err != nil {
		// Log?
		http.Error(w, "Confirmation failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Subscription Confirmed!"))
}

func (s *Server) ListTopicsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topics, err := s.store.ListTopics(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(topics)
}

func (s *Server) ListSubscriptionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	subs, err := s.store.ListSubscriptions(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(subs)
}

func (s *Server) GetTopicAttributesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost { // SNS uses POST for GetTopicAttributes usually? Or GET? AWS uses POST with Action=GetTopicAttributes. Here we simplify.
		// Let's assume POST for consistency with others if we want body, but GET is better for attributes.
		// But if we follow simple REST: GET /topic/attributes?arn=...
		// Let's stick to POST as per existing handlers for now, or match AWS Action style if we were doing that.
		// Existing handlers use POST and body.
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		TopicArn string `json:"topicArn"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	attrs, err := s.store.GetTopicAttributes(r.Context(), req.TopicArn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]map[string]string{"Attributes": attrs})
}

func (s *Server) SetTopicAttributesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		TopicArn       string `json:"topicArn"`
		AttributeName  string `json:"attributeName"` // AWS allows one at a time usually, but our store supports map.
		AttributeValue string `json:"attributeValue"`
		// Or a map?
		Attributes map[string]string `json:"attributes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Support both single (AWS style) and bulk (our style)
	attrs := req.Attributes
	if attrs == nil {
		attrs = make(map[string]string)
	}
	if req.AttributeName != "" {
		attrs[req.AttributeName] = req.AttributeValue
	}

	if err := s.store.SetTopicAttributes(r.Context(), req.TopicArn, attrs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) GetSubscriptionAttributesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		SubscriptionArn string `json:"subscriptionArn"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	attrs, err := s.store.GetSubscriptionAttributes(r.Context(), req.SubscriptionArn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]map[string]string{"Attributes": attrs})
}

func (s *Server) SetSubscriptionAttributesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		SubscriptionArn string            `json:"subscriptionArn"`
		AttributeName   string            `json:"attributeName"`
		AttributeValue  string            `json:"attributeValue"`
		Attributes      map[string]string `json:"attributes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	attrs := req.Attributes
	if attrs == nil {
		attrs = make(map[string]string)
	}
	if req.AttributeName != "" {
		attrs[req.AttributeName] = req.AttributeValue
	}

	if err := s.store.SetSubscriptionAttributes(r.Context(), req.SubscriptionArn, attrs); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
