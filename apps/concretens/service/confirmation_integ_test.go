package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	nsstore "github.com/kiroku-inc/kiroku-core/apps/concretens/store"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestIntegration_SubscriptionConfirmation(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	// 1. Setup Mock Endpoint (Subscriber)
	// -----------------------------------
	confirmTokenChan := make(chan string, 1)
	messageChan := make(chan string, 1)

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("Failed to decode payload: %v", err)
			return
		}

		msgType, _ := payload["Type"].(string)
		if msgType == "SubscriptionConfirmation" {
			token, _ := payload["Token"].(string)
			confirmTokenChan <- token
		} else if msgType == "Notification" {
			msg, _ := payload["Message"].(string)
			messageChan <- msg
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	// 2. Setup Store & Dispatcher
	// ---------------------------
	s, err := nsstore.NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	s.ClearQueue(context.Background())

	d := NewDispatcher(s, 1)
	ctx := context.Background()
	d.Start(ctx)
	defer d.Stop()

	// 3. Create Topic
	// ---------------
	topicName := "confirm-test-" + uuid.New().String()
	topic, err := s.CreateTopic(ctx, topicName, nil)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// 4. Subscribe (Should be Pending)
	// --------------------------------
	sub := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "http",
		Endpoint: mockServer.URL,
	}
	createdSub, err := s.Subscribe(ctx, sub)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if createdSub.Status != "PendingConfirmation" {
		t.Fatalf("Expected PendingConfirmation, got %s", createdSub.Status)
	}
	t.Log("Subscription created with status PendingConfirmation")

	// 5. Wait for Confirmation Message
	// --------------------------------
	var token string
	select {
	case token = <-confirmTokenChan:
		t.Logf("Received confirmation token: %s", token)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for confirmation message")
	}

	// 6. Publish Message (Should NOT be delivered yet)
	// ------------------------------------------------
	msg1 := &models.Message{TopicArn: topic.TopicArn, Message: "ShouldSkip"}
	if err := s.PublishMessage(ctx, msg1); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}

	select {
	case m := <-messageChan:
		t.Fatalf("Received message '%s' but subscription should be pending!", m)
	case <-time.After(2 * time.Second):
		t.Log("Verified no message delivered while pending")
	}

	// 7. Confirm Subscription
	// -----------------------
	confirmedSub, err := s.ConfirmSubscription(ctx, topic.TopicArn, token)
	if err != nil {
		t.Fatalf("ConfirmSubscription failed: %v", err)
	}
	if confirmedSub.Status != "Active" {
		t.Fatalf("Expected Active, got %s", confirmedSub.Status)
	}
	t.Log("Subscription confirmed")

	// 8. Publish Message (Should be delivered now)
	// --------------------------------------------
	msg2 := &models.Message{TopicArn: topic.TopicArn, Message: "ShouldDeliver"}
	if err := s.PublishMessage(ctx, msg2); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	select {
	case m := <-messageChan:
		if m != "ShouldDeliver" {
			t.Errorf("Expected 'ShouldDeliver', got '%s'", m)
		} else {
			t.Log("Success! Message delivered after confirmation.")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message delivery after confirmation")
	}
}
