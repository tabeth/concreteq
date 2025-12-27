package store

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestStore_Validation(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	ctx := context.Background()

	// 1. Invalid Topic Names
	invalidNames := []string{"", "bad name", "name/slash", strings.Repeat("a", 257)}
	for _, name := range invalidNames {
		_, err := s.CreateTopic(ctx, name, nil)
		if err == nil {
			t.Errorf("Expected error for invalid topic name '%s'", name)
		}
	}

	// 2. FIFO Suffix Check
	topic, err := s.CreateTopic(ctx, "valid-fifo.fifo", nil)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
	if !topic.FifoTopic {
		t.Error("FIFO topic not marked")
	}

	_, err = s.CreateTopic(ctx, "not-fifo-but-flag", map[string]string{"FifoTopic": "true"})
	if err == nil {
		t.Error("Expected error creating topic with FifoTopic=true but no .fifo suffix")
	}

	// 3. Message Size Limit
	tArn := topic.TopicArn
	bigBody := strings.Repeat("x", 256*1024+1)
	err = s.PublishMessage(ctx, &models.Message{TopicArn: tArn, Message: bigBody, MessageGroupId: "g"})
	if err == nil {
		t.Error("Expected error for message > 256KB")
	}
}

func TestStore_SystemAttributes(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	s.ClearQueue(context.Background())
	ctx := context.Background()

	// Setup Topic
	topic, _ := s.CreateTopic(ctx, "attr-test-"+uuid.New().String(), nil)

	// Add 2 Confirmed Subscriptions (Active)
	// Add 1 Pending Subscription

	// Sub 1: Active
	sub1, _ := s.Subscribe(ctx, &models.Subscription{TopicArn: topic.TopicArn, Protocol: "http", Endpoint: "e1"})
	// Confirm it manually by updating status (hack) or confirm flow
	// Confirm flow needs token.
	s.ConfirmSubscription(ctx, topic.TopicArn, sub1.ConfirmationToken)

	// Sub 2: Active
	sub2, _ := s.Subscribe(ctx, &models.Subscription{TopicArn: topic.TopicArn, Protocol: "http", Endpoint: "e2"})
	s.ConfirmSubscription(ctx, topic.TopicArn, sub2.ConfirmationToken)

	// Sub 3: Pending
	s.Subscribe(ctx, &models.Subscription{TopicArn: topic.TopicArn, Protocol: "http", Endpoint: "e3"})

	// Verify Attributes
	attrs, err := s.GetTopicAttributes(ctx, topic.TopicArn)
	if err != nil {
		t.Fatalf("GetTopicAttributes failed: %v", err)
	}

	if attrs["TopicArn"] != topic.TopicArn {
		t.Error("Missing System Attribute TopicArn")
	}
	if attrs["SubscriptionsConfirmed"] != "2" {
		t.Errorf("Expected 2 Confirmed, got %s", attrs["SubscriptionsConfirmed"])
	}
	if attrs["SubscriptionsPending"] != "1" {
		t.Errorf("Expected 1 Pending, got %s", attrs["SubscriptionsPending"])
	}
}

func TestStore_PublishBatch(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	ctx := context.Background()
	topic, _ := s.CreateTopic(ctx, "batch-"+uuid.New().String(), nil)

	// 1. Success Batch
	req := &models.PublishBatchRequest{
		TopicArn: topic.TopicArn,
		PublishBatchRequestEntries: []models.PublishBatchRequestEntry{
			{Id: "1", Message: "m1"},
			{Id: "2", Message: "m2"},
		},
	}
	resp, err := s.PublishBatch(ctx, req)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}
	if len(resp.Successful) != 2 {
		t.Errorf("Expected 2 successful, got %d", len(resp.Successful))
	}
	if len(resp.Failed) != 0 {
		t.Errorf("Expected 0 failed, got %d", len(resp.Failed))
	}

	// 2. Limit Check
	limitEntries := make([]models.PublishBatchRequestEntry, 11)
	reqLimit := &models.PublishBatchRequest{TopicArn: topic.TopicArn, PublishBatchRequestEntries: limitEntries}
	_, err = s.PublishBatch(ctx, reqLimit)
	if err == nil {
		t.Error("Expected check failure for 11 entries")
	}
}
