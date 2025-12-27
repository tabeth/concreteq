package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestStore_FilterPolicyScope_MessageBody(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	ctx := context.Background()

	// 1. Create Topic
	if err := s.ClearQueue(ctx); err != nil {
		t.Fatalf("Failed to clear queue: %v", err)
	}
	topicName := "filter-body-topic-" + uuid.New().String()
	topic, _ := s.CreateTopic(ctx, topicName, nil)

	// 2. Create Subscribers
	// Sub A: Matches "category": "sports" in BODY
	subA := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "sqs",
		Endpoint: "sub-a",
		Attributes: map[string]string{
			"FilterPolicyScope": "MessageBody",
		},
		FilterPolicy: `{"category": ["sports"]}`,
	}
	subA, _ = s.Subscribe(ctx, subA)

	// Sub B: Matches "category": "news" in BODY
	subB := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "sqs",
		Endpoint: "sub-b",
		Attributes: map[string]string{
			"FilterPolicyScope": "MessageBody",
		},
		FilterPolicy: `{"category": ["news"]}`,
	}
	subB, _ = s.Subscribe(ctx, subB)

	// Sub C: Default Scope (Attributes) - Control Group
	subC := &models.Subscription{
		TopicArn:     topic.TopicArn,
		Protocol:     "sqs",
		Endpoint:     "sub-c",
		FilterPolicy: `{"sender": ["myself"]}`,
	}
	subC, _ = s.Subscribe(ctx, subC)

	// 3. Publish Message (Body has category=sports, Attributes has sender=myself)
	msg := &models.Message{
		TopicArn: topic.TopicArn,
		Message:  `{"category": "sports", "details": {"id": 123}}`,
		MessageAttributes: map[string]string{
			"sender": "myself",
		},
	}
	if err := s.PublishMessage(ctx, msg); err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	// 4. Verify Delivery
	// Since PollDeliveryTasks claims all visible tasks, verify all subscriptions in one go to avoid locking issues.
	deadline := time.Now().Add(5 * time.Second)
	foundMap := make(map[string]bool)
	expectedSubs := []string{subA.SubscriptionArn, subC.SubscriptionArn}
	// SubB should NOT be found

	for time.Now().Before(deadline) {
		tasks, _ := s.PollDeliveryTasks(ctx, 100)
		for _, task := range tasks {
			if task.MessageID == msg.MessageID {
				foundMap[task.SubscriptionArn] = true
			}
		}

		// Check if we have all expected
		allFound := true
		for _, sub := range expectedSubs {
			if !foundMap[sub] {
				allFound = false
				break
			}
		}
		if allFound {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Assertions
	if !foundMap[subA.SubscriptionArn] {
		t.Errorf("Subscription %s (SubA): expected delivery true, got false", subA.SubscriptionArn)
	}
	if foundMap[subB.SubscriptionArn] {
		t.Errorf("Subscription %s (SubB): expected delivery false, got true", subB.SubscriptionArn)
	}
	if !foundMap[subC.SubscriptionArn] {
		t.Errorf("Subscription %s (SubC): expected delivery true, got false", subC.SubscriptionArn)
	}
}

func TestStore_FilterPolicyScope_InvalidBody(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	topic, _ := s.CreateTopic(ctx, "filter-invalid-body", nil)

	sub := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "sqs",
		Endpoint: "sub-req-body",
		Attributes: map[string]string{
			"FilterPolicyScope": "MessageBody",
		},
		FilterPolicy: `{"foo": ["bar"]}`,
	}
	sub, _ = s.Subscribe(ctx, sub)

	// Publish non-JSON body
	msg := &models.Message{
		TopicArn: topic.TopicArn,
		Message:  "not-json", // Should fail parsing and SKIP delivery
	}
	s.PublishMessage(ctx, msg)

	tasks, _ := s.PollDeliveryTasks(ctx, 10)
	if len(tasks) > 0 {
		t.Error("Should have skipped delivery for invalid JSON body")
	}
}
