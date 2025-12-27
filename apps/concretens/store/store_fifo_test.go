package store

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestStore_FIFO_Basics(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. Create Topic Validation (Must end in .fifo)
	_, err = s.CreateTopic(ctx, "invalid-fifo", map[string]string{"FifoTopic": "true"})
	if err == nil {
		t.Error("Expected error creating FIFO topic without .fifo suffix")
	}

	// 2. Create Valid FIFO Topic
	topicName := "valid-" + uuid.New().String() + ".fifo"
	topic, err := s.CreateTopic(ctx, topicName, nil)
	if err != nil {
		t.Fatalf("Failed to create FIFO topic: %v", err)
	}
	if !topic.FifoTopic {
		t.Error("Expected FifoTopic to be true")
	}

	// 3. Subscribe Restriction (Only SQS)
	subHTTP := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "http",
		Endpoint: "http://endpoint",
	}
	_, err = s.Subscribe(ctx, subHTTP)
	if err == nil {
		t.Error("Expected error subscribing HTTP to FIFO topic")
	}

	subSQS := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "sqs",
		Endpoint: "sqs://queue",
	}
	_, err = s.Subscribe(ctx, subSQS)
	if err != nil {
		t.Fatalf("Failed to subscribe SQS to FIFO topic: %v", err)
	}
}

func TestStore_FIFO_Deduplication(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	s.ClearQueue(context.Background())
	ctx := context.Background()

	// 1. Setup
	topic, _ := s.CreateTopic(ctx, "dedup-"+uuid.New().String()+".fifo", nil)
	s.Subscribe(ctx, &models.Subscription{TopicArn: topic.TopicArn, Protocol: "sqs", Endpoint: "e"})

	// 2. Publish Message 1 (Group A, Dedup 1)
	msg1 := &models.Message{
		TopicArn:               topic.TopicArn,
		Message:                "body1",
		MessageGroupId:         "groupA",
		MessageDeduplicationId: "dedup1",
	}
	if err := s.PublishMessage(ctx, msg1); err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}

	// 3. Publish Message 2 (Group A, Dedup 1) -> Duplicate
	msg2 := &models.Message{
		TopicArn:               topic.TopicArn,
		Message:                "body1-retry",
		MessageGroupId:         "groupA",
		MessageDeduplicationId: "dedup1",
	}
	if err := s.PublishMessage(ctx, msg2); err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	// 4. Publish Message 3 (Group A, Dedup 2) -> New
	msg3 := &models.Message{
		TopicArn:               topic.TopicArn,
		Message:                "body2",
		MessageGroupId:         "groupA",
		MessageDeduplicationId: "dedup2",
	}
	if err := s.PublishMessage(ctx, msg3); err != nil {
		t.Fatalf("Publish 3 failed: %v", err)
	}

	// 5. Verify Tasks
	tasks, err := s.PollDeliveryTasks(ctx, 100)
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	// Expect 2 tasks (Msg1 and Msg3). Msg2 should have been deduplicated.
	if len(tasks) != 2 {
		t.Fatalf("Expected 2 tasks, got %d", len(tasks))
	}

	// Verify IDs (assuming FIFO order isn't strictly enforced by Poll yet, just dedup)
	// Actually Poll returns in key order (VisibleAfter).
	// Since all published effectively "now", order matches insertion.
	// But duplicate shouldn't exist.

	// Verify content
	m1, _ := s.GetMessage(ctx, topic.TopicArn, tasks[0].MessageID)
	// m2 should correspond to msg3 (second task)
	m2, _ := s.GetMessage(ctx, topic.TopicArn, tasks[1].MessageID)

	// Since MessageID changes on Publish, we can't check ID equality with msg1 struct directly because Publish updates it locally?
	// Wait, PublishMessage(ctx, msg) updates msg.MessageID and .PublishedTime in place (pointer).
	// But msg2 was DUPLICATE logic, so it returned early (nil).
	// So msg2.MessageID might be empty or whatever it was before if it didn't reach ID generation?
	// In my implementation, ID generation happens AFTER dedup check!
	// So msg2.MessageID remains empty (or input value).

	if m1.Message != "body1" {
		t.Errorf("Task 1 content expected body1, got %s", m1.Message)
	}
	if m2.Message != "body2" {
		t.Errorf("Task 2 content expected body2, got %s", m2.Message)
	}
}

func TestStore_FIFO_GroupIdRequired(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()
	topic, _ := s.CreateTopic(ctx, "req-group-"+uuid.New().String()+".fifo", nil)

	msg := &models.Message{
		TopicArn: topic.TopicArn,
		Message:  "fail",
		// Missing GroupId
	}
	err = s.PublishMessage(ctx, msg)
	if err == nil {
		t.Error("Expected error when publishing to FIFO without MessageGroupId")
	}
}
