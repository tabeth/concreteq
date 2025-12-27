package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestStore_FIFO_StrictOrdering_Serial(t *testing.T) {
	// Verification: FIFO Topics MUST preserve order of receipt.
	// Publish 0, 1, 2... 99 serially.
	// Poll must return 0, 1, 2... 99.
	// Failure condition: If `VisibleAfter` keys collide, order is random (UUID tie-break).

	fdbtest.SkipIfFDBUnavailable(t)

	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()
	topicName := "fifo-serial.fifo"
	topic, err := s.CreateTopic(ctx, topicName, map[string]string{"ContentBasedDeduplication": "true"})
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Create subscription (SQS)
	_, err = s.Subscribe(ctx, &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "sqs",
		Endpoint: "arn:sqs:queue",
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish Serially
	numMessages := 50 // Enough to potential hit timestamp collision if fast
	for i := 0; i < numMessages; i++ {
		msg := &models.Message{
			TopicArn:       topic.TopicArn,
			Message:        fmt.Sprintf("msg-%d", i),
			MessageGroupId: "group-1",
		}
		if err := s.PublishMessage(ctx, msg); err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Poll and Verify
	// We need to poll all of them. `PollDeliveryTasks` returns un-ordered list?
	// NO, `PollDeliveryTasks` relies on `s.subDir` prefix scan, which IS ORDERED by Key (Timestamp).
	// So `tasks` slice IS ordered by key.
	// We check if `tasks` order matches receipt order.

	// Retrieve all
	var allTasks []*models.DeliveryTask
	for len(allTasks) < numMessages {
		tasks, err := s.PollDeliveryTasks(ctx, 100)
		if err != nil {
			t.Fatalf("Poll failed: %v", err)
		}
		if len(tasks) == 0 {
			break
		}
		// Since Poll locks tasks (re-inserts with future timestamp), we might get duplicates if we don't ack?
		// Poll extends visibility. So they disappear from "Visible Now" view.
		// Wait, `PollDeliveryTasks` output order?
		// Iteration over range -> Ordered.

		// For verification, we just collect them.

		// Note: Poll extends visibility by deleting and re-inserting with new timestamp.
		// So subsequent polls won't see them. safe.
		allTasks = append(allTasks, tasks...)
	}

	if len(allTasks) != numMessages {
		t.Fatalf("Expected %d tasks, got %d", numMessages, len(allTasks))
	}

	// Verify Order
	for i, task := range allTasks {
		// We need to fetch the message content to know 'i'.
		// Task contains MessageID only.
		msg, err := s.GetMessage(ctx, topic.TopicArn, task.MessageID)
		if err != nil {
			t.Fatalf("GetMessage failed: %v", err)
		}
		expectedBody := fmt.Sprintf("msg-%d", i)
		if msg.Message != expectedBody {
			t.Errorf("Order Mismatch at index %d: Expected '%s', Got '%s'", i, expectedBody, msg.Message)
		}
	}
}
