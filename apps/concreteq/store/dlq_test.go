package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
)

func TestRedrivePolicy_MoveToDLQ(t *testing.T) {
	s, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	// 1. Create DLQ
	dlqName := "my-dlq"
	_, err := s.CreateQueue(ctx, dlqName, nil, nil)
	require.NoError(t, err)

	// 2. Create Source Queue with RedrivePolicy
	sourceQueueName := "my-source-queue"
	dlqArn := fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:%s", dlqName)
	redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"1"}`, dlqArn)
	attributes := map[string]string{
		"RedrivePolicy": redrivePolicy,
	}
	_, err = s.CreateQueue(ctx, sourceQueueName, attributes, nil)
	require.NoError(t, err)

	// 3. Send Message to Source
	msgBody := "poll-me"
	sendReq := &models.SendMessageRequest{
		QueueUrl:    sourceQueueName,
		MessageBody: msgBody,
	}
	_, err = s.SendMessage(ctx, sourceQueueName, sendReq)
	require.NoError(t, err)

	// 4. Receive Message #1 (Count becomes 1, Max is 1. Should be received.)
	receiveReq := &models.ReceiveMessageRequest{
		QueueUrl:            sourceQueueName,
		MaxNumberOfMessages: models.Ptr(1),
		VisibilityTimeout: models.Ptr(1), // Short timeout to retry quickly
	}
	resp1, err := s.ReceiveMessage(ctx, sourceQueueName, receiveReq)
	require.NoError(t, err)
	require.Len(t, resp1.Messages, 1)
	handle1 := resp1.Messages[0].ReceiptHandle

	// Wait for visibility timeout
	time.Sleep(1500 * time.Millisecond)

	// 5. Receive Message #2 (Count becomes 2 > 1. Should move to DLQ.)
	// The message should NOT be returned here if it moved to DLQ immediately.
	// Or it might return empty?
	// ConcreteQ logic checks `ReceivedCount > maxReceiveCount`.
	// On 1st receive: Count=1. 1 > 1 is False. Returns message.
	// On 2nd receive: Count=2. 2 > 1 is True. Moves to DLQ. continue loop.
	// If it was the only message, ReceiveMessage returns empty (or waits if Long Polling).
	resp2, err := s.ReceiveMessage(ctx, sourceQueueName, receiveReq)
	require.NoError(t, err)
	assert.Empty(t, resp2.Messages, "Message should have moved to DLQ and not be returned from Source")

	// 6. Verify Message is in DLQ
	dlqReceiveReq := &models.ReceiveMessageRequest{
		QueueUrl:            dlqName,
		MaxNumberOfMessages: models.Ptr(1),
	}
	respDLQ, err := s.ReceiveMessage(ctx, dlqName, dlqReceiveReq)
	require.NoError(t, err)
	require.Len(t, respDLQ.Messages, 1)
	assert.Equal(t, msgBody, respDLQ.Messages[0].Body)

	// 7. Verify Message is gone from Source
	// (Already verified by Step 5 returning empty)

	// Clean up handles (though unnecessary for test store)
	_ = handle1
}

func TestListDeadLetterSourceQueues(t *testing.T) {
	s, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	dlqName := "test-dlq"
	_, err := s.CreateQueue(ctx, dlqName, nil, nil)
	require.NoError(t, err)

	source1 := "source-1"
	source2 := "source-2"
	dlqArn := fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:%s", dlqName)
	redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":10}`, dlqArn)
	attrs := map[string]string{"RedrivePolicy": redrivePolicy}

	_, err = s.CreateQueue(ctx, source1, attrs, nil)
	require.NoError(t, err)
	_, err = s.CreateQueue(ctx, source2, attrs, nil)
	require.NoError(t, err)

	// Test List
	sources, _, err := s.ListDeadLetterSourceQueues(ctx, dlqName, 100, "")
	require.NoError(t, err)
	assert.Len(t, sources, 2)
	assert.Contains(t, sources, source1)
	assert.Contains(t, sources, source2)

	// Remove Source2 RedrivePolicy (Update queue)
	// emptyAttrs := map[string]string{"RedrivePolicy": ""}

	// But UpdateDLQIndex logic: checks old vs new.
	// "If there is a new DLQ... if there was an old..."
	// If we set RedrivePolicy to empty string, it means "no DLQ".
	// Need to check how SetQueueAttributes handles overwrite vs merge. Logic says "overwrites all".
	// So passing empty map means NO attributes.
	// But `RedrivePolicy` key missing might mean "no change" or "remove"?
	// SetQueueAttributes overwrites ALL attributes with provided ones.
	// So if we provide a map without RedrivePolicy, it removes it?
	// Let's verify `fdb.go` `SetQueueAttributes`. It gets OLD attributes to compare.
	// `updateDLQIndex` gets old and new map.
	// If new map doesn't have RedrivePolicy, `getDLQName` returns "". So it removes from index. Correct.

	err = s.SetQueueAttributes(ctx, source2, map[string]string{"RedrivePolicy": ""})
	require.NoError(t, err)

	// Test List again
	sources2, _, err := s.ListDeadLetterSourceQueues(ctx, dlqName, 100, "")
	require.NoError(t, err)
	assert.Len(t, sources2, 1)
	assert.Contains(t, sources2, source1)
	assert.NotContains(t, sources2, source2)
}
