package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
)

func TestLargeMessage_ChunkingAndReassembly(t *testing.T) {
	s, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	queueName := "large-message-queue"
	_, err := s.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// 1. Create a large message (approx 250KB)
	largeBody := strings.Repeat("a", 250*1024)
	sendReq := &models.SendMessageRequest{
		QueueUrl:    queueName,
		MessageBody: largeBody,
	}

	// 2. Send Message (should chunk)
	sendResp, err := s.SendMessage(ctx, queueName, sendReq)
	require.NoError(t, err)
	assert.NotEmpty(t, sendResp.MessageId)

	// 3. Receive Message (should reassemble)
	receiveReq := &models.ReceiveMessageRequest{
		QueueUrl:            queueName,
		MaxNumberOfMessages: models.Ptr(1),
	}
	receiveResp, err := s.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	require.Len(t, receiveResp.Messages, 1)
	assert.Equal(t, largeBody, receiveResp.Messages[0].Body)

	// 4. Delete Message (should clear all chunks)
	err = s.DeleteMessage(ctx, queueName, receiveResp.Messages[0].ReceiptHandle)
	assert.NoError(t, err)

	// 5. Verify it's gone
	receiveResp2, err := s.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	assert.Empty(t, receiveResp2.Messages)
}

func TestRedriveAllowPolicy_Enforcement(t *testing.T) {
	s, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	dlqName := "test-dlq"
	sourceQueueName := "test-source"

	tests := []struct {
		name           string
		allowPolicy    string
		shouldMove     bool
		sourceArnMatch bool
	}{
		{
			name:        "AllowAll",
			allowPolicy: `{"redrivePermission":"allowAll"}`,
			shouldMove:  true,
		},
		{
			name:        "DenyAll",
			allowPolicy: `{"redrivePermission":"denyAll"}`,
			shouldMove:  false,
		},
		{
			name:        "ByQueue_Allowed",
			allowPolicy: fmt.Sprintf(`{"redrivePermission":"byQueue","sourceQueueArns":["arn:aws:sqs:us-east-1:123456789012:%s"]}`, sourceQueueName),
			shouldMove:  true,
		},
		{
			name:        "ByQueue_Denied",
			allowPolicy: `{"redrivePermission":"byQueue","sourceQueueArns":["arn:aws:sqs:us-east-1:123456789012:other-queue"]}`,
			shouldMove:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			curSource := sourceQueueName + "-" + tt.name
			curDLQ := dlqName + "-" + tt.name
			curDLQArn := fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:%s", curDLQ)
			curRedrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"1"}`, curDLQArn)

			// Fix policy to use curSource if it's ByQueue
			curAllowPolicy := tt.allowPolicy
			if tt.name == "ByQueue_Allowed" {
				curAllowPolicy = fmt.Sprintf(`{"redrivePermission":"byQueue","sourceQueueArns":["arn:aws:sqs:us-east-1:123456789012:%s"]}`, curSource)
			}

			// Create DLQ with specific RedriveAllowPolicy
			attrs := map[string]string{"RedriveAllowPolicy": curAllowPolicy}
			_, err := s.CreateQueue(ctx, curDLQ, attrs, nil)
			require.NoError(t, err)

			// Create Source Queue
			_, err = s.CreateQueue(ctx, curSource, map[string]string{"RedrivePolicy": curRedrivePolicy}, nil)
			require.NoError(t, err)

			// Send message
			_, err = s.SendMessage(ctx, curSource, &models.SendMessageRequest{MessageBody: "test"})
			require.NoError(t, err)

			// 1st receive (Count -> 1)
			resp1, err := s.ReceiveMessage(ctx, curSource, &models.ReceiveMessageRequest{VisibilityTimeout: models.Ptr(1)})
			require.NoError(t, err)
			require.Len(t, resp1.Messages, 1)

			// Wait for visibility timeout
			time.Sleep(1500 * time.Millisecond)

			// 2nd receive (Count -> 2 > Max 1). Should move to DLQ if allowed.
			resp2, err := s.ReceiveMessage(ctx, curSource, &models.ReceiveMessageRequest{})
			require.NoError(t, err)

			if tt.shouldMove {
				assert.Empty(t, resp2.Messages, "Message should have moved to DLQ")
				// Verify in DLQ
				respDLQ, err := s.ReceiveMessage(ctx, curDLQ, &models.ReceiveMessageRequest{})
				require.NoError(t, err, "Failed to receive from DLQ")
				assert.Len(t, respDLQ.Messages, 1, "Message should be found in DLQ")
			} else {
				assert.Len(t, resp2.Messages, 1, "Message should NOT have moved to DLQ and should be received again")
			}
		})
	}
}

func TestStore_Tagging(t *testing.T) {
	s, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	queueName := "tag-test-queue"
	_, err := s.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// 1. Tag Queue
	tags := map[string]string{"Env": "Test", "Dept": "Engineering"}
	err = s.TagQueue(ctx, queueName, tags)
	assert.NoError(t, err)

	// 2. List Tags
	listedTags, err := s.ListQueueTags(ctx, queueName)
	assert.NoError(t, err)
	assert.Equal(t, tags, listedTags)

	// 3. Untag Queue
	err = s.UntagQueue(ctx, queueName, []string{"Dept"})
	assert.NoError(t, err)

	// 4. Verify Untag
	finalTags, err := s.ListQueueTags(ctx, queueName)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"Env": "Test"}, finalTags)
}

func TestStore_MessageMoveTasks(t *testing.T) {
	s, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	sourceArn := "arn:aws:sqs:us-east-1:123456789012:source-q"
	destArn := "arn:aws:sqs:us-east-1:123456789012:dest-q"

	// 1. Start Task
	handle, err := s.StartMessageMoveTask(ctx, sourceArn, destArn)
	assert.NoError(t, err)
	assert.NotEmpty(t, handle)

	// 2. List Tasks
	tasks, err := s.ListMessageMoveTasks(ctx, sourceArn)
	assert.NoError(t, err)
	assert.NotEmpty(t, tasks)
	assert.Equal(t, sourceArn, tasks[0].SourceArn)

	// 3. Cancel Task
	err = s.CancelMessageMoveTask(ctx, handle)
	assert.NoError(t, err)
}

func TestLargeMessage_PurgeQueue(t *testing.T) {
	s, teardown := setupTestDB(t)
	defer teardown()
	ctx := context.Background()

	queueName := "purge-large-queue"
	_, err := s.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	largeBody := strings.Repeat("x", 150*1024)
	_, err = s.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: largeBody})
	require.NoError(t, err)

	// Purge
	err = s.PurgeQueue(ctx, queueName)
	require.NoError(t, err)

	// Verify empty
	resp, err := s.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.Messages)
}
