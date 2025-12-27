package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
)

func TestFDBStore_Coverage(t *testing.T) {
	ctx := t.Context()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "coverage-queue"
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	t.Run("Permissions", func(t *testing.T) {
		err := store.AddPermission(ctx, queueName, "MyLabel", []string{"123456789012"}, []string{"SendMessage"})
		assert.NoError(t, err)

		err = store.RemovePermission(ctx, queueName, "MyLabel")
		assert.NoError(t, err)

		// Test Remove non-existent
		err = store.RemovePermission(ctx, queueName, "NonExistentLabel")
		assert.ErrorIs(t, err, ErrLabelDoesNotExist)
	})

	t.Run("Tags", func(t *testing.T) {
		tags := map[string]string{"Env": "Test", "Owner": "Me"}
		err := store.TagQueue(ctx, queueName, tags)
		assert.NoError(t, err)

		listedTags, err := store.ListQueueTags(ctx, queueName)
		assert.NoError(t, err)
		assert.Equal(t, tags, listedTags)

		err = store.UntagQueue(ctx, queueName, []string{"Env"})
		assert.NoError(t, err)

		listedTags2, err := store.ListQueueTags(ctx, queueName)
		assert.NoError(t, err)
		assert.Equal(t, map[string]string{"Owner": "Me"}, listedTags2)
	})

	t.Run("QueueUrl_CaseSensitivity", func(t *testing.T) {
		// Just to cover GetQueueURL logic fully if needed
		url, err := store.GetQueueURL(ctx, queueName)
		assert.NoError(t, err)
		assert.Contains(t, url, queueName)
	})

	t.Run("DeleteMessage_Empty", func(t *testing.T) {
		err := store.DeleteMessage(ctx, queueName, "receipt-handle-does-not-exist")
		// Should return nil (success) if handle not found/expired per SQS spec
		assert.NoError(t, err)
	})

	t.Run("MessageMoveTask_Real", func(t *testing.T) {
		srcQueue := "move-src"
		dstQueue := "move-dst"
		_, err := store.CreateQueue(ctx, srcQueue, nil, nil)
		require.NoError(t, err)
		_, err = store.CreateQueue(ctx, dstQueue, nil, nil)
		require.NoError(t, err)

		// Send message to src
		_, err = store.SendMessage(ctx, srcQueue, &models.SendMessageRequest{MessageBody: "moving-msg"})
		require.NoError(t, err)

		srcArn := "arn:aws:sqs:us-east-1:123456789012:" + srcQueue
		handle, err := store.StartMessageMoveTask(ctx, srcArn, "arn:aws:sqs:us-east-1:123456789012:"+dstQueue)
		require.NoError(t, err)

		// Wait for move to complete
		require.Eventually(t, func() bool {
			resp, err := store.ReceiveMessage(ctx, dstQueue, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			return err == nil && len(resp.Messages) > 0
		}, 6*time.Second, 200*time.Millisecond)

		// Cancel check
		err = store.CancelMessageMoveTask(ctx, handle)
		require.NoError(t, err)

		// List check
		tasks, err := store.ListMessageMoveTasks(ctx, srcArn)
		require.NoError(t, err)
		assert.NotEmpty(t, tasks)
	})
}
