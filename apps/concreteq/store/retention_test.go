package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
)

func TestMessageRetention(t *testing.T) {
	ctx := t.Context()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("StandardQueue_MessageExpires", func(t *testing.T) {
		queueName := "retention-std"
		// Set retention to 1 second
		_, err := store.CreateQueue(ctx, queueName, map[string]string{"MessageRetentionPeriod": "1"}, nil)
		require.NoError(t, err)

		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "test"})
		require.NoError(t, err)

		// Wait 2s (Use sufficient buffer for flake resistance)
		time.Sleep(2200 * time.Millisecond)

		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: models.Ptr(1)})
		require.NoError(t, err)
		assert.Empty(t, resp.Messages)
	})

	t.Run("StandardQueue_MessageDoesNotExpire", func(t *testing.T) {
		queueName := "retention-std-ok"
		// Set retention to 10 seconds
		_, err := store.CreateQueue(ctx, queueName, map[string]string{"MessageRetentionPeriod": "10"}, nil)
		require.NoError(t, err)

		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "test-ok"})
		require.NoError(t, err)

		// No wait or short wait
		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: models.Ptr(1)})
		require.NoError(t, err)
		require.Len(t, resp.Messages, 1)
		assert.Equal(t, "test-ok", resp.Messages[0].Body)
	})

	t.Run("FifoQueue_MessageExpires", func(t *testing.T) {
		queueName := "retention-fifo.fifo"
		_, err := store.CreateQueue(ctx, queueName, map[string]string{
			"MessageRetentionPeriod": "1",
			"FifoQueue":              "true",
		}, nil)
		require.NoError(t, err)

		kv := "g1"
		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{
			MessageBody:    "test-fifo",
			MessageGroupId: &kv,
		})
		require.NoError(t, err)

		time.Sleep(2200 * time.Millisecond)

		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: models.Ptr(1)})
		require.NoError(t, err)
		assert.Empty(t, resp.Messages)
	})

	t.Run("FifoQueue_MessageDoesNotExpire", func(t *testing.T) {
		queueName := "retention-fifo-ok.fifo"
		_, err := store.CreateQueue(ctx, queueName, map[string]string{
			"MessageRetentionPeriod": "10",
			"FifoQueue":              "true",
		}, nil)
		require.NoError(t, err)

		kv := "g1"
		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{
			MessageBody:    "test-fifo-ok",
			MessageGroupId: &kv,
		})
		require.NoError(t, err)

		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: models.Ptr(1)})
		require.NoError(t, err)
		require.Len(t, resp.Messages, 1)
		assert.Equal(t, "test-fifo-ok", resp.Messages[0].Body)
	})
}
