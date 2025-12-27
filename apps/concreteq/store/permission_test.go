package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFDBStore_Permissions(t *testing.T) {
	s, err := NewFDBStoreAtPath("test-permissions")
	require.NoError(t, err)

	ctx := context.Background()
	queueName := "perm-queue"

	// Cleanup
	s.DeleteQueue(ctx, queueName)

	t.Run("Permissions on Non-Existent Queue", func(t *testing.T) {
		err := s.AddPermission(ctx, queueName, "label1", []string{"123"}, []string{"*"})
		assert.Equal(t, ErrQueueDoesNotExist, err)

		err = s.RemovePermission(ctx, queueName, "label1")
		assert.Equal(t, ErrQueueDoesNotExist, err)
	})

	// Create Queue
	_, err = s.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)
	defer s.DeleteQueue(ctx, queueName)

	t.Run("AddPermission - Success", func(t *testing.T) {
		err := s.AddPermission(ctx, queueName, "label1", []string{"123"}, []string{"SendMessage"})
		assert.NoError(t, err)

		// Verify policy was created (implicit via re-add or visual inspection if we could)
		// We can't easily get Policy back except via GetQueueAttributes("Policy").
		// But AddPermission logic should persist it.
	})

	t.Run("RemovePermission - Success", func(t *testing.T) {
		err := s.RemovePermission(ctx, queueName, "label1")
		assert.NoError(t, err)
	})

	t.Run("RemovePermission - Label Not Found", func(t *testing.T) {
		err := s.RemovePermission(ctx, queueName, "non-existent-label")
		assert.Equal(t, ErrLabelDoesNotExist, err)
	})
}
