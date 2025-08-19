package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
)

// NOTE: These tests require a running FoundationDB instance.
// They are integration tests, not unit tests.

func setupTestDB(t *testing.T) (*FDBStore, func()) {
	t.Helper()
	fdb.MustAPIVersion(730)
	db, err := fdb.OpenDefault()
	if err != nil {
		t.Logf("FoundationDB integration tests skipped: could not open default FDB database: %v", err)
		t.Skip("skipping FoundationDB tests: could not open default FDB database")
	}

	// Pre-test condition to check if the cluster is up with a short timeout.
	t.Log("Checking FoundationDB cluster availability...")
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Skipf("skipping FoundationDB tests: could not create transaction: %v", err)
	}
	// Set a 1-second timeout for the check to avoid long waits.
	err = tr.Options().SetTimeout(1000)
	if err != nil {
		t.Skipf("skipping FoundationDB tests: could not set transaction timeout: %v", err)
	}

	_, err = tr.Get(fdb.Key("\xff\xff/status/json")).Get()
	if err != nil {
		t.Logf("FoundationDB integration tests skipped: could not connect to cluster: %v. Please ensure FoundationDB is running.", err)
		t.Skip("Please ensure FoundationDB is running and accessible.")
	}
	t.Log("FoundationDB cluster is available. Proceeding with tests.")

	// Clean up the ConcreteQ directory before running tests
	dir, err := directory.CreateOrOpen(db, []string{"concreteq"}, nil)
	require.NoError(t, err)

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		subdirs, err := dir.List(tr, []string{})
		if err != nil {
			return nil, err
		}
		for _, subdir := range subdirs {
			_, err := dir.Remove(tr, []string{subdir})
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)

	store, err := NewFDBStore()
	require.NoError(t, err)

	teardown := func() {
		// Teardown logic can be added here if needed, but for now,
		// we are cleaning at the start of the test run.
	}

	return store, teardown
}

func TestFDBStore_CreateQueue(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("creates a new queue successfully", func(t *testing.T) {
		queueName := "test-queue-1"
		err := store.CreateQueue(ctx, queueName, nil, nil)
		assert.NoError(t, err)

		// Verify the directory was created
		exists, err := directory.Exists(store.db, []string{"concreteq", queueName})
		assert.NoError(t, err)
		assert.True(t, exists, "expected queue directory to exist")
	})

	t.Run("returns error if queue already exists", func(t *testing.T) {
		queueName := "test-queue-2"
		// Create it once
		err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// Try to create it again
		err = store.CreateQueue(ctx, queueName, nil, nil)
		assert.ErrorIs(t, err, ErrQueueAlreadyExists)
	})

	t.Run("stores attributes and tags correctly", func(t *testing.T) {
		queueName := "test-queue-3"
		attributes := map[string]string{"VisibilityTimeout": "60"}
		tags := map[string]string{"Project": "concreteq"}

		err := store.CreateQueue(ctx, queueName, attributes, tags)
		require.NoError(t, err)

		// Verify data directly in FDB
		_, err = store.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			queueDir, err := directory.Open(rtr, []string{"concreteq", queueName}, nil)
			require.NoError(t, err)

			// Check attributes
			attrsBytes, err := rtr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
			require.NoError(t, err)
			var storedAttrs map[string]string
			err = json.Unmarshal(attrsBytes, &storedAttrs)
			require.NoError(t, err)
			assert.Equal(t, attributes, storedAttrs)

			// Check tags
			tagsBytes, err := rtr.Get(queueDir.Pack(tuple.Tuple{"tags"})).Get()
			require.NoError(t, err)
			var storedTags map[string]string
			err = json.Unmarshal(tagsBytes, &storedTags)
			require.NoError(t, err)
			assert.Equal(t, tags, storedTags)

			return nil, nil
		})
		require.NoError(t, err)
	})
}

func TestFDBStore_ListQueues(t *testing.T) {
	store, teardown := setupTestDB(t)
	defer teardown()

	// --- Test Data ---
	// Create a set of test queues
	numTestQueues := 5
	testQueueNames := make([]string, numTestQueues)
	for i := 0; i < numTestQueues; i++ {
		testQueueNames[i] = fmt.Sprintf("test-queue-%d", i)
	}

	prefixQueueNames := []string{"prefix-a", "prefix-b"}
	allTestQueues := append(testQueueNames, prefixQueueNames...)

	// Create the queues in FDB
	for _, name := range allTestQueues {
		err := store.CreateQueue(context.Background(), name, nil, nil)
		// We ignore "already exists" error to make tests idempotent
		if err != nil && err != ErrQueueAlreadyExists {
			t.Fatalf("Failed to create test queue %s: %v", name, err)
		}
	}
	sort.Strings(allTestQueues)

	// --- Test Cases ---
	t.Run("List all queues", func(t *testing.T) {
		queues, nextToken, err := store.ListQueues(context.Background(), 0, "", "")
		assert.NoError(t, err)
		assert.Empty(t, nextToken)

		// We can't be sure about other queues in the DB, so we just check
		// that our test queues are present.
		queueSet := make(map[string]bool)
		for _, q := range queues {
			queueSet[q] = true
		}
		for _, name := range allTestQueues {
			assert.True(t, queueSet[name], "expected queue %s to be in the list", name)
		}
	})

	t.Run("List with MaxResults", func(t *testing.T) {
		queues, nextToken, err := store.ListQueues(context.Background(), 2, "", "")
		assert.NoError(t, err)
		assert.NotEmpty(t, nextToken)
		assert.Len(t, queues, 2)
	})

	t.Run("List with QueueNamePrefix", func(t *testing.T) {
		queues, nextToken, err := store.ListQueues(context.Background(), 0, "", "prefix-")
		assert.NoError(t, err)
		assert.Empty(t, nextToken)
		assert.Len(t, queues, 2)
		sort.Strings(queues)
		assert.Equal(t, []string{"prefix-a", "prefix-b"}, queues)
	})

	t.Run("Pagination", func(t *testing.T) {
		// Get the first page
		queues1, nextToken1, err := store.ListQueues(context.Background(), 3, "", "")
		assert.NoError(t, err)
		assert.NotEmpty(t, nextToken1)
		assert.Len(t, queues1, 3)

		// Get the second page
		queues2, _, err := store.ListQueues(context.Background(), 10, nextToken1, "")
		assert.NoError(t, err)

		// Check that the second page contains the rest of the queues
		// and there is no overlap with the first page.
		assert.NotContains(t, queues2, queues1[0])
		assert.NotContains(t, queues2, queues1[1])
		assert.NotContains(t, queues2, queues1[2])

		// Check that all queues are returned across all pages
		allListedQueues := append(queues1, queues2...)
		queueSet := make(map[string]bool)
		for _, q := range allListedQueues {
			queueSet[q] = true
		}
		for _, name := range allTestQueues {
			assert.True(t, queueSet[name], "expected queue %s to be in the list", name)
		}
	})
}

func TestFDBStore_DeleteQueue(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("deletes an existing queue", func(t *testing.T) {
		queueName := "queue-to-delete"
		// Create the queue first
		err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// Now delete it
		err = store.DeleteQueue(ctx, queueName)
		assert.NoError(t, err)

		// Verify it's gone
		exists, err := directory.Exists(store.db, []string{"concreteq", queueName})
		assert.NoError(t, err)
		assert.False(t, exists, "expected queue directory to be deleted")
	})

	t.Run("returns error for non-existent queue", func(t *testing.T) {
		queueName := "non-existent-queue"
		err := store.DeleteQueue(ctx, queueName)
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})
}

func TestFDBStore_SendMessageBatch(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-batch-queue"
	err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	t.Run("sends a batch successfully to a standard queue", func(t *testing.T) {
		batchRequest := &models.SendMessageBatchRequest{
			QueueUrl: "http://localhost/" + queueName,
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "msg1", MessageBody: "batch message 1"},
				{Id: "msg2", MessageBody: "batch message 2"},
			},
		}

		resp, err := store.SendMessageBatch(ctx, queueName, batchRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp)

		assert.Len(t, resp.Successful, 2)
		assert.Len(t, resp.Failed, 0)

		// Verify messages in DB
		for _, success := range resp.Successful {
			msg, err := getMessageFromDB(store, queueName, success.MessageId)
			require.NoError(t, err)
			if success.Id == "msg1" {
				assert.Equal(t, "batch message 1", msg.Body)
			} else {
				assert.Equal(t, "batch message 2", msg.Body)
			}
		}
	})

	t.Run("sends a batch to a fifo queue with deduplication", func(t *testing.T) {
		fifoQueueName := "test-batch-fifo.fifo"
		err := store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		dedupID := "batch-dedup-1"
		gID := "batch-group-1"

		batchRequest := &models.SendMessageBatchRequest{
			QueueUrl: "http://localhost/" + fifoQueueName,
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "fifo1", MessageBody: "fifo batch 1", MessageGroupId: &gID, MessageDeduplicationId: &dedupID},
				{Id: "fifo2", MessageBody: "fifo batch 2", MessageGroupId: &gID},
			},
		}

		// First send
		resp1, err := store.SendMessageBatch(ctx, fifoQueueName, batchRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp1)
		assert.Len(t, resp1.Successful, 2)
		assert.Len(t, resp1.Failed, 0)
		originalMsg1ID := resp1.Successful[0].MessageId

		// Second send with one duplicate entry
		batchRequest2 := &models.SendMessageBatchRequest{
			QueueUrl: "http://localhost/" + fifoQueueName,
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "fifo1", MessageBody: "fifo batch 1", MessageGroupId: &gID, MessageDeduplicationId: &dedupID}, // This one is a duplicate
				{Id: "fifo3", MessageBody: "fifo batch 3", MessageGroupId: &gID},
			},
		}

		resp2, err := store.SendMessageBatch(ctx, fifoQueueName, batchRequest2)
		assert.NoError(t, err)
		require.NotNil(t, resp2)
		assert.Len(t, resp2.Successful, 2)
		assert.Len(t, resp2.Failed, 0)

		// Check that the first message was deduplicated (same message ID)
		assert.Equal(t, originalMsg1ID, resp2.Successful[0].MessageId)
		// Check that the second message is new
		assert.NotEqual(t, resp1.Successful[1].MessageId, resp2.Successful[1].MessageId)
	})

	t.Run("returns error if queue does not exist", func(t *testing.T) {
		batchRequest := &models.SendMessageBatchRequest{
			QueueUrl: "http://localhost/non-existent",
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "msg1", MessageBody: "wont be sent"},
			},
		}
		_, err := store.SendMessageBatch(ctx, "non-existent", batchRequest)
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})

	t.Run("handles partial failures within a batch", func(t *testing.T) {
		gID := "batch-group-2"
		batchRequest := &models.SendMessageBatchRequest{
			QueueUrl: "http://localhost/" + queueName,
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "valid1", MessageBody: "this one is fine"},
				{Id: "invalid1", MessageBody: "this one has delay on fifo", DelaySeconds: new(int32), MessageGroupId: &gID},
				{Id: "valid2", MessageBody: "this one is also fine"},
				{Id: "invalid2", MessageBody: "this one is missing group id", MessageDeduplicationId: new(string)},
			},
		}

		// Use a FIFO queue to test FIFO-specific validation
		fifoQueueName := "test-batch-partial-fail.fifo"
		err := store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		// Set group IDs for valid entries
		batchRequest.Entries[0].MessageGroupId = &gID
		batchRequest.Entries[2].MessageGroupId = &gID

		resp, err := store.SendMessageBatch(ctx, fifoQueueName, batchRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp)

		assert.Len(t, resp.Successful, 2)
		assert.Len(t, resp.Failed, 2)

		// Verify the successful messages are in the DB
		_, err = getMessageFromDB(store, fifoQueueName, resp.Successful[0].MessageId)
		assert.NoError(t, err)
		_, err = getMessageFromDB(store, fifoQueueName, resp.Successful[1].MessageId)
		assert.NoError(t, err)

		// Verify the failed messages have the correct error codes
		for _, f := range resp.Failed {
			if f.Id == "invalid1" {
				assert.Equal(t, "InvalidParameterValue", f.Code)
			}
			if f.Id == "invalid2" {
				assert.Equal(t, "MissingParameter", f.Code)
			}
		}
	})
}

func TestFDBStore_FifoQueue_Fairness(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-fifo-fairness.fifo"
	err := store.CreateQueue(ctx, queueName, map[string]string{"FifoQueue": "true"}, nil)
	require.NoError(t, err)

	// Send many messages to group-a (the noisy neighbor)
	// and one message to two other groups.
	noisyGroup := "group-a"
	quietGroupB := "group-b"
	quietGroupC := "group-c"

	for i := 0; i < 10; i++ {
		_, err := store.SendMessage(ctx, queueName, &models.SendMessageRequest{
			MessageBody:    fmt.Sprintf("noisy-%d", i),
			QueueUrl:       queueName,
			MessageGroupId: &noisyGroup,
		})
		require.NoError(t, err)
	}
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{
		MessageBody:    "quiet-b",
		QueueUrl:       queueName,
		MessageGroupId: &quietGroupB,
	})
	require.NoError(t, err)
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{
		MessageBody:    "quiet-c",
		QueueUrl:       queueName,
		MessageGroupId: &quietGroupC,
	})
	require.NoError(t, err)

	// In an unfair system, we would expect to receive all 10 messages from 'group-a'
	// before ever seeing messages from the other groups.
	// A fair system should interleave messages from different groups.

	var receivedGroupIDs []string
	for i := 0; i < 3; i++ {
		// Receive one message at a time to see the ordering.
		// We set a high visibility timeout to ensure groups are locked for the duration of the test.
		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{
			MaxNumberOfMessages: 1,
			QueueUrl:            queueName,
			VisibilityTimeout:   60,
		})
		require.NoError(t, err)
		// It's possible to receive no messages if all groups are locked,
		// but in this test setup, we should always get a message.
		if len(resp.Messages) > 0 {
			msg := resp.Messages[0]
			// To get the group ID, we need to fetch the full message from the DB,
			// as it's not returned in the ReceiveMessage response.
			fullMsg, err := getMessageFromDB(store, queueName, msg.MessageId)
			require.NoError(t, err)
			receivedGroupIDs = append(receivedGroupIDs, fullMsg.MessageGroupId)

			// We must delete the message to unlock the group for the next receive.
			// This simulates a worker processing the message and deleting it.
			err = store.DeleteMessage(ctx, queueName, msg.ReceiptHandle)
			require.NoError(t, err)
		}
	}

	// With the current unfair implementation, this will be ["group-a", "group-a", "group-a"]
	// A perfectly fair implementation would be ["group-a", "group-b", "group-c"] in some order.
	// We will assert that we see messages from at least two different groups in the first 3 receives.
	t.Logf("Received group IDs: %v", receivedGroupIDs)
	groupSet := make(map[string]bool)
	for _, id := range receivedGroupIDs {
		groupSet[id] = true
	}
	assert.Greater(t, len(groupSet), 1, "Expected to receive messages from more than one group")
}

// Helper function to get the full message details from the database
func getMessageFromDB(s *FDBStore, queueName, messageID string) (*models.Message, error) {
	var msg models.Message
	_, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		queueDir, err := directory.Open(rtr, []string{"concreteq", queueName}, nil)
		if err != nil {
			return nil, err
		}
		messagesDir := queueDir.Sub("messages")
		msgBytes, err := rtr.Get(messagesDir.Pack(tuple.Tuple{messageID})).Get()
		if err != nil {
			return nil, err
		}
		if msgBytes == nil {
			return nil, fmt.Errorf("message %s not found", messageID)
		}
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func TestFDBStore_DeleteMessage(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-delete-queue"
	err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// --- Sub-test: Successful Deletion ---
	t.Run("Successful Deletion", func(t *testing.T) {
		// Send and receive a message to get a valid receipt handle
		_, err := store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg-to-delete"})
		require.NoError(t, err)
		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
		require.NoError(t, err)
		require.Len(t, resp.Messages, 1)

		// Delete the message
		err = store.DeleteMessage(ctx, queueName, resp.Messages[0].ReceiptHandle)
		assert.NoError(t, err)

		// Verify the message is gone by trying to receive again after timeout
		// (though in this implementation, deleting also removes it from inflight)
		time.Sleep(100 * time.Millisecond) // Give time for any background processing
		resp2, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1, WaitTimeSeconds: 0})
		require.NoError(t, err)
		assert.Len(t, resp2.Messages, 0)
	})

	// --- Sub-test: Non-Existent Receipt Handle ---
	t.Run("Non-Existent Receipt Handle", func(t *testing.T) {
		// Attempting to delete a message with a handle that doesn't exist should succeed
		err := store.DeleteMessage(ctx, queueName, "non-existent-handle")
		assert.NoError(t, err, "deleting a non-existent receipt handle should not return an error")
	})

	// --- Sub-test: Deleting from Non-Existent Queue ---
	t.Run("Non-Existent Queue", func(t *testing.T) {
		err := store.DeleteMessage(ctx, "non-existent-queue", "any-handle")
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})

	// --- Sub-test: Malformed Receipt Handle ---
	t.Run("Malformed Receipt Handle", func(t *testing.T) {
		// To test this, we need to manually insert a bad receipt handle into FDB
		badHandle := "bad-handle"
		_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			queueDir, _ := directory.Open(tr, []string{"concreteq", queueName}, nil)
			inflightDir := queueDir.Sub("inflight")
			tr.Set(inflightDir.Pack(tuple.Tuple{badHandle}), []byte("this is not valid json"))
			return nil, nil
		})
		require.NoError(t, err)

		err = store.DeleteMessage(ctx, queueName, badHandle)
		assert.ErrorIs(t, err, ErrInvalidReceiptHandle)
	})
}

func TestFDBStore_StandardQueue_ReceiveDelete(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-std-queue"
	err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// Send one message
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1", QueueUrl: queueName})
	require.NoError(t, err)

	// 1. Receive the message
	receiveReq := &models.ReceiveMessageRequest{MaxNumberOfMessages: 1, QueueUrl: queueName}
	resp1, err := store.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	require.Len(t, resp1.Messages, 1)
	msg1 := resp1.Messages[0]

	// 2. Try to receive again, should get nothing as it's in-flight
	resp2, err := store.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	assert.Len(t, resp2.Messages, 0)

	// 3. Delete the message
	err = store.DeleteMessage(ctx, queueName, msg1.ReceiptHandle)
	require.NoError(t, err)

	// 4. Try to receive again, should still get nothing as it's deleted
	resp3, err := store.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	assert.Len(t, resp3.Messages, 0)
}

func TestFDBStore_FifoQueue_ReceiveDelete(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-fifo-queue.fifo"
	err := store.CreateQueue(ctx, queueName, map[string]string{"FifoQueue": "true"}, nil)
	require.NoError(t, err)

	// Send messages to two different groups
	g1 := "group1"
	g2 := "group2"
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "g1-msg1", QueueUrl: queueName, MessageGroupId: &g1})
	require.NoError(t, err)
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "g1-msg2", QueueUrl: queueName, MessageGroupId: &g1})
	require.NoError(t, err)
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "g2-msg1", QueueUrl: queueName, MessageGroupId: &g2})
	require.NoError(t, err)

	// 1. Receive from the queue, should get the first message from the first group
	receiveReq := &models.ReceiveMessageRequest{MaxNumberOfMessages: 10, QueueUrl: queueName}
	resp1, err := store.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	require.Len(t, resp1.Messages, 2)
	assert.Equal(t, "g1-msg1", resp1.Messages[0].Body)
	assert.Equal(t, "g1-msg2", resp1.Messages[1].Body)

	// 2. group1 is now locked. Receive again, should get message from group2.
	resp2, err := store.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	require.Len(t, resp2.Messages, 1)
	assert.Equal(t, "g2-msg1", resp2.Messages[0].Body)

	// 3. Both groups are now locked. Receive again, should get nothing.
	resp3, err := store.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	assert.Len(t, resp3.Messages, 0)
}

func TestFDBStore_FifoQueue_ReceiveDeduplication(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-fifo-dedup.fifo"
	err := store.CreateQueue(ctx, queueName, map[string]string{"FifoQueue": "true"}, nil)
	require.NoError(t, err)

	g1 := "group1"
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1", QueueUrl: queueName, MessageGroupId: &g1})
	require.NoError(t, err)

	// 1. Receive with an attempt ID
	attemptID := "dedup-attempt-1"
	receiveReq := &models.ReceiveMessageRequest{
		QueueUrl:                queueName,
		ReceiveRequestAttemptId: attemptID,
	}
	resp1, err := store.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	require.Len(t, resp1.Messages, 1)

	// 2. Receive again with the same attempt ID, should get the same message
	resp2, err := store.ReceiveMessage(ctx, queueName, receiveReq)
	require.NoError(t, err)
	require.Len(t, resp2.Messages, 1)
	assert.Equal(t, resp1.Messages[0].MessageId, resp2.Messages[0].MessageId)
	assert.Equal(t, resp1.Messages[0].ReceiptHandle, resp2.Messages[0].ReceiptHandle)
}

func TestFDBStore_PurgeQueue(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("purges a queue successfully", func(t *testing.T) {
		queueName := "queue-to-purge"
		attributes := map[string]string{"VisibilityTimeout": "30"}
		tags := map[string]string{"Owner": "test"}

		// 1. Create queue with attributes and tags
		err := store.CreateQueue(ctx, queueName, attributes, tags)
		require.NoError(t, err)

		// 2. Add some dummy message keys directly to FDB
		_, err = store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			queueDir, err := directory.Open(tr, []string{"concreteq", queueName}, nil)
			require.NoError(t, err)
			messagesDir := queueDir.Sub("messages")
			tr.Set(messagesDir.Pack(tuple.Tuple{"msg1"}), []byte("message 1"))
			tr.Set(messagesDir.Pack(tuple.Tuple{"msg2"}), []byte("message 2"))
			return nil, nil
		})
		require.NoError(t, err)

		// 3. Purge the queue
		err = store.PurgeQueue(ctx, queueName)
		assert.NoError(t, err)

		// 4. Verify messages are gone and metadata remains
		_, err = store.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			queueDir, err := directory.Open(rtr, []string{"concreteq", queueName}, nil)
			require.NoError(t, err)
			messagesDir := queueDir.Sub("messages")

			// Check messages are deleted
			prefix := messagesDir.Pack(tuple.Tuple{})
			pr, err := fdb.PrefixRange(prefix)
			require.NoError(t, err)
			r := rtr.GetRange(pr, fdb.RangeOptions{})
			kvs, err := r.GetSliceWithError()
			require.NoError(t, err)
			assert.Empty(t, kvs, "expected messages to be deleted")

			// Check attributes still exist
			attrsBytes, err := rtr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
			require.NoError(t, err)
			assert.NotEmpty(t, attrsBytes, "expected attributes to remain")

			// Check last_purged_at was set
			lastPurgedBytes, err := rtr.Get(queueDir.Pack(tuple.Tuple{"last_purged_at"})).Get()
			require.NoError(t, err)
			assert.NotEmpty(t, lastPurgedBytes, "expected last_purged_at to be set")

			return nil, nil
		})
		require.NoError(t, err)
	})

	t.Run("returns error for non-existent queue", func(t *testing.T) {
		err := store.PurgeQueue(ctx, "non-existent-queue-for-purge")
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})

	t.Run("returns error if purged recently", func(t *testing.T) {
		queueName := "queue-to-purge-twice"
		err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// First purge should succeed
		err = store.PurgeQueue(ctx, queueName)
		require.NoError(t, err)

		// Immediate second purge should fail
		err = store.PurgeQueue(ctx, queueName)
		assert.ErrorIs(t, err, ErrPurgeQueueInProgress)
	})
}

func TestFDBStore_SendMessage(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-send-message-queue"
	err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	t.Run("sends a simple message successfully", func(t *testing.T) {
		msgRequest := &models.SendMessageRequest{
			MessageBody: "hello from fdb test",
			QueueUrl:    "http://localhost:8080/queues/" + queueName,
		}

		resp, err := store.SendMessage(ctx, queueName, msgRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp)

		assert.NotEmpty(t, resp.MessageId)
		assert.Equal(t, "880265a6e6fa189b855dc792ed428f2c", resp.MD5OfMessageBody) // md5 of "hello from fdb test"
		assert.Nil(t, resp.MD5OfMessageAttributes)
		assert.Nil(t, resp.SequenceNumber)

		// Verify message is in the database
		_, err = store.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			queueDir, _ := directory.Open(rtr, []string{"concreteq", queueName}, nil)
			messagesDir := queueDir.Sub("messages")
			msgBytes, err := rtr.Get(messagesDir.Pack(tuple.Tuple{resp.MessageId})).Get()
			require.NoError(t, err)
			require.NotEmpty(t, msgBytes)

			var storedMsg models.Message
			err = json.Unmarshal(msgBytes, &storedMsg)
			require.NoError(t, err)
			assert.Equal(t, msgRequest.MessageBody, storedMsg.Body)
			return nil, nil
		})
		require.NoError(t, err)
	})

	t.Run("sends a message with attributes and verifies hash", func(t *testing.T) {
		stringValue := "my_attribute_value_1"
		msgRequest := &models.SendMessageRequest{
			MessageBody: "message with attributes",
			QueueUrl:    "http://localhost:8080/queues/" + queueName,
			MessageAttributes: map[string]models.MessageAttributeValue{
				"my_attribute_name_1": {
					DataType:    "String",
					StringValue: &stringValue,
				},
			},
		}

		resp, err := store.SendMessage(ctx, queueName, msgRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.MessageId)
		// This hash is pre-calculated based on the SQS specification for a single attribute.
		assert.Equal(t, "8ef4d60dbc8efda9f260e1dfd09d29f3", *resp.MD5OfMessageAttributes)
	})

	t.Run("sends a message with delay", func(t *testing.T) {
		delaySeconds := int32(10)
		msgRequest := &models.SendMessageRequest{
			MessageBody:  "delayed message",
			DelaySeconds: &delaySeconds,
			QueueUrl:     "http://localhost:8080/queues/" + queueName,
		}

		startTime := time.Now().Unix()
		resp, err := store.SendMessage(ctx, queueName, msgRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp)

		// Verify message is in the database and VisibleAfter is set correctly
		_, err = store.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			queueDir, _ := directory.Open(rtr, []string{"concreteq", queueName}, nil)
			messagesDir := queueDir.Sub("messages")
			msgBytes, err := rtr.Get(messagesDir.Pack(tuple.Tuple{resp.MessageId})).Get()
			require.NoError(t, err)
			var storedMsg models.Message
			err = json.Unmarshal(msgBytes, &storedMsg)
			require.NoError(t, err)
			assert.InDelta(t, startTime+10, storedMsg.VisibleAfter, 1)
			return nil, nil
		})
		require.NoError(t, err)
	})

	t.Run("sends a message to a fifo queue and checks deduplication", func(t *testing.T) {
		fifoQueueName := "test-send-fifo-queue.fifo"
		err := store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		dedupID := "dedup-id-123"
		gID := "group1"
		msgRequest := &models.SendMessageRequest{
			MessageBody:            "hello fifo",
			MessageGroupId:         &gID,
			MessageDeduplicationId: &dedupID,
			QueueUrl:               "http://localhost:8080/queues/" + fifoQueueName,
		}

		// First send
		resp1, err := store.SendMessage(ctx, fifoQueueName, msgRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp1)
		assert.NotEmpty(t, resp1.MessageId)
		assert.NotEmpty(t, resp1.SequenceNumber)

		// Second send with same dedup ID should be deduplicated and return the original response
		resp2, err := store.SendMessage(ctx, fifoQueueName, msgRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp2)
		assert.Equal(t, resp1, resp2)
	})

	t.Run("returns error if queue does not exist", func(t *testing.T) {
		msgRequest := &models.SendMessageRequest{
			MessageBody: "hello",
		}
		_, err := store.SendMessage(ctx, "non-existent-queue", msgRequest)
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})
}
