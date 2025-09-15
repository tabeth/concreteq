package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/models"
)

func TestFDBStore_NewFDBStore(t *testing.T) {
	store, err := NewFDBStore()
	require.NoError(t, err)
	assert.NotNil(t, store)
	assert.NotNil(t, store.GetDB())
}

func TestFDBStore_Unimplemented(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	_, err := store.GetQueueURL(ctx, "q")
	assert.NoError(t, err)
	assert.NoError(t, store.AddPermission(ctx, "q", "l", nil))
	assert.NoError(t, store.RemovePermission(ctx, "q", "l"))
	_, err = store.ListDeadLetterSourceQueues(ctx, "q")
	assert.NoError(t, err)
	_, err = store.StartMessageMoveTask(ctx, "s", "d")
	assert.NoError(t, err)
	assert.NoError(t, store.CancelMessageMoveTask(ctx, "t"))
	_, err = store.ListMessageMoveTasks(ctx, "s")
	assert.NoError(t, err)
}

func TestFDBStore_Unimplemented_Stubs(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	_, err := store.ListDeadLetterSourceQueues(ctx, "q")
	assert.NoError(t, err)
	_, err = store.StartMessageMoveTask(ctx, "s", "d")
	assert.NoError(t, err)
	err = store.CancelMessageMoveTask(ctx, "t")
	assert.NoError(t, err)
	_, err = store.ListMessageMoveTasks(ctx, "s")
	assert.NoError(t, err)
}

func TestHashSystemAttributes(t *testing.T) {
	sv := "value"
	attrs := map[string]models.MessageSystemAttributeValue{
		"attr1": {DataType: "String", StringValue: &sv},
		"attr2": {DataType: "Binary", BinaryValue: []byte("value2")},
	}
	hash := hashSystemAttributes(attrs)
	assert.NotEmpty(t, hash)
}

func TestFDBStore_ChangeMessageVisibility(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-change-visibility"
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// Send a message
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg-to-change"})
	require.NoError(t, err)

	// Receive the message
	resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	receiptHandle := resp.Messages[0].ReceiptHandle

	// Change visibility
	err = store.ChangeMessageVisibility(ctx, queueName, receiptHandle, 1)
	require.NoError(t, err)

	// Try to receive again, should get nothing
	resp2, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
	require.NoError(t, err)
	assert.Len(t, resp2.Messages, 0)

	// Wait for new visibility timeout to expire
	time.Sleep(1200 * time.Millisecond)

	// Receive again, should get the message
	resp3, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
	require.NoError(t, err)
	assert.Len(t, resp3.Messages, 1)
}

func TestFDBStore_ChangeMessageVisibilityBatch(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-change-visibility-batch"
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// Send two messages
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1"})
	require.NoError(t, err)
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg2"})
	require.NoError(t, err)

	// Receive them
	resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 2})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 2)

	// Change visibility in a batch
	entries := []models.ChangeMessageVisibilityBatchRequestEntry{
		{Id: "m1", ReceiptHandle: resp.Messages[0].ReceiptHandle, VisibilityTimeout: 1},
		{Id: "m2", ReceiptHandle: resp.Messages[1].ReceiptHandle, VisibilityTimeout: 1},
		{Id: "m3", ReceiptHandle: "non-existent-handle", VisibilityTimeout: 1},
	}

	batchResp, err := store.ChangeMessageVisibilityBatch(ctx, queueName, entries)
	require.NoError(t, err)
	require.NotNil(t, batchResp)
	assert.Len(t, batchResp.Successful, 2)
	assert.Len(t, batchResp.Failed, 1)
	assert.Equal(t, "m3", batchResp.Failed[0].Id)
	assert.Equal(t, "ReceiptHandleIsInvalid", batchResp.Failed[0].Code)

	// Try to receive again, should get nothing
	resp2, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 2})
	require.NoError(t, err)
	assert.Len(t, resp2.Messages, 0)

	// Wait for timeout
	time.Sleep(1200 * time.Millisecond)

	// Receive again, should get both messages
	resp3, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 2})
	require.NoError(t, err)
	assert.Len(t, resp3.Messages, 2)
}

func TestFDBStore_DeleteMessageBatch_InvalidReceipt(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-delete-batch-invalid-receipt"
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// Manually insert a malformed receipt handle
	_, err = store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		qDir, _ := store.dir.Open(tr, []string{queueName}, nil)
		tr.Set(qDir.Sub("inflight").Pack(tuple.Tuple{"bad-handle"}), []byte("not-json"))
		return nil, nil
	})
	require.NoError(t, err)

	entries := []models.DeleteMessageBatchRequestEntry{
		{Id: "1", ReceiptHandle: "bad-handle"},
	}
	resp, err := store.DeleteMessageBatch(ctx, queueName, entries)
	require.NoError(t, err)
	assert.Len(t, resp.Failed, 1)
	assert.Equal(t, "InvalidReceiptHandle", resp.Failed[0].Code)
}

func TestFDBStore_ChangeMessageVisibility_NotInflight(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-change-visibility-not-inflight"
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	err = store.ChangeMessageVisibility(ctx, queueName, "not-a-real-handle", 60)
	assert.ErrorIs(t, err, ErrInvalidReceiptHandle)
}

func TestFDBStore_DeleteMessageBatch_MessageNotFound(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-delete-batch-msg-not-found"
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// Send and receive a message
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1"})
	require.NoError(t, err)
	resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	receiptHandle := resp.Messages[0].ReceiptHandle

	// Delete the message
	err = store.DeleteMessage(ctx, queueName, receiptHandle)
	require.NoError(t, err)

	// Try to delete it again in a batch
	entries := []models.DeleteMessageBatchRequestEntry{
		{Id: "1", ReceiptHandle: receiptHandle},
	}
	delResp, err := store.DeleteMessageBatch(ctx, queueName, entries)
	require.NoError(t, err)
	assert.Len(t, delResp.Successful, 1)
}

// NOTE: These tests require a running FoundationDB instance.
// They are integration tests, not unit tests.

func generateRandomString(length int) string {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

func setupTestDB(tb testing.TB) (*FDBStore, func()) {
	tb.Helper()
	fdb.MustAPIVersion(730)
	db, err := fdb.OpenDefault()
	if err != nil {
		tb.Logf("FoundationDB integration tests skipped: could not open default FDB database: %v", err)
		tb.Skip("skipping FoundationDB tests: could not open default FDB database")
	}

	// Pre-test condition to check if the cluster is up with a short timeout.
	tb.Log("Checking FoundationDB cluster availability...")
	tr, err := db.CreateTransaction()
	if err != nil {
		tb.Skipf("skipping FoundationDB tests: could not create transaction: %v", err)
	}
	// Set a 1-second timeout for the check to avoid long waits.
	err = tr.Options().SetTimeout(1000)
	if err != nil {
		tb.Skipf("skipping FoundationDB tests: could not set transaction timeout: %v", err)
	}

	_, err = tr.Get(fdb.Key("\xff\xff/status/json")).Get()
	if err != nil {
		tb.Logf("FoundationDB integration tests skipped: could not connect to cluster: %v. Please ensure FoundationDB is running.", err)
		tb.Skip("Please ensure FoundationDB is running and accessible.")
	}
	tb.Log("FoundationDB cluster is available. Proceeding with tests.")

	// Create a unique directory for this test to ensure isolation
	testDirName := "test-" + strings.ReplaceAll(tb.Name(), "/", "_") + "-" + generateRandomString(4)
	testPath := []string{"concreteq_test_root", testDirName}

	store, err := NewFDBStoreAtPath(testPath...)
	require.NoError(tb, err)

	teardown := func() {
		// Clean up the test directory after the test
		root, err := directory.Open(db, []string{"concreteq_test_root"}, nil)
		if err != nil {
			tb.Logf("failed to open root test directory for cleanup: %v", err)
			return
		}
		_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			_, err := root.Remove(tr, []string{testDirName})
			return nil, err
		})
		if err != nil {
			tb.Logf("failed to remove test directory %s during cleanup: %v", testDirName, err)
		}
	}

	return store, teardown
}

func TestFDBStore_CreateQueue(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("creates a new queue successfully", func(t *testing.T) {
		queueName := "test-queue-1"
		existingAttrs, err := store.CreateQueue(ctx, queueName, nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, existingAttrs, "Expected nil for existing attributes on new queue creation")

		// Verify the directory was created within the store's subspace
		exists, err := store.dir.Exists(store.db, []string{queueName})
		assert.NoError(t, err)
		assert.True(t, exists, "expected queue directory to exist")
	})

	t.Run("returns attributes if queue already exists", func(t *testing.T) {
		queueName := "test-queue-2"
		attributes := map[string]string{"VisibilityTimeout": "100"}
		// Create it once
		_, err := store.CreateQueue(ctx, queueName, attributes, nil)
		require.NoError(t, err)

		// Try to create it again
		existingAttrs, err := store.CreateQueue(ctx, queueName, attributes, nil)
		assert.NoError(t, err)
		assert.Equal(t, attributes, existingAttrs)
	})

	t.Run("stores attributes and tags correctly", func(t *testing.T) {
		queueName := "test-queue-3"
		attributes := map[string]string{"VisibilityTimeout": "60"}
		tags := map[string]string{"Project": "concreteq"}

		_, err := store.CreateQueue(ctx, queueName, attributes, tags)
		require.NoError(t, err)

		// Verify data directly in FDB
		_, err = store.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			queueDir, err := store.dir.Open(rtr, []string{queueName}, nil)
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
		_, err := store.CreateQueue(context.Background(), name, nil, nil)
		// We ignore "already exists" error to make tests idempotent
		if err != nil {
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
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// Now delete it
		err = store.DeleteQueue(ctx, queueName)
		assert.NoError(t, err)

		// Verify it's gone
		exists, err := store.dir.Exists(store.db, []string{queueName})
		assert.NoError(t, err)
		assert.False(t, exists, "expected queue directory to be deleted")
	})

	t.Run("returns error for non-existent queue", func(t *testing.T) {
		queueName := "non-existent-queue"
		err := store.DeleteQueue(ctx, queueName)
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})

	t.Run("deletes a queue and all its contents", func(t *testing.T) {
		queueName := "queue-with-contents"
		// 1. Create queue and add a message
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)
		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "this should be deleted"})
		require.NoError(t, err)

		// 2. Delete the queue
		err = store.DeleteQueue(ctx, queueName)
		require.NoError(t, err)

		// 3. Re-create the queue with the same name
		_, err = store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// 4. Try to receive a message - should be empty
		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1, WaitTimeSeconds: 0})
		require.NoError(t, err)
		assert.Len(t, resp.Messages, 0, "queue should be empty after being deleted and re-created")
	})
}

func TestFDBStore_SendMessageBatch(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("sends a batch successfully to a standard queue", func(t *testing.T) {
		queueName := "test-batch-std"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)
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
		fifoQueueName := "test-batch-fifo-dedup.fifo"
		_, err := store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
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

		resp1, err := store.SendMessageBatch(ctx, fifoQueueName, batchRequest)
		require.NoError(t, err)
		require.NotNil(t, resp1)
		require.Len(t, resp1.Successful, 2)
		require.Len(t, resp1.Failed, 0)
		originalMsg1ID := resp1.Successful[0].MessageId

		batchRequest2 := &models.SendMessageBatchRequest{
			QueueUrl: "http://localhost/" + fifoQueueName,
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "fifo1", MessageBody: "fifo batch 1", MessageGroupId: &gID, MessageDeduplicationId: &dedupID},
				{Id: "fifo3", MessageBody: "fifo batch 3", MessageGroupId: &gID},
			},
		}
		resp2, err := store.SendMessageBatch(ctx, fifoQueueName, batchRequest2)
		require.NoError(t, err)
		require.NotNil(t, resp2)
		require.Len(t, resp2.Successful, 2)
		require.Len(t, resp2.Failed, 0)

		assert.Equal(t, originalMsg1ID, resp2.Successful[0].MessageId)
		assert.NotEqual(t, resp1.Successful[1].MessageId, resp2.Successful[1].MessageId)
	})

	t.Run("returns error if queue does not exist", func(t *testing.T) {
		batchRequest := &models.SendMessageBatchRequest{
			QueueUrl: "http://localhost/non-existent",
			Entries:  []models.SendMessageBatchRequestEntry{{Id: "msg1", MessageBody: "wont be sent"}},
		}
		_, err := store.SendMessageBatch(ctx, "non-existent", batchRequest)
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})

	t.Run("handles partial failures within a batch", func(t *testing.T) {
		fifoQueueName := "test-batch-partial-fail.fifo"
		_, err := store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		gID := "batch-group-2"
		delay := int32(10)
		batchRequest := &models.SendMessageBatchRequest{
			QueueUrl: "http://localhost/" + fifoQueueName,
			Entries: []models.SendMessageBatchRequestEntry{
				{Id: "valid1", MessageBody: "this one is fine", MessageGroupId: &gID},
				{Id: "invalid_delay", MessageBody: "this one has delay on fifo", DelaySeconds: &delay, MessageGroupId: &gID},
				{Id: "valid2", MessageBody: "this one is also fine", MessageGroupId: &gID},
				{Id: "invalid_no_group", MessageBody: "this one is missing group id"},
			},
		}

		resp, err := store.SendMessageBatch(ctx, fifoQueueName, batchRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, resp.Successful, 2)
		assert.Len(t, resp.Failed, 2)

		_, err = getMessageFromDB(store, fifoQueueName, resp.Successful[0].MessageId)
		assert.NoError(t, err)
		_, err = getMessageFromDB(store, fifoQueueName, resp.Successful[1].MessageId)
		assert.NoError(t, err)

		failedCodes := make(map[string]string)
		for _, f := range resp.Failed {
			failedCodes[f.Id] = f.Code
		}
		assert.Equal(t, "InvalidParameterValue", failedCodes["invalid_delay"])
		assert.Equal(t, "MissingParameter", failedCodes["invalid_no_group"])
	})
}

func TestFDBStore_FifoQueue_Fairness(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-fifo-fairness.fifo"
	_, err := store.CreateQueue(ctx, queueName, map[string]string{"FifoQueue": "true"}, nil)
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
		// Use the store's directory, not a hardcoded path
		queueDir, err := s.dir.Open(rtr, []string{queueName}, nil)
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

func TestFDBStore_DeleteMessageBatch(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("successfully deletes a batch from a standard queue", func(t *testing.T) {
		queueName := "std-delete-batch"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// Send and receive two messages
		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1"})
		require.NoError(t, err)
		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg2"})
		require.NoError(t, err)
		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 2})
		require.NoError(t, err)
		require.Len(t, resp.Messages, 2)

		// Delete them in a batch
		entries := []models.DeleteMessageBatchRequestEntry{
			{Id: "del1", ReceiptHandle: resp.Messages[0].ReceiptHandle},
			{Id: "del2", ReceiptHandle: resp.Messages[1].ReceiptHandle},
		}
		delResp, err := store.DeleteMessageBatch(ctx, queueName, entries)
		require.NoError(t, err)
		assert.Len(t, delResp.Successful, 2)
		assert.Len(t, delResp.Failed, 0)

		// Verify queue is empty
		finalResp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 2})
		require.NoError(t, err)
		assert.Len(t, finalResp.Messages, 0)
	})

	t.Run("handles partial success in a batch", func(t *testing.T) {
		queueName := "std-delete-batch-partial"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// Send and receive one message
		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1"})
		require.NoError(t, err)
		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
		require.NoError(t, err)
		require.Len(t, resp.Messages, 1)

		// Try to delete one valid, one non-existent, and one malformed handle
		entries := []models.DeleteMessageBatchRequestEntry{
			{Id: "valid_del", ReceiptHandle: resp.Messages[0].ReceiptHandle},
			{Id: "non_existent_del", ReceiptHandle: "does-not-exist"},
			{Id: "malformed_del", ReceiptHandle: "this-is-not-a-uuid"},
		}
		// Manually insert a malformed receipt handle record
		_, err = store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			qDir, _ := store.dir.Open(tr, []string{queueName}, nil)
			tr.Set(qDir.Sub("inflight").Pack(tuple.Tuple{"this-is-not-a-uuid"}), []byte("bad-data"))
			return nil, nil
		})
		require.NoError(t, err)


		delResp, err := store.DeleteMessageBatch(ctx, queueName, entries)
		require.NoError(t, err)
		assert.Len(t, delResp.Successful, 2) // valid_del and non_existent_del
		assert.Len(t, delResp.Failed, 1)
		assert.Equal(t, "malformed_del", delResp.Failed[0].Id)
		assert.Equal(t, "InvalidReceiptHandle", delResp.Failed[0].Code)
	})
}

func TestFDBStore_DeleteMessage(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-delete-queue"
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
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
			queueDir, _ := store.dir.Open(tr, []string{queueName}, nil)
			inflightDir := queueDir.Sub("inflight")
			tr.Set(inflightDir.Pack(tuple.Tuple{badHandle}), []byte("this is not valid json"))
			return nil, nil
		})
		require.NoError(t, err)

		err = store.DeleteMessage(ctx, queueName, badHandle)
		assert.ErrorIs(t, err, ErrInvalidReceiptHandle)
	})

	t.Run("Deleting from FIFO queue unlocks message group", func(t *testing.T) {
		fifoQueueName := "fifo-delete-unlock.fifo"
		_, err := store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		g1 := "group-to-unlock"
		_, err = store.SendMessage(ctx, fifoQueueName, &models.SendMessageRequest{MessageBody: "g1-msg1", MessageGroupId: &g1})
		require.NoError(t, err)
		_, err = store.SendMessage(ctx, fifoQueueName, &models.SendMessageRequest{MessageBody: "g1-msg2", MessageGroupId: &g1})
		require.NoError(t, err)

		// 1. Receive the first message, which should lock the group
		resp1, err := store.ReceiveMessage(ctx, fifoQueueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
		require.NoError(t, err)
		require.Len(t, resp1.Messages, 1)
		assert.Equal(t, "g1-msg1", resp1.Messages[0].Body)

		// 2. Try to receive again, should get nothing because the group is locked
		resp2, err := store.ReceiveMessage(ctx, fifoQueueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
		require.NoError(t, err)
		require.Len(t, resp2.Messages, 0)

		// 3. Delete the first message
		err = store.DeleteMessage(ctx, fifoQueueName, resp1.Messages[0].ReceiptHandle)
		require.NoError(t, err)

		// 4. Receive again, should now get the second message
		resp3, err := store.ReceiveMessage(ctx, fifoQueueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
		require.NoError(t, err)
		require.Len(t, resp3.Messages, 1)
		assert.Equal(t, "g1-msg2", resp3.Messages[0].Body)
	})
}

func TestFDBStore_ReceiveMessage(t *testing.T) {
	ctx := context.Background()

	t.Run("Standard Queue", func(t *testing.T) {
		t.Run("Receive from empty queue", func(t *testing.T) {
			store, teardown := setupTestDB(t)
			defer teardown()
			queueName := "std-empty"
			_, err := store.CreateQueue(ctx, queueName, nil, nil)
			require.NoError(t, err)

			resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			require.NoError(t, err)
			assert.Len(t, resp.Messages, 0)
		})

		t.Run("Receive respects visibility timeout", func(t *testing.T) {
			store, teardown := setupTestDB(t)
			defer teardown()
			queueName := "std-visibility"
			// Use a short visibility timeout for the test
			_, err := store.CreateQueue(ctx, queueName, map[string]string{"VisibilityTimeout": "1"}, nil)
			require.NoError(t, err)
			_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "visible-test"})
			require.NoError(t, err)

			// 1. Receive the message
			resp1, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			require.NoError(t, err)
			require.Len(t, resp1.Messages, 1)

			// 2. Immediately try to receive again, should get nothing
			resp2, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			require.NoError(t, err)
			assert.Len(t, resp2.Messages, 0)

			// 3. Wait for visibility timeout to expire
			time.Sleep(1200 * time.Millisecond)

			// 4. Receive again, should get the same message
			resp3, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			require.NoError(t, err)
			require.Len(t, resp3.Messages, 1)
			assert.Equal(t, resp1.Messages[0].Body, resp3.Messages[0].Body)
		})

		t.Run("Long polling waits for message", func(t *testing.T) {
			store, teardown := setupTestDB(t)
			defer teardown()
			queueName := "std-long-poll"
			_, err := store.CreateQueue(ctx, queueName, nil, nil)
			require.NoError(t, err)

			msgChan := make(chan *models.ReceiveMessageResponse)
			go func() {
				// This will block for up to 2 seconds
				resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{WaitTimeSeconds: 2})
				assert.NoError(t, err)
				msgChan <- resp
			}()

			// Send a message after a short delay
			time.Sleep(200 * time.Millisecond)
			_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "long-poll-msg"})
			require.NoError(t, err)

			// The response should come through the channel
			select {
			case resp := <-msgChan:
				require.Len(t, resp.Messages, 1)
				assert.Equal(t, "long-poll-msg", resp.Messages[0].Body)
			case <-time.After(3 * time.Second):
				t.Fatal("long poll did not receive message in time")
			}
		})
	})

	t.Run("FIFO Queue", func(t *testing.T) {
		t.Run("Receive respects message group lock", func(t *testing.T) {
			store, teardown := setupTestDB(t)
			defer teardown()
			queueName := "fifo-group-lock.fifo"
			_, err := store.CreateQueue(ctx, queueName, map[string]string{"FifoQueue": "true"}, nil)
			require.NoError(t, err)
			g1, g2 := "group1", "group2"
			_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "g1-msg1", MessageGroupId: &g1})
			require.NoError(t, err)
			_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "g2-msg1", MessageGroupId: &g2})
			require.NoError(t, err)

			// 1. Receive, should get g1-msg1 and lock group1
			resp1, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			require.NoError(t, err)
			require.Len(t, resp1.Messages, 1)
			assert.Equal(t, "g1-msg1", resp1.Messages[0].Body)

			// 2. Receive again, should get g2-msg1 because group1 is locked
			resp2, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			require.NoError(t, err)
			require.Len(t, resp2.Messages, 1)
			assert.Equal(t, "g2-msg1", resp2.Messages[0].Body)

			// 3. Receive again, should get nothing because both groups are locked
			resp3, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
			require.NoError(t, err)
			assert.Len(t, resp3.Messages, 0)
		})

		t.Run("Receive with deduplication attempt ID", func(t *testing.T) {
			store, teardown := setupTestDB(t)
			defer teardown()
			queueName := "fifo-dedup-recv.fifo"
			_, err := store.CreateQueue(ctx, queueName, map[string]string{"FifoQueue": "true"}, nil)
			require.NoError(t, err)
			g1 := "group1"
			_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1", MessageGroupId: &g1})
			require.NoError(t, err)

			attemptID := "dedup-attempt-ABC"
			req := &models.ReceiveMessageRequest{ReceiveRequestAttemptId: attemptID}

			// 1. Receive with attempt ID
			resp1, err := store.ReceiveMessage(ctx, queueName, req)
			require.NoError(t, err)
			require.Len(t, resp1.Messages, 1)

			// 2. Receive again with same ID, should get the same message back
			resp2, err := store.ReceiveMessage(ctx, queueName, req)
			require.NoError(t, err)
			require.Len(t, resp2.Messages, 1)
			assert.Equal(t, resp1.Messages[0].MessageId, resp2.Messages[0].MessageId)
			assert.Equal(t, resp1.Messages[0].ReceiptHandle, resp2.Messages[0].ReceiptHandle)
		})
	})
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
		_, err := store.CreateQueue(ctx, queueName, attributes, tags)
		require.NoError(t, err)

		// 2. Add some dummy message keys directly to FDB
		_, err = store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			queueDir, err := store.dir.Open(tr, []string{queueName}, nil)
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
			queueDir, err := store.dir.Open(rtr, []string{queueName}, nil)
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
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// First purge should succeed
		err = store.PurgeQueue(ctx, queueName)
		require.NoError(t, err)

		// Immediate second purge should fail
		err = store.PurgeQueue(ctx, queueName)
		assert.ErrorIs(t, err, ErrPurgeQueueInProgress)
	})

	t.Run("purges a FIFO queue successfully", func(t *testing.T) {
		queueName := "fifo-queue-to-purge.fifo"
		_, err := store.CreateQueue(ctx, queueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)
		g1 := "group1"
		_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "msg1", MessageGroupId: &g1})
		require.NoError(t, err)

		// Purge the queue
		err = store.PurgeQueue(ctx, queueName)
		assert.NoError(t, err)

		// Verify queue is empty
		resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1})
		require.NoError(t, err)
		assert.Len(t, resp.Messages, 0, "fifo queue should be empty after purge")
	})
}

func TestFDBStore_DeleteRecreateIsolation(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "queue-to-delete-and-recreate"
	// 1. Create queue and add a message
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)
	_, err = store.SendMessage(ctx, queueName, &models.SendMessageRequest{MessageBody: "this should be deleted"})
	require.NoError(t, err)

	// 2. Delete the queue
	err = store.DeleteQueue(ctx, queueName)
	require.NoError(t, err)

	// 3. Re-create the queue with the same name
	_, err = store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// 4. Try to receive a message - should be empty
	resp, err := store.ReceiveMessage(ctx, queueName, &models.ReceiveMessageRequest{MaxNumberOfMessages: 1, WaitTimeSeconds: 0})
	require.NoError(t, err, "ReceiveMessage should not fail after delete and re-create")
	require.NotNil(t, resp, "Response from ReceiveMessage should not be nil")
	assert.Len(t, resp.Messages, 0, "queue should be empty after being deleted and re-created")
}

func TestFDBStore_SendMessage(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("sends a simple message successfully", func(t *testing.T) {
		queueName := "test-send-simple"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		msgRequest := &models.SendMessageRequest{
			MessageBody: "hello from fdb test",
			QueueUrl:    "http://localhost:8080/queues/" + queueName,
		}

		resp, err := store.SendMessage(ctx, queueName, msgRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.MessageId)
		assert.Equal(t, "880265a6e6fa189b855dc792ed428f2c", resp.MD5OfMessageBody)
		assert.Nil(t, resp.MD5OfMessageAttributes)
		assert.Nil(t, resp.SequenceNumber)

		msg, err := getMessageFromDB(store, queueName, resp.MessageId)
		require.NoError(t, err)
		assert.Equal(t, msgRequest.MessageBody, msg.Body)
	})

	t.Run("sends a message with attributes and verifies hash", func(t *testing.T) {
		queueName := "test-send-with-attrs"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

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
		assert.Equal(t, "8ef4d60dbc8efda9f260e1dfd09d29f3", *resp.MD5OfMessageAttributes)
	})

	t.Run("sends a message with delay", func(t *testing.T) {
		queueName := "test-send-with-delay"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)
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

		msg, err := getMessageFromDB(store, queueName, resp.MessageId)
		require.NoError(t, err)
		assert.InDelta(t, startTime+10, msg.VisibleAfter, 1)
	})

	t.Run("sends a message to a fifo queue and checks deduplication", func(t *testing.T) {
		fifoQueueName := "test-send-fifo-dedup.fifo"
		_, err := store.CreateQueue(ctx, fifoQueueName, map[string]string{"FifoQueue": "true"}, nil)
		require.NoError(t, err)

		dedupID := "dedup-id-123"
		gID := "group1"
		msgRequest := &models.SendMessageRequest{
			MessageBody:            "hello fifo",
			MessageGroupId:         &gID,
			MessageDeduplicationId: &dedupID,
			QueueUrl:               "http://localhost:8080/queues/" + fifoQueueName,
		}

		resp1, err := store.SendMessage(ctx, fifoQueueName, msgRequest)
		assert.NoError(t, err)
		require.NotNil(t, resp1)
		assert.NotEmpty(t, resp1.MessageId)
		assert.NotEmpty(t, resp1.SequenceNumber)

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

func TestFDBStore_QueueTags(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-tags-queue"
	_, err := store.CreateQueue(ctx, queueName, nil, nil)
	require.NoError(t, err)

	// 1. List tags on a new queue, should be empty
	tags, err := store.ListQueueTags(ctx, queueName)
	require.NoError(t, err)
	assert.Empty(t, tags)

	// 2. Tag the queue
	tagsToSet := map[string]string{"env": "test", "project": "concreteq"}
	err = store.TagQueue(ctx, queueName, tagsToSet)
	require.NoError(t, err)

	// 3. List tags again, should have the new tags
	tags, err = store.ListQueueTags(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, tagsToSet, tags)

	// 4. Untag one key
	err = store.UntagQueue(ctx, queueName, []string{"env"})
	require.NoError(t, err)

	// 5. List tags again, should have one less tag
	tags, err = store.ListQueueTags(ctx, queueName)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"project": "concreteq"}, tags)

	// 6. Test error cases
	err = store.TagQueue(ctx, "non-existent-queue", tagsToSet)
	assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	err = store.UntagQueue(ctx, "non-existent-queue", []string{"env"})
	assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	_, err = store.ListQueueTags(ctx, "non-existent-queue")
	assert.ErrorIs(t, err, ErrQueueDoesNotExist)
}

func TestFDBStore_UntagQueue_Coverage(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("Untagging a queue with no tags", func(t *testing.T) {
		queueName := "test-tags-queue-no-tags"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		err = store.UntagQueue(ctx, queueName, []string{"any-key"})
		assert.NoError(t, err)
	})

	t.Run("Untagging with corrupt tags", func(t *testing.T) {
		queueName := "test-corrupt-tags"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		// Manually insert corrupt tag data
		_, err = store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			qDir, _ := store.dir.Open(tr, []string{queueName}, nil)
			tr.Set(qDir.Pack(tuple.Tuple{"tags"}), []byte("not-json"))
			return nil, nil
		})
		require.NoError(t, err)

		err = store.UntagQueue(ctx, queueName, []string{"any-key"})
		assert.Error(t, err)
	})
}

func TestFDBStore_SetQueueAttributes(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	queueName := "test-set-attrs-queue"
	initialAttrs := map[string]string{"VisibilityTimeout": "30"}

	// Create the queue with initial attributes
	_, err := store.CreateQueue(ctx, queueName, initialAttrs, nil)
	require.NoError(t, err)

	t.Run("sets attributes on an existing queue", func(t *testing.T) {
		newAttrs := map[string]string{
			"VisibilityTimeout":             "120",
			"ReceiveMessageWaitTimeSeconds": "10",
		}
		err := store.SetQueueAttributes(ctx, queueName, newAttrs)
		assert.NoError(t, err)

		// Verify the attributes were updated
		storedAttrs, err := store.GetQueueAttributes(ctx, queueName)
		require.NoError(t, err)
		assert.Equal(t, newAttrs, storedAttrs)
	})

	t.Run("returns error for non-existent queue", func(t *testing.T) {
		err := store.SetQueueAttributes(ctx, "non-existent-queue", map[string]string{"a": "b"})
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})
}

func TestFDBStore_GetQueueAttributes(t *testing.T) {
	ctx := context.Background()
	store, teardown := setupTestDB(t)
	defer teardown()

	t.Run("gets attributes for an existing queue", func(t *testing.T) {
		queueName := "test-queue-with-attrs"
		attributes := map[string]string{
			"VisibilityTimeout":      "120",
			"MessageRetentionPeriod": "86400",
		}
		_, err := store.CreateQueue(ctx, queueName, attributes, nil)
		require.NoError(t, err)

		storedAttrs, err := store.GetQueueAttributes(ctx, queueName)
		assert.NoError(t, err)
		assert.Equal(t, attributes, storedAttrs)
	})

	t.Run("returns empty map for queue with no attributes", func(t *testing.T) {
		queueName := "test-queue-no-attrs"
		_, err := store.CreateQueue(ctx, queueName, nil, nil)
		require.NoError(t, err)

		storedAttrs, err := store.GetQueueAttributes(ctx, queueName)
		assert.NoError(t, err)
		assert.NotNil(t, storedAttrs)
		assert.Empty(t, storedAttrs)
	})

	t.Run("returns error for non-existent queue", func(t *testing.T) {
		_, err := store.GetQueueAttributes(ctx, "non-existent-queue-for-attrs")
		assert.ErrorIs(t, err, ErrQueueDoesNotExist)
	})
}
