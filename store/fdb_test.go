package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOTE: These tests require a running FoundationDB instance.
// They are integration tests, not unit tests.

func setupTestDB(t *testing.T) (*FDBStore, func()) {
	fdb.MustAPIVersion(740)
	db, err := fdb.OpenDefault()
	if err != nil {
		t.Fatalf("could not open default FDB database: %v", err)
	}

	// Check if the database is reachable
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

	// Clean up the SQS directory before running tests
	dir, err := directory.CreateOrOpen(db, []string{"sqs"}, nil)
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
		exists, err := directory.Exists(store.db, []string{"sqs", queueName})
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
			queueDir, err := directory.Open(rtr, []string{"sqs", queueName}, nil)
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
