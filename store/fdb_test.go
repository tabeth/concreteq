package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestDB connects to FDB and cleans up the test directory.
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
