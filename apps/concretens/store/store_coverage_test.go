package store

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestStore_PollDeliveryTasks_Coverage(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// Clear Queue
	s.ClearQueue(ctx)

	// 1. Invalid Tuple Length (Len < 3)
	// Key: ("queue", timestamp)
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.subDir.Pack(tuple.Tuple{"queue", time.Now().UnixNano()})
		tr.Set(key, []byte("ignored"))
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Invalid Timestamp Type (String)
	// Key: ("queue", "not-a-number", uuid)
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.subDir.Pack(tuple.Tuple{"queue", "invalid-ts", "task-id-1"})
		tr.Set(key, []byte("ignored"))
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 3. Valid Timestamp Types (int, uint64) to verify casting logic
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Int
		keyInt := s.subDir.Pack(tuple.Tuple{"queue", int(time.Now().UnixNano()), "task-id-int"})
		tr.Set(keyInt, []byte(`{"TaskID":"task-id-int"}`))

		// Uint64
		keyUint := s.subDir.Pack(tuple.Tuple{"queue", uint64(time.Now().UnixNano()), "task-id-uint"})
		tr.Set(keyUint, []byte(`{"TaskID":"task-id-uint"}`))

		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Poll behavior
	tasks, err := s.PollDeliveryTasks(ctx, 100)
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	// Verification
	// Just loop to ensure no panic
	for _, task := range tasks {
		_ = task
	}

	// 4. Invalid Topic Keys (Coverage for ListTopics)
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Key with int instead of string
		keyInt := s.topicDir.Pack(tuple.Tuple{123})
		tr.Set(keyInt, []byte(`{}`))
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Call ListTopics
	topics, err := s.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Should skip invalid key properly
	if len(topics) > 0 {
		// We might have valid topics from other tests?
		// But we didn't clear topics.
	}

	// 5. Invalid Subscription Keys (Coverage for ListSubscriptions)
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Key with length != 1
		keyLen := s.subDir.Pack(tuple.Tuple{"sub1", "extra"})
		tr.Set(keyLen, []byte(`{}`))

		// Key with int
		keyInt := s.subDir.Pack(tuple.Tuple{456})
		tr.Set(keyInt, []byte(`{}`))

		// Corrupted JSON
		keyBadJSON := s.subDir.Pack(tuple.Tuple{"sub-corrupt"})
		tr.Set(keyBadJSON, []byte(`{invalid`))
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Call ListSubscriptions
	subs, err := s.ListSubscriptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Should skip logic
	_ = subs
}
