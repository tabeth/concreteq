package store

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdempotency_Success_Retry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	db := NewTestDB(t)

	prefix := tuple.Tuple{"test", uuid.New().String()}
	ps := NewPageStore(db, prefix, DefaultConfig())
	ctx := context.Background()

	// Initial setup
	err := ps.SetVersionAndSize(ctx, 100, 4096, "", 0)
	require.NoError(t, err)

	// Attempt 1: Success
	reqID := uuid.New().String()
	baseVer := int64(100)
	newVer := int64(101)

	err = ps.SetVersionAndSize(ctx, newVer, 8192, reqID, baseVer)
	assert.NoError(t, err)

	// Verify state
	st, err := ps.CurrentVersion(ctx)
	assert.NoError(t, err)
	assert.Equal(t, newVer, st.Version)
	assert.Equal(t, int64(8192), st.Size)

	// Attempt 2: Retry (Same RequestID, Same BaseVersion)
	// Even though current version is 101, and baseVer is 100, requestID match should make it return success (nil)
	// AND not change state.
	err = ps.SetVersionAndSize(ctx, newVer, 8192, reqID, baseVer)
	assert.NoError(t, err, "Should succeed due to idempotency")

	// Verify it didn't do anything weird (like increment again if we had logic for that, but here it just returns nil)
}

func TestIdempotency_Conflict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	db := NewTestDB(t)

	prefix := tuple.Tuple{"test", uuid.New().String()}
	ps := NewPageStore(db, prefix, DefaultConfig())
	ctx := context.Background()

	// Initial setup: Version 100
	require.NoError(t, ps.SetVersionAndSize(ctx, 100, 4096, "", 0))

	// Client A updates to 101
	reqA := uuid.New().String()
	err := ps.SetVersionAndSize(ctx, 101, 4096, reqA, 100)
	assert.NoError(t, err)

	// Client B tries to update to 102, based on 100 (OLD)
	reqB := uuid.New().String()
	err = ps.SetVersionAndSize(ctx, 102, 4096, reqB, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "conflict")
}

func TestIdempotency_GC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	db := NewTestDB(t)
	prefix := tuple.Tuple{"test", uuid.New().String()}
	ps := NewPageStore(db, prefix, DefaultConfig())
	ctx := context.Background()

	// Manually insert an expired key
	expiredReqID := "expired-req"
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		k := ps.subspace.Pack(tuple.Tuple{"tx_map", expiredReqID})
		// 1 hour ago
		expiry := time.Now().Add(-1 * time.Hour).UnixNano()
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(expiry))
		tr.Set(k, buf)
		return nil, nil
	})
	require.NoError(t, err)

	// Insert fresh key
	freshReqID := "fresh-req"
	err = ps.SetVersionAndSize(ctx, 200, 4096, freshReqID, 0)

	// Run GC
	err = ps.GarbageCollectTxMap(ctx)
	assert.NoError(t, err)

	// Verify
	_, err = db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		// Check expired
		k1 := ps.subspace.Pack(tuple.Tuple{"tx_map", expiredReqID})
		v1 := r.Get(k1).MustGet()
		assert.Empty(t, v1, "Expired key should be gone")

		// Check fresh
		k2 := ps.subspace.Pack(tuple.Tuple{"tx_map", freshReqID})
		v2 := r.Get(k2).MustGet()
		assert.NotEmpty(t, v2, "Fresh key should remain")
		return nil, nil
	})
	assert.NoError(t, err)
}
