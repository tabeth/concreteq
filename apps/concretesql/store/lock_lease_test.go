package store

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/psanford/sqlite3vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sharedfdb "github.com/tabeth/kiroku-core/libs/fdb"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestParseExpiry_Error(t *testing.T) {
	val := ParseExpiry([]byte("short"))
	assert.Equal(t, int64(0), val)
	val = ParseExpiry(nil)
	assert.Equal(t, int64(0), val)
}

func TestParseExpiry_WithVersion(t *testing.T) {
	buf := make([]byte, 16)
	expected := int64(123456789)
	binary.BigEndian.PutUint64(buf[0:8], uint64(expected))
	val := ParseExpiry(buf)
	assert.Equal(t, expected, val)
}

func newTestDBLocal(t testing.TB) fdb.Database {
	fdbtest.SkipIfFDBUnavailable(t)
	db, err := sharedfdb.OpenDB(620)
	if err != nil {
		t.Fatalf("Failed to open FDB: %v", err)
	}
	return db
}

func TestLockManager_LeaseStealing(t *testing.T) {
	db := newTestDBLocal(t)
	prefix := tuple.Tuple{"test", "lock_leases"}

	// Cleanup
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})

	lm1 := NewLockManager(db, prefix, DefaultConfig()) // "Owner" (Simulated Crash)
	lm2 := NewLockManager(db, prefix, DefaultConfig()) // "Stealer"
	ctx := context.Background()

	// 1. LM1 acquires Exclusive Lock
	err := lm1.Lock(ctx, sqlite3vfs.LockExclusive, 0)

	require.NoError(t, err)

	// 2. LM2 tries to acquire -> Should Block
	ctxShort, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err = lm2.Lock(ctxShort, sqlite3vfs.LockShared, 0)

	// Should timeout or return error (LockShared blocks until exclusive is gone)
	// Our implementation returns context deadline exceeded if it can't get it.
	// Wait, Lock() loop uses ctx.Err().
	require.Error(t, err)

	// 3. Simulate LM1 Crash (Stop Heartbeat manually, wait for expiry)
	lm1.Kill()
	// We can't easily stop LM1's heartbeat without Unlocking, but we want it to hold the lock in DB.
	// We can Manually Overwrite the lock key with an OLD timestamp to simulate expiry.
	// Or just wait > LeaseDuration (5s). Waiting 6s is slow for unit test.
	// Let's overwrite the key.

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		lockSubspace := subspace.FromBytes(prefix.Pack()).Sub("locks")
		// Overwrite exclusive key with expired timestamp (1970)
		expired := make([]byte, 8) // 0 int64
		tr.Set(lockSubspace.Pack(tuple.Tuple{"exclusive"}), expired)
		return nil, nil
	})
	require.NoError(t, err)

	// 4. LM2 tries to acquire -> Should Succeed (Steal)
	// Only if it checks expiry.
	ctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()

	err = lm2.Lock(ctx2, sqlite3vfs.LockExclusive, 0)

	require.NoError(t, err)

	// 5. Verify LM2 owns it (or at least has it)
	// We can check checks.
}

func TestLockManager_Heartbeat(t *testing.T) {
	db := newTestDBLocal(t)
	prefix := tuple.Tuple{"test", "lock_heartbeat"}
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})

	lm := NewLockManager(db, prefix, DefaultConfig())
	ctx := context.Background()

	err := lm.Lock(ctx, sqlite3vfs.LockExclusive, 0)

	require.NoError(t, err)

	// Check timestamp
	var ts1 int64
	val1, err := db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		lockSubspace := subspace.FromBytes(prefix.Pack()).Sub("locks")
		v := r.Get(lockSubspace.Pack(tuple.Tuple{"exclusive"})).MustGet()
		return ParseExpiry(v), nil // We need to export ParseExpiry or duplicate logic
	})
	require.NoError(t, err)
	ts1 = val1.(int64)

	// Wait for heartbeat (2s interval)
	time.Sleep(3 * time.Second)

	// Check timestamp again -> should be higher
	var ts2 int64
	val2, err := db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		lockSubspace := subspace.FromBytes(prefix.Pack()).Sub("locks")
		v := r.Get(lockSubspace.Pack(tuple.Tuple{"exclusive"})).MustGet()
		return ParseExpiry(v), nil
	})
	require.NoError(t, err)
	ts2 = val2.(int64)

	require.Greater(t, ts2, ts1, "Heartbeat should have updated timestamp")

	lm.Unlock(sqlite3vfs.LockNone)
}

func TestLockManager_IsLocked_EdgeCases(t *testing.T) {
	db := newTestDBLocal(t)
	prefix := tuple.Tuple{"test", "islocked_" + uuid.New().String()}
	lm := NewLockManager(db, prefix, DefaultConfig())

	// Test 1: Empty -> False
	assert.False(t, lm.isLocked(nil))
	assert.False(t, lm.isLocked([]byte{}))

	// Test 2: Valid 8-byte Expiry -> True (future)
	future := make([]byte, 8)
	binary.BigEndian.PutUint64(future, uint64(time.Now().Add(time.Hour).UnixNano()))
	assert.True(t, lm.isLocked(future))

	// Test 3: Valid 8-byte Expiry -> False (past)
	past := make([]byte, 8)
	binary.BigEndian.PutUint64(past, uint64(time.Now().Add(-time.Hour).UnixNano()))
	assert.False(t, lm.isLocked(past))

	// Test 4: Valid 16-byte -> True (future)
	future16 := make([]byte, 16)
	binary.BigEndian.PutUint64(future16, uint64(time.Now().Add(time.Hour).UnixNano()))
	assert.True(t, lm.isLocked(future16))

	// Test 5: Invalid/Garbage -> True (Assume locked for safety)
	assert.True(t, lm.isLocked([]byte("garbage")))
}

func TestLockManager_RefreshLegacy(t *testing.T) {
	db := newTestDBLocal(t)
	prefix := tuple.Tuple{"test", "legacy_" + uuid.New().String()}
	lm := NewLockManager(db, prefix, DefaultConfig())
	// 1. Manually create a Legacy Shared Lock (8 bytes)
	expiry := time.Now().Add(time.Minute).UnixNano()
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(expiry))

	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(subspace.FromBytes(prefix.Pack()).Sub("locks", "shared").Pack(tuple.Tuple{lm.id}), val)
		return nil, nil
	})
	require.NoError(t, err)

	// 2. Set local state to Shared so Refresh works
	lm.currentLevel = LockLevel(sqlite3vfs.LockShared)

	// 3. Trigger Refresh
	lm.refreshLease()

	// 4. Verify it is STILL 8 bytes (Legacy format preserved/updated)
	savedVal, err := db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		// return value bytes
		return r.Get(subspace.FromBytes(prefix.Pack()).Sub("locks", "shared").Pack(tuple.Tuple{lm.id})).MustGet(), nil
	})
	require.NoError(t, err)

	bytes := savedVal.([]byte)
	assert.Equal(t, 8, len(bytes), "Legacy lock should remain 8 bytes after refresh")

	// 5. Cleanup
	lm.currentLevel = LockLevel(sqlite3vfs.LockNone) // Prevent unlock logic confusion if we called Unlock
}

func TestLockManager_SharedBlockedByPending(t *testing.T) {
	db := newTestDBLocal(t)
	prefix := tuple.Tuple{"test", "blocked_pending_" + uuid.New().String()}
	lm := NewLockManager(db, prefix, DefaultConfig())
	ctx := context.Background()

	// 1. Manually place PENDING lock
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		expiry := time.Now().Add(time.Minute).UnixNano()
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, uint64(expiry))
		tr.Set(subspace.FromBytes(prefix.Pack()).Sub("locks").Pack(tuple.Tuple{"pending"}), val)
		return nil, nil
	})
	require.NoError(t, err)

	// 2. Try to acquire SHARED -> Should fail/timeout
	ctxShort, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err = lm.Lock(ctxShort, sqlite3vfs.LockShared, 0)
	require.Error(t, err)
	assert.Equal(t, LockNone, lm.currentLevel)
}

func TestLockManager_ReservedBlocked(t *testing.T) {
	db := newTestDBLocal(t)
	prefix := tuple.Tuple{"test", "blocked_reserved_" + uuid.New().String()}
	lm1 := NewLockManager(db, prefix, DefaultConfig())
	lm2 := NewLockManager(db, prefix, DefaultConfig())
	ctx := context.Background()

	// 1. LM1 acquires RESERVED (via Shared)
	require.NoError(t, lm1.Lock(ctx, sqlite3vfs.LockShared, 0))
	require.NoError(t, lm1.Lock(ctx, sqlite3vfs.LockReserved, 0))

	// 2. LM2 acquires SHARED
	require.NoError(t, lm2.Lock(ctx, sqlite3vfs.LockShared, 0))

	// 3. LM2 tries RESERVED -> Should fail/timeout (Only one Reserved allowed)
	ctxShort, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err := lm2.Lock(ctxShort, sqlite3vfs.LockReserved, 0)
	require.Error(t, err)
}
