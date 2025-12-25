package store

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
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

	lm1 := NewLockManager(db, prefix) // "Owner" (Simulated Crash)
	lm2 := NewLockManager(db, prefix) // "Stealer"
	ctx := context.Background()

	// 1. LM1 acquires Exclusive Lock
	err := lm1.Lock(ctx, sqlite3vfs.LockExclusive)
	require.NoError(t, err)

	// 2. LM2 tries to acquire -> Should Block
	ctxShort, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	err = lm2.Lock(ctxShort, sqlite3vfs.LockShared)
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

	err = lm2.Lock(ctx2, sqlite3vfs.LockExclusive)
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

	lm := NewLockManager(db, prefix)
	ctx := context.Background()

	err := lm.Lock(ctx, sqlite3vfs.LockExclusive)
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
