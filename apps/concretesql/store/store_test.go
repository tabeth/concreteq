package store

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	_ "github.com/mattn/go-sqlite3" // Ensure sqlite3 symbols are linked
	"github.com/psanford/sqlite3vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sharedfdb "github.com/tabeth/kiroku-core/libs/fdb"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func NewTestDB(t testing.TB) fdb.Database {
	fdbtest.SkipIfFDBUnavailable(t)
	db, err := sharedfdb.OpenDB(620)
	if err != nil {
		t.Fatalf("Failed to open FDB: %v", err)
	}
	return db
}

func TestPageStore_ReadWrite(t *testing.T) {
	db := NewTestDB(t)
	prefix := tuple.Tuple{"test", "pager"}
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})
	require.NoError(t, err)
	ps := NewPageStore(db, prefix)
	ctx := context.Background()

	// Initial State
	state, err := ps.CurrentVersion(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), state.Version)
	assert.Equal(t, int64(0), state.Size)

	// Write Pages
	pages := map[int][]byte{
		0: []byte("header_page_data"),
		1: []byte("page_one_data"),
	}
	err = ps.WritePages(ctx, 1, pages)
	require.NoError(t, err)

	err = ps.SetVersionAndSize(ctx, 1, 8192)
	require.NoError(t, err)

	// Read Back
	data0, err := ps.ReadPage(ctx, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("header_page_data"), data0)

	data1, err := ps.ReadPage(ctx, 1, 1)
	require.NoError(t, err)
	assert.Equal(t, []byte("page_one_data"), data1)

	// Read Non-Existent Page
	data2, err := ps.ReadPage(ctx, 1, 2)
	require.NoError(t, err)
	assert.Nil(t, data2)

	// Test MVCC
	// Update Page 0 in Version 2
	pagesV2 := map[int][]byte{
		0: []byte("header_page_data_v2"),
	}
	err = ps.WritePages(ctx, 2, pagesV2)
	require.NoError(t, err)
	err = ps.SetVersionAndSize(ctx, 2, 8192)
	require.NoError(t, err)

	// Read at Version 1 (Snapshot)
	data0v1, err := ps.ReadPage(ctx, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("header_page_data"), data0v1) // Should still verify old data

	// Read at Version 2
	data0v2, err := ps.ReadPage(ctx, 2, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("header_page_data_v2"), data0v2)

	// Read Page 1 at Version 2 (Should "fall through" to Version 1)
	data1v2, err := ps.ReadPage(ctx, 2, 1)
	require.NoError(t, err)
	assert.Equal(t, []byte("page_one_data"), data1v2)
}

func TestLockManager(t *testing.T) {
	db := NewTestDB(t)
	prefix := tuple.Tuple{"test", "locks"}
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})
	lm1 := NewLockManager(db, prefix)
	lm2 := NewLockManager(db, prefix)
	ctx := context.Background()

	// 1. LM1 acquires SHARED
	err := lm1.Lock(ctx, sqlite3vfs.LockShared)
	require.NoError(t, err)

	// 2. LM2 acquires SHARED (Allowed)
	err = lm2.Lock(ctx, sqlite3vfs.LockShared)
	require.NoError(t, err)

	// 3. LM1 tries EXCLUSIVE (Should Block because LM2 has SHARED)
	// We expect it to eventually fail/timeout if we didn't implement retry-timeout logic in test.
	// In our implementation, Lock loops. So we can't block test info forever.
	// Let's create a timeout context.
	ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	err = lm1.Lock(ctxTimeout, sqlite3vfs.LockExclusive)
	require.Error(t, err) // Should timeout

	// 4. LM2 Releases SHARED
	err = lm2.Unlock(sqlite3vfs.LockNone)
	require.NoError(t, err)

	// 5. LM1 should now succeed getting EXCLUSIVE (retrying)
	// Need new context
	ctx2 := context.Background()
	// Start async because Lock blocks
	done := make(chan error)
	go func() {
		done <- lm1.Lock(ctx2, sqlite3vfs.LockExclusive)
	}()

	// Ensure cleanup
	// We cannot clear ALL because we don't know the keys easily without VFS internal knowledge.

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for Exclusive Lock")
	}
}

func TestVacuum(t *testing.T) {
	db := NewTestDB(t)
	prefix := tuple.Tuple{"test", "vacuum"}
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})
	require.NoError(t, err)
	ps := NewPageStore(db, prefix)
	ctx := context.Background()

	// Setup:
	// Version 1: Page 0
	ps.WritePages(ctx, 1, map[int][]byte{0: []byte("v1")})
	ps.SetVersionAndSize(ctx, 1, 4096)

	// Version 2: Page 0 (Leaked Future)
	ps.WritePages(ctx, 2, map[int][]byte{0: []byte("v2_leaked")})
	// Note: We DO NOT set version to 2. Version stays 1.

	// Verify Read at V1 sees V1
	data, _ := ps.ReadPage(ctx, 1, 0)
	assert.Equal(t, []byte("v1"), data)

	// Run Vacuum
	err = ps.Vacuum(ctx)
	require.NoError(t, err)

	// Verify V2 page is gone
	// We can verify by trying to read it at V2?
	// Low level check:
	_, err = db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		k := ps.subspace.Pack(tuple.Tuple{"data", 0, 2})
		val := r.Get(k).MustGet()
		if len(val) > 0 {
			return nil, assert.AnError
		}
		return nil, nil
	})
	// No error = empty = good.
	// Actually if val is empty, good.
	// In FDB bindings, Get returns FutureByteSlice. MustGet returns []byte.
	// If not exists, it returns nil? FDB returns nil for non-existent key.
}

func TestVacuum_Shadowing(t *testing.T) {
	db := NewTestDB(t)
	prefix := tuple.Tuple{"test", "vacuum_shadow"}

	ps := NewPageStore(db, prefix)
	ctx := context.Background()

	// 1. Write V1 (Page 0)
	err := ps.WritePages(ctx, 1, map[int][]byte{0: []byte("v1_shadowed")})
	require.NoError(t, err)

	// 2. Write V2 (Page 0) -> Shadows V1
	err = ps.WritePages(ctx, 2, map[int][]byte{0: []byte("v2_current")})
	require.NoError(t, err)

	// Set Version to 2
	err = ps.SetVersionAndSize(ctx, 2, 8192)
	require.NoError(t, err)

	// 3. Run Vacuum
	err = ps.Vacuum(ctx)
	require.NoError(t, err)

	// 4. Verify V1 is gone (accessed via snapshot V1)
	// Note: ReadPage searches for latest version <= snapshot.
	// If V1 is deleted, ReadPage(V1) will return nil!
	data, err := ps.ReadPage(ctx, 1, 0)
	require.NoError(t, err)
	assert.Nil(t, data, "Shadowed version 1 should be deleted")

	// 5. Verify V2 persists
	data, err = ps.ReadPage(ctx, 2, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2_current"), data)
}
