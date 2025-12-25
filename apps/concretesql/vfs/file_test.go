package vfs

import (
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concretesql/store"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

// Helper to create test file
func newTestFile(t *testing.T) *File {
	fdbtest.SkipIfFDBUnavailable(t)

	// We need sharedfdb or direct connection?
	// driver uses `sharedfdb.OpenDB(620)`. Store tests use `NewTestDB`.
	// Let's implement minimal DB access here or import from store_test if exported.
	// Since `store.NewTestDB` is in `_test.go`, we can't import it.
	// We should copy the initialization or assume integration environment.

	fdb.MustAPIVersion(620)
	db := fdb.MustOpenDefault()

	prefix := tuple.Tuple{"test", "vfs_file"}
	// Clear
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})

	ps := store.NewPageStore(db, prefix, store.DefaultConfig())
	lm := store.NewLockManager(db, prefix, store.DefaultConfig())

	f, err := NewFile("test.db", ps, lm, 0)
	require.NoError(t, err)

	return f
}

func TestFile_Truncate(t *testing.T) {
	f := newTestFile(t)
	defer f.Close()

	err := f.Truncate(100)
	require.NoError(t, err)

	sz, err := f.FileSize()
	require.NoError(t, err)
	require.Equal(t, int64(100), sz)
}

func TestFile_CheckReservedLock(t *testing.T) {
	f := newTestFile(t)
	defer f.Close()

	isReserved, err := f.CheckReservedLock()
	require.NoError(t, err)
	require.False(t, isReserved)

	// TODO: Acquire reserved lock and check?
	// LockManager handles it.
}

func TestFile_OptimisticLocking(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	fdb.MustAPIVersion(620)
	db := fdb.MustOpenDefault()
	prefix := tuple.Tuple{"test", "vfs_file_opt"}

	// Clear
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})

	ps := store.NewPageStore(db, prefix, store.DefaultConfig())
	lm := store.NewLockManager(db, prefix, store.DefaultConfig())

	f, err := NewFile("test.db", ps, lm, 0)
	require.NoError(t, err)
	defer f.Close()

	// 1. Initial State. Version 0.
	data := make([]byte, f.SectorSize())
	f.WriteAt(data, 0)

	// 2. Simulate concurrent modification
	ctx := context.Background()
	err = ps.SetVersionAndSize(ctx, 100, 4096, "", 0)
	require.NoError(t, err)

	// 3. Sync
	// Should detect version mismatch (0 vs 100) and return conflict error.
	// 3. Sync
	// Should detect version mismatch (0 vs 100) and return conflict error.
	err = f.Sync(0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflict")
}

func TestFile_ReadWrite(t *testing.T) {
	f := newTestFile(t)
	defer f.Close()

	data := []byte("hello world")
	off := int64(0)

	// Write
	n, err := f.WriteAt(data, off)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	// Sync
	err = f.Sync(0)
	require.NoError(t, err)

	// Read back
	buf := make([]byte, len(data))
	n, err = f.ReadAt(buf, off)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, buf)

	// Partial Read
	buf2 := make([]byte, 5)
	n, err = f.ReadAt(buf2, off)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), buf2)
}

func TestFile_Locking(t *testing.T) {
	f := newTestFile(t)
	defer f.Close()

	// 1. Shared
	err := f.Lock(sqlite3vfs.LockShared)
	require.NoError(t, err)

	// 2. Reserved
	err = f.Lock(sqlite3vfs.LockReserved)
	require.NoError(t, err)

	// 3. Exclusive
	err = f.Lock(sqlite3vfs.LockExclusive)
	require.NoError(t, err)

	// 4. Unlock to Shared
	err = f.Unlock(sqlite3vfs.LockShared)
	require.NoError(t, err)

	// 5. Unlock completely
	err = f.Unlock(sqlite3vfs.LockNone)
	require.NoError(t, err)
}

func TestFile_FileControl(t *testing.T) {
	f := newTestFile(t)
	defer f.Close()

	// Test PRAGMA interception
	args := []string{"pragma", "concrete_request_id", "req-123"}
	// The cast in FileControl expects *[]string
	ok, err := f.FileControl(14, &args) // 14 == SQLITE_FCNTL_PRAGMA
	require.NoError(t, err)
	require.True(t, ok)

	// Verify unknown pragma ignored
	args2 := []string{"pragma", "journal_mode", "wal"}
	ok, err = f.FileControl(14, &args2)
	require.NoError(t, err)
	require.False(t, ok) // Handled? Actually returns false if not handled
}

func TestFile_PageSizeSnoop(t *testing.T) {
	f := newTestFile(t)
	defer f.Close()

	// Current pageSize default 4096 (or 0 initially)
	// We want to write a header that sets it to 1024.
	// SQLite header: 100 bytes.
	// Offset 16 (2 bytes) is page size.
	// 1024 = 0x0400

	header := make([]byte, 100)
	header[16] = 0x04
	header[17] = 0x00

	// Write header at 0
	_, err := f.WriteAt(header, 0)
	require.NoError(t, err)

	// Verify pageSize updated in File struct
	// We need to access f.pageSize, but it's private.
	// We can check SectorSize() if it returns pageSize?
	require.Equal(t, int64(1024), f.SectorSize())
}

func TestFile_DeviceCharacteristics(t *testing.T) {
	f := newTestFile(t)
	defer f.Close()
	dc := f.DeviceCharacteristics()
	require.Equal(t, sqlite3vfs.DeviceCharacteristic(0), dc)
}

func TestFile_NewWithOptions(t *testing.T) {
	// Setup
	fdbtest.SkipIfFDBUnavailable(t)
	fdb.MustAPIVersion(620)
	db := fdb.MustOpenDefault()
	prefix := tuple.Tuple{"test", "vfs_file_opts"}
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})
	ps := store.NewPageStore(db, prefix, store.DefaultConfig())
	lm := store.NewLockManager(db, prefix, store.DefaultConfig())

	// 1. New DB with InitPageSize 8192
	f1, err := NewFile("test_opts.db", ps, lm, 8192)
	require.NoError(t, err)
	require.Equal(t, int64(8192), f1.SectorSize())

	// Create some state so it isn't treated as "New/Empty"
	_, err = f1.WriteAt([]byte{1}, 0)
	require.NoError(t, err)
	err = f1.Sync(0)
	require.NoError(t, err)

	f1.Close()

	// 2. Open Existing DB (should respect established size 8192 even if we ask for 4096)
	// New instance needed
	f2, err := NewFile("test_opts.db", ps, lm, 4096)
	require.NoError(t, err)
	defer f2.Close()
	// Should be 8192 because it picks up state from DB (and Version > 0)
	require.Equal(t, int64(8192), f2.SectorSize())
}

func TestFile_SyncClean(t *testing.T) {
	f := newTestFile(t)
	defer f.Close()
	// Sync without writes
	err := f.Sync(0)
	require.NoError(t, err)
}
