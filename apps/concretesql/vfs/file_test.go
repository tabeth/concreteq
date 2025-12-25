package vfs

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	_ "github.com/mattn/go-sqlite3"
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

	ps := store.NewPageStore(db, prefix)
	lm := store.NewLockManager(db, prefix)

	f, err := NewFile("test.db", ps, lm)
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
