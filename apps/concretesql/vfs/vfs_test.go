package vfs

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/psanford/sqlite3vfs"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concretesql/store"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestVFS_CRUD(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	fdb.MustAPIVersion(620)
	db := fdb.MustOpenDefault()

	fsName := "test_vfs_crud"
	prefix := tuple.Tuple{fsName}

	// Reset
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(subspace.FromBytes(prefix.Pack()))
		return nil, nil
	})

	v := &FDBVFS{
		db:     db,
		fsName: fsName,
		config: store.DefaultConfig(),
	}

	// 1. Access (Empty) -> False
	exists, err := v.Access("file1.db", sqlite3vfs.AccessExists)
	require.NoError(t, err)
	require.False(t, exists)

	// 2. Open (Create)
	f, _, err := v.Open("file1.db", sqlite3vfs.OpenReadWrite|sqlite3vfs.OpenCreate)
	require.NoError(t, err)
	require.NotNil(t, f)

	// Write something to make it exist physically
	// Just opening doesn't create keys in FDB until we Write/Sync in current implementation
	// (NewPageStore doesn't touch DB, NewLockManager doesn't).
	// So we need to write?
	// Actually NewFile calls CurrentVersion. But doesn't write.
	// So Access will still be false?
	// Access checks if any key exists in range.
	// So yes, we need to write.

	file := f.(*File)
	_, err = file.WriteAt([]byte("data"), 0)
	require.NoError(t, err)
	err = file.Sync(0)
	require.NoError(t, err)
	file.Close()

	// 3. Access (Exists) -> True
	exists, err = v.Access("file1.db", sqlite3vfs.AccessExists)
	require.NoError(t, err)
	require.True(t, exists)

	// 4. FullPathname
	require.Equal(t, "file1.db", v.FullPathname("file1.db"))

	// 5. Delete
	err = v.Delete("file1.db", true)
	require.NoError(t, err)

	// 6. Access (Deleted) -> False
	exists, err = v.Access("file1.db", sqlite3vfs.AccessExists)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestVFS_Register(t *testing.T) {
	// Just check if it returns nil (or error if already registered)
	// We might clash with other tests?
	// FS Name unique
	fdbtest.SkipIfFDBUnavailable(t)
	db := fdb.MustOpenDefault()
	err := Register("test_vfs_reg", db, store.DefaultConfig())
	// It might succeed or fail if run multiple times.
	// Since tests run in parallel or sequential in same binary, safe to name unique.
	require.NoError(t, err)
}
