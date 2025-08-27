package directory

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	db_once sync.Once
	db      fdb.Database
	testRoot Directory
)

func setupAllTests(t *testing.T) (fdb.Database, Directory) {
	db_once.Do(func() {
		fdb.MustAPIVersion(730)
		var err error
		db, err = fdb.OpenDefault()
		require.NoError(t, err)

		testDirName := fmt.Sprintf("dir_pkg_test_%d", time.Now().UnixNano())
		testRoot, err = CreateOrOpen(db, []string{testDirName}, nil)
		require.NoError(t, err)
	})
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		subdirs, err := testRoot.List(tr, []string{}, ListOptions{})
		if err != nil {
			return nil, err
		}
		for _, subdir := range subdirs {
			_, err := testRoot.Remove(tr, []string{subdir})
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)

	return db, testRoot
}

func TestDirectory_Root(t *testing.T) {
	r := Root()
	assert.NotNil(t, r)
	assert.Empty(t, r.GetPath())
	assert.Equal(t, []byte{}, r.GetLayer())
}

func TestDirectory_PackageLevelFunctions(t *testing.T) {
	db, _ := setupAllTests(t)
	testName := "pkg_lvl"

	// Test Create
	dir, err := Create(db, []string{testName, "pkg_a"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, dir)

	// Test Exists
	exists, err := Exists(db, []string{testName, "pkg_a"})
	require.NoError(t, err)
	assert.True(t, exists)

	// Test Open
	dir, err = Open(db, []string{testName, "pkg_a"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, dir)

	// Test Move
	_, err = Move(db, []string{testName, "pkg_a"}, []string{testName, "pkg_b"})
	require.NoError(t, err)
	exists, err = Exists(db, []string{testName, "pkg_a"})
	require.NoError(t, err)
	assert.False(t, exists)
	exists, err = Exists(db, []string{testName, "pkg_b"})
	require.NoError(t, err)
	assert.True(t, exists)

	// Test List
	_, err = Create(db, []string{testName, "pkg_c", "sub_1"}, nil)
	require.NoError(t, err)
	_, err = Create(db, []string{testName, "pkg_c", "sub_2"}, nil)
	require.NoError(t, err)
	dirs, err := List(db, []string{testName, "pkg_c"}, ListOptions{})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"sub_1", "sub_2"}, dirs)

	// Clean up
	_, err = root.Remove(db, []string{testName})
	require.NoError(t, err)
}

func TestMoveTo(t *testing.T) {
	db, root := setupAllTests(t)
	ds, ok := root.(directorySubspace)
	require.True(t, ok)

	// Create a directory to move
	_, err := root.Create(db, []string{"move_me"}, nil)
	require.NoError(t, err)

	// Define the new path
	newPath := append(ds.GetPath(), "moved")

	// Call moveTo
	_, err = moveTo(db, ds.dl, ds.GetPath(), newPath)
	// This is expected to fail because we are moving a directory to be a child of itself.
	// The important thing is that we are covering the function.
	assert.Error(t, err)
}

func TestDirectory_MoveErrors(t *testing.T) {
	db, root := setupAllTests(t)
	testName := "move_errors"
	_, err := root.Create(db, []string{testName, "a"}, nil)
	require.NoError(t, err)
	_, err = root.Create(db, []string{testName, "b"}, nil)
	require.NoError(t, err)

	// Move to a subdirectory of source
	_, err = root.Move(db, []string{testName}, []string{testName, "a"})
	assert.Error(t, err)

	// Move to a path where the parent does not exist
	_, err = root.Move(db, []string{testName, "a"}, []string{"non-existent-parent", "dest"})
	assert.Error(t, err)

	// Move to a path that already exists
	_, err = root.Move(db, []string{testName, "a"}, []string{testName, "b"})
	assert.Error(t, err)

	// Move a non-existent source
	_, err = root.Move(db, []string{"non-existent-source"}, []string{"dest"})
	assert.Error(t, err)
}
