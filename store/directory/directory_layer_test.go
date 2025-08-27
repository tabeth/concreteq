package directory_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concreteq/store/directory"
)

var (
	db_once sync.Once
	db fdb.Database
	testRoot directory.Directory
)

func setupAllTests(t *testing.T) (fdb.Database, directory.Directory) {
	db_once.Do(func() {
		fdb.MustAPIVersion(730)
		var err error
		db, err = fdb.OpenDefault()
		require.NoError(t, err)

		testDirName := fmt.Sprintf("dir_pkg_test_%d", time.Now().UnixNano())
		testRoot, err = directory.CreateOrOpen(db, []string{testDirName}, nil)
		require.NoError(t, err)
	})
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		subdirs, err := testRoot.List(tr, []string{}, directory.ListOptions{})
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

func TestDirectory_CoreOperations(t *testing.T) {
	db, root := setupAllTests(t)

	_, err := root.Create(db, []string{"a"}, nil)
	require.NoError(t, err)
	exists, err := root.Exists(db, []string{"a"})
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestDirectorySubspace_CreatePrefix(t *testing.T) {
	db, root := setupAllTests(t)

	// The root from setupAllTests doesn't allow manual prefixes
	_, err := root.CreatePrefix(db, []string{"sub"}, nil, []byte("prefix"))
	assert.Error(t, err)
}

func TestDirectory_Remove(t *testing.T) {
	db, root := setupAllTests(t)

	_, err := root.Create(db, []string{"b"}, nil)
	require.NoError(t, err)
	removed, err := root.Remove(db, []string{"b"})
	require.NoError(t, err)
	assert.True(t, removed)
	exists, err := root.Exists(db, []string{"b"})
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestDirectory_Move(t *testing.T) {
	db, root := setupAllTests(t)

	_, err := root.Create(db, []string{"c"}, nil)
	require.NoError(t, err)
	_, err = root.Create(db, []string{"d"}, nil)
	require.NoError(t, err)
	_, err = root.Move(db, []string{"c"}, []string{"d", "c_moved"})
	require.NoError(t, err)
	exists, err := root.Exists(db, []string{"c"})
	require.NoError(t, err)
	assert.False(t, exists)
	exists, err = root.Exists(db, []string{"d", "c_moved"})
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestDirectory_Pagination(t *testing.T) {
	db, root := setupAllTests(t)

	dirCount := 10
	for i := 0; i < dirCount; i++ {
		_, err := root.Create(db, []string{fmt.Sprintf("page_test_%02d", i)}, nil)
		require.NoError(t, err)
	}

	// List All
	dirs, err := root.List(db, []string{}, directory.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, dirs, dirCount)

	// List with Limit
	limit := 5
	dirs, err = root.List(db, []string{}, directory.ListOptions{Limit: limit})
	require.NoError(t, err)
	assert.Len(t, dirs, limit)

	// Paginate Multiple Pages
	limit = 4
	page1, err := root.List(db, []string{}, directory.ListOptions{Limit: limit})
	require.NoError(t, err)
	assert.Len(t, page1, limit)
	assert.Equal(t, "page_test_00", page1[0])
	assert.Equal(t, "page_test_03", page1[3])

	afterToken := page1[len(page1)-1]
	page2, err := root.List(db, []string{}, directory.ListOptions{Limit: limit, After: afterToken})
	require.NoError(t, err)
	assert.Len(t, page2, limit)
	assert.Equal(t, "page_test_04", page2[0])
	assert.Equal(t, "page_test_07", page2[3])

	afterToken = page2[len(page2)-1]
	page3, err := root.List(db, []string{}, directory.ListOptions{Limit: limit, After: afterToken})
	require.NoError(t, err)
	assert.Len(t, page3, 2)
	assert.Equal(t, "page_test_08", page3[0])
}

func TestDirectory_Pagination_Reverse(t *testing.T) {
	db, root := setupAllTests(t)

	dirCount := 10
	for i := 0; i < dirCount; i++ {
		_, err := root.Create(db, []string{fmt.Sprintf("reverse_page_test_%02d", i)}, nil)
		require.NoError(t, err)
	}

	// List in reverse with a limit
	limit := 4
	page1, err := root.List(db, []string{}, directory.ListOptions{Limit: limit, Reverse: true})
	require.NoError(t, err)
	assert.Len(t, page1, limit)
	assert.Equal(t, "reverse_page_test_09", page1[0])
	assert.Equal(t, "reverse_page_test_06", page1[3])

	// Get the previous page (which is the next page when reading in reverse)
	beforeToken := page1[len(page1)-1]
	page2, err := root.List(db, []string{}, directory.ListOptions{Limit: limit, Before: beforeToken, Reverse: true})
	require.NoError(t, err)
	assert.Len(t, page2, limit)
	assert.Equal(t, "reverse_page_test_05", page2[0])
	assert.Equal(t, "reverse_page_test_02", page2[3])

	// Test error case
	_, err = root.List(db, []string{}, directory.ListOptions{After: "a", Before: "b"})
	assert.Error(t, err)
}

func newIsolatedDirectoryLayer(t *testing.T) directory.Directory {
	t.Helper()
	prefix := []byte(fmt.Sprintf("isolated_test_%d", time.Now().UnixNano()))
	nodeSS := subspace.FromBytes(append(prefix, []byte("_nodes")...))
	contentSS := subspace.FromBytes(append(prefix, []byte("_content")...))
	return directory.NewDirectoryLayer(nodeSS, contentSS, true)
}

func TestDirectoryLayer_Errors(t *testing.T) {
	db, err := fdb.OpenDefault()
	require.NoError(t, err)

	dl := newIsolatedDirectoryLayer(t)

	// Test incompatible layer
	_, err = dl.Create(db, []string{"layer_test"}, []byte("layer1"))
	require.NoError(t, err)
	_, err = dl.Open(db, []string{"layer_test"}, []byte("layer2"))
	assert.Error(t, err)

	// Test move destination is subdir
	_, err = dl.Move(db, []string{"a"}, []string{"a", "b"})
	assert.Error(t, err)

	// Test createOrOpen with allowCreate=false and directory does not exist
	_, err = dl.Open(db, []string{"non_existent"}, nil)
	assert.Error(t, err)

	// Test createOrOpen with allowOpen=false and directory exists
	_, err = dl.Create(db, []string{"dir_exists"}, nil)
	require.NoError(t, err)
	_, err = dl.Create(db, []string{"dir_exists"}, nil)
	assert.Error(t, err)

	// Test Move with source not existing
	_, err = dl.Move(db, []string{"non_existent_src"}, []string{"dst"})
	assert.Error(t, err)

	// Test Move with destination existing
	_, err = dl.Create(db, []string{"src_exists"}, nil)
	require.NoError(t, err)
	_, err = dl.Create(db, []string{"dst_exists"}, nil)
	require.NoError(t, err)
	_, err = dl.Move(db, []string{"src_exists"}, []string{"dst_exists"})
	assert.Error(t, err)

	// Test Move with parent of destination not existing
	_, err = dl.Move(db, []string{"src_exists"}, []string{"non_existent_parent", "dst"})
	assert.Error(t, err)

	// Test List on a directory that does not exist
	_, err = dl.List(db, []string{"non_existent_list"}, directory.ListOptions{})
	assert.Error(t, err)
}


func TestDirectoryLayer_MoveRemovePartitions(t *testing.T) {
	db, err := fdb.OpenDefault()
	require.NoError(t, err)

	// Create a clean directory layer for the test
	prefix := []byte(fmt.Sprintf("move_remove_partitions_%d", time.Now().UnixNano()))
	nodeSS := subspace.FromBytes(append(prefix, []byte("_nodes")...))
	contentSS := subspace.FromBytes(append(prefix, []byte("_content")...))
	dl := directory.NewDirectoryLayer(nodeSS, contentSS, true)

	// Create two partitions
	p1, err := dl.CreatePrefix(db, []string{"p1"}, []byte("partition"), []byte("prefix1"))
	require.NoError(t, err)
	_, err = dl.CreatePrefix(db, []string{"p2"}, []byte("partition"), []byte("prefix2"))
	require.NoError(t, err)

	// Try to move between partitions (should fail)
	_, err = p1.MoveTo(db, []string{"p2", "moved"})
	assert.Error(t, err)

	// Remove a partition
	removed, err := dl.Remove(db, []string{"p1"})
	require.NoError(t, err)
	assert.True(t, removed)
}

func TestDirectoryLayer_CreatePrefix(t *testing.T) {
	db, _ := setupAllTests(t)
	dl := newIsolatedDirectoryLayer(t)

	_, err := dl.CreatePrefix(db, []string{"prefix_test"}, nil, []byte("prefix123"))
	require.NoError(t, err)

	exists, err := dl.Exists(db, []string{"prefix_test"})
	require.NoError(t, err)
	assert.True(t, exists)

	// Test creating a prefix that is already in use
	_, err = dl.CreatePrefix(db, []string{"prefix_test_2"}, nil, []byte("prefix123"))
	assert.Error(t, err)
}

func TestDirectoryLayer_IsPrefixFree(t *testing.T) {
	db, _ := setupAllTests(t)
	dl := newIsolatedDirectoryLayer(t)

	_, err := dl.CreatePrefix(db, []string{"prefix_test"}, nil, []byte("prefix123"))
	require.NoError(t, err)

	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		isFree, err := directory.IsPrefixFreeForTesting(dl, tr, []byte("prefix123"))
		require.NoError(t, err)
		assert.False(t, isFree)

		isFree, err = directory.IsPrefixFreeForTesting(dl, tr, []byte("prefix124"))
		require.NoError(t, err)
		assert.True(t, isFree)
		return nil, nil
	})
	require.NoError(t, err)
}

func TestDirectoryLayer_MoveToRoot(t *testing.T) {
	db, err := fdb.OpenDefault()
	require.NoError(t, err)

	dl := newIsolatedDirectoryLayer(t)
	_, err = dl.MoveTo(db, []string{"anywhere"})
	assert.Error(t, err)
}

func TestDirectoryLayer_RemoveNonExistent(t *testing.T) {
	db, err := fdb.OpenDefault()
	require.NoError(t, err)

	dl := newIsolatedDirectoryLayer(t)
	// Remove a non-existent directory
	removed, err := dl.Remove(db, []string{"non_existent"})
	require.NoError(t, err)
	assert.False(t, removed)
}

func TestDirectory_Partition(t *testing.T) {
	db, root := setupAllTests(t)

	partition, err := root.Create(db, []string{"partition_test"}, []byte("partition"))
	require.NoError(t, err)

	_, err = partition.Create(db, []string{"a"}, nil)
	require.NoError(t, err)

	exists, err := partition.Exists(db, []string{"a"})
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = root.Exists(db, []string{"partition_test", "a"})
	require.NoError(t, err)
	assert.True(t, exists)
}
