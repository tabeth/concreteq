package directory_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
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
