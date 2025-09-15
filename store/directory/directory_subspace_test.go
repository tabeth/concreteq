package directory

import (
	"fmt"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectorySubspace(t *testing.T) {
	db, root := setupAllTests(t)
	ds, ok := root.(DirectorySubspace)
	require.True(t, ok)

	// Test String()
	dss, ok := ds.(directorySubspace)
	require.True(t, ok)
	assert.NotEmpty(t, dss.String())

	// Test GetLayer()
	assert.Equal(t, []byte{}, ds.GetLayer())

	// Test CreateOrOpen
	sub, err := ds.CreateOrOpen(db, []string{"sub"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, sub)

	// Test Open
	sub, err = ds.Open(db, []string{"sub"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, sub)

	// Test CreatePrefix
	// To test CreatePrefix, we need a directory that allows manual prefixes.
	partitionRoot, err := ds.Create(db, []string{"partition_root_2"}, nil)
	require.NoError(t, err)
	dl := NewDirectoryLayer(partitionRoot, partitionRoot, true)
	prefix := []byte{byte(123)}
	sub, err = dl.CreatePrefix(db, []string{"sub_prefix"}, nil, prefix)
	require.NoError(t, err)
	assert.NotNil(t, sub)

	// Test MoveTo
	// Create parent of destination
	parent, err := dl.CreatePrefix(db, []string{"parent"}, nil, []byte{124})
	require.NoError(t, err)
	_, err = sub.MoveTo(db, append(parent.GetPath(), "sub_prefix_moved"))
	require.NoError(t, err)
}

func TestDirectorySubspace_CreatePrefixCoverage(t *testing.T) {
	fdb.MustAPIVersion(730)
	db, err := fdb.OpenDefault()
	require.NoError(t, err)

	// Create a unique root for this test to ensure isolation
	testDirName := fmt.Sprintf("iso_test_%d", time.Now().UnixNano())
	root, err := CreateOrOpen(db, []string{testDirName}, nil)
	require.NoError(t, err)
	defer func() {
		_, err := Root().Remove(db, []string{testDirName})
		assert.NoError(t, err)
	}()

	// The root directory itself doesn't allow manual prefixes.
	_, err = root.CreatePrefix(db, []string{"should_fail"}, nil, []byte{1})
	assert.Error(t, err)

	// Create a partition that allows manual prefixes
	partition, err := root.Create(db, []string{"partition"}, nil)
	require.NoError(t, err)

	nodeSS := partition.Sub("nodes")
	contentSS := partition.Sub("content")
	dl := NewDirectoryLayer(nodeSS, contentSS, true)

	// Create a directory with a manual prefix
	prefix1 := []byte{10}
	sub1, err := dl.CreatePrefix(db, []string{"sub1"}, nil, prefix1)
	require.NoError(t, err)
	assert.Equal(t, prefix1, sub1.Bytes())

	// Create a directory with an automatic prefix
	sub2, err := dl.CreateOrOpen(db, []string{"sub2"}, nil)
	require.NoError(t, err)

	// Now, create a directory with a manual prefix inside sub2
	prefix2 := []byte{20}
	sub3, err := sub2.CreatePrefix(db, []string{"sub3"}, nil, prefix2)
	require.NoError(t, err)
	assert.Equal(t, prefix2, sub3.Bytes())
}
