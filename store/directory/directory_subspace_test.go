package directory

import (
	"testing"

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
