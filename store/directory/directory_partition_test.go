package directory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectoryPartition(t *testing.T) {
	db, root := setupAllTests(t)

	// Create a directory with a prefix to get a directoryPartition
	// We need to use a root that allows manual prefixes.
	ds, ok := root.(DirectorySubspace)
	require.True(t, ok)
	// Create a new subspace for the partition to avoid conflicts
	partitionRoot, err := ds.Create(db, []string{"partition_root"}, nil)
	require.NoError(t, err)
	prefix := []byte{byte(time.Now().UnixNano())}
	partition, err := NewDirectoryLayer(partitionRoot, partitionRoot, true).CreatePrefix(db, []string{"partition_test"}, []byte("partition"), prefix)
	require.NoError(t, err)

	assert.Equal(t, []byte("partition"), partition.GetLayer())

	// Test the panicking methods
	assert.Panics(t, func() { partition.Sub("test") })
	assert.Panics(t, func() { partition.Bytes() })
	assert.Panics(t, func() { partition.Pack(nil) })
	assert.Panics(t, func() { _, _ = partition.PackWithVersionstamp(nil) })
	assert.Panics(t, func() { _, _ = partition.Unpack(nil) })
	assert.Panics(t, func() { partition.Contains(nil) })
	assert.Panics(t, func() { partition.FDBKey() })
	assert.Panics(t, func() { _, _ = partition.FDBRangeKeys() })
	assert.Panics(t, func() { _, _ = partition.FDBRangeKeySelectors() })

	// Test Exists
	exists, err := partition.Exists(db, []string{})
	require.NoError(t, err)
	assert.True(t, exists)

	// Test Remove
	removed, err := partition.Remove(db, []string{})
	require.NoError(t, err)
	assert.True(t, removed)
}
