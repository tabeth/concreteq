package directory

import (
	"fmt"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowSize(t *testing.T) {
	assert.Equal(t, int64(64), windowSize(0))
	assert.Equal(t, int64(64), windowSize(254))
	assert.Equal(t, int64(1024), windowSize(255))
	assert.Equal(t, int64(1024), windowSize(65534))
	assert.Equal(t, int64(8192), windowSize(65535))
}

func TestHCA_Allocate(t *testing.T) {
	fdb.MustAPIVersion(730)
	db, err := fdb.OpenDefault()
	require.NoError(t, err)

	prefix := []byte(fmt.Sprintf("alloc_test_%d", time.Now().UnixNano()))
	s := subspace.FromBytes(prefix)
	hca := newHCA(s)

	allocated := make(map[string]struct{})

	for i := 0; i < 100; i++ {
		ss, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			return hca.allocate(tr, s)
		})
		require.NoError(t, err)
		sub, ok := ss.(subspace.Subspace)
		require.True(t, ok)
		key := string(sub.Bytes())
		_, exists := allocated[key]
		assert.False(t, exists)
		allocated[key] = struct{}{}
	}
}

func TestHCA_Allocate_WindowAdvancement(t *testing.T) {
	fdb.MustAPIVersion(730)
	db, err := fdb.OpenDefault()
	require.NoError(t, err)

	prefix := []byte(fmt.Sprintf("alloc_test_win_%d", time.Now().UnixNano()))
	s := subspace.FromBytes(prefix)
	hca := newHCA(s)

	// Allocate enough times to advance the window
	// windowSize(0) is 64, so we need to allocate at least 32 times
	for i := 0; i < 35; i++ {
		_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			return hca.allocate(tr, s)
		})
		require.NoError(t, err)
	}

	// The window should have advanced. We can check this by looking at the counters.
	_, err = db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		rr := rtr.GetRange(hca.counters, fdb.RangeOptions{Limit: 1, Reverse: true})
		kvs, err := rr.GetSliceWithError()
		require.NoError(t, err)
		require.Len(t, kvs, 1)
		tup, err := hca.counters.Unpack(kvs[0].Key)
		require.NoError(t, err)
		start := tup[0].(int64)
		assert.Equal(t, int64(64), start)
		return nil, nil
	})
	require.NoError(t, err)
}
