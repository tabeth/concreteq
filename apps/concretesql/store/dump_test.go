package store_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/stretchr/testify/require"
	"github.com/tabeth/concretesql/store"
	sharedfdb "github.com/tabeth/kiroku-core/libs/fdb"
)

func TestDumpToWriter(t *testing.T) {
	// Setup FDB
	db, err := sharedfdb.OpenDB(620)
	require.NoError(t, err)

	prefix := tuple.Tuple{"test_dump", "db1"}
	cfg := store.DefaultConfig()
	ps := store.NewPageStore(db, prefix, cfg)

	// Cleanup
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(fdb.KeyRange{Begin: fdb.Key(prefix.Pack()), End: fdb.Key(append(prefix.Pack(), 0xFF))})
		return nil, nil
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Initialize Page Size
	pageSize := 4096
	err = ps.SetPageSize(ctx, pageSize)
	require.NoError(t, err)

	// Create dummy pages
	// Version 1: Write Page 1 and Page 3. (Page 2 sparse)
	page1 := make([]byte, pageSize)
	copy(page1, []byte("Page 1 Content"))

	page3 := make([]byte, pageSize)
	copy(page3, []byte("Page 3 Content"))

	pages := map[int][]byte{
		1: page1,
		3: page3,
	}

	// Total size = 3 pages = 3 * 4096
	totalSize := int64(3 * pageSize)

	err = ps.WritePages(ctx, 1, pages)
	require.NoError(t, err)

	// Set Version and Size
	err = ps.SetVersionAndSize(ctx, 1, totalSize, "", 0)
	require.NoError(t, err)

	// Dump
	var buf bytes.Buffer
	err = ps.DumpToWriter(ctx, &buf)
	require.NoError(t, err)

	dumped := buf.Bytes()
	require.Equal(t, int(totalSize), len(dumped))

	// Verify content
	// Page 1
	require.Equal(t, page1, dumped[0:pageSize])

	// Page 2 (Zeroes)
	zeros := make([]byte, pageSize)
	require.Equal(t, zeros, dumped[pageSize:2*pageSize])

	// Page 3
	require.Equal(t, page3, dumped[2*pageSize:3*pageSize])
}
