package driver_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "github.com/tabeth/concretesql/driver"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

// Helper to open DB
func openDB(t *testing.T) *sql.DB {
	dbName := fmt.Sprintf("stress_db_%d_%d", time.Now().UnixNano(), time.Now().Unix())
	dsn := fmt.Sprintf("file:%s?vfs=concretesql", dbName)
	db, err := sql.Open("concretesql", dsn)
	require.NoError(t, err)
	err = db.Ping()
	require.NoError(t, err)
	return db
}

func TestLargeTransaction(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE blob_test (id INTEGER PRIMARY KEY, data BLOB)`)
	require.NoError(t, err)

	// Generate 12MB blob
	blobSize := 12 * 1024 * 1024
	blob := make([]byte, blobSize)
	// Fill with some pattern
	blob[0] = 0xBE
	blob[blobSize-1] = 0xEF

	// Begin Tx
	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO blob_test (data) VALUES (?)`, blob)
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify
	row := db.QueryRow(`SELECT length(data) FROM blob_test WHERE id = 1`)
	var length int
	err = row.Scan(&length)
	require.NoError(t, err)
	assert.Equal(t, blobSize, length)
}

func TestLongDurationTransaction(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE time_test (id INTEGER PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`INSERT INTO time_test (val) VALUES ('start')`)
	require.NoError(t, err)

	// Sleep 6 seconds (exceeds FDB 5s limit)
	// If we held a single FDB transaction open, this would fail on Commit (or next op).
	// But since we use Shadow Paging and optimistic locking/detached writes, it should pass.
	time.Sleep(6 * time.Second)

	_, err = tx.Exec(`INSERT INTO time_test (val) VALUES ('end')`)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify
	rows, err := db.Query(`SELECT val FROM time_test ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		rows.Scan(&v)
		vals = append(vals, v)
	}
	assert.Equal(t, []string{"start", "end"}, vals)
}

func TestAbortedTransaction(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE safety_test (id INTEGER PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)

	// 1. Successful Insert
	_, err = db.Exec(`INSERT INTO safety_test (val) VALUES ('committed')`)
	require.NoError(t, err)

	// 2. Aborted Insert
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO safety_test (val) VALUES ('aborted')`)
	require.NoError(t, err)
	err = tx.Rollback()
	require.NoError(t, err)

	// 3. Verify
	rows, err := db.Query(`SELECT val FROM safety_test`)
	require.NoError(t, err)
	defer rows.Close()

	var vals []string
	for rows.Next() {
		var v string
		rows.Scan(&v)
		vals = append(vals, v)
	}
	assert.Equal(t, []string{"committed"}, vals)
}

func TestDriver_AutoVFSInjection(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	dbName := fmt.Sprintf("auto_vfs_db_%d", time.Now().UnixNano())
	// No vfs=concretesql here
	db, err := sql.Open("concretesql", dbName)
	require.NoError(t, err)
	defer db.Close()

	// Ping should succeed and use VFS (implied)
	err = db.Ping()
	require.NoError(t, err)
}
