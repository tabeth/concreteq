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

func TestIntegration_CRUD(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	// Ensure FDB is available via shared lib check?
	// The driver init does OpenDB(620).
	// But fdbtest might need to run first or we assume environment is good.
	// SkipIfFDBUnavailable does a check.

	// Format: "fdb-connection-string?vfs=concretesql&db=unique-db-name"
	// Since we ignore the connection string part in PoC and just use FDB default bundle,
	// we just need a unique name for the DB key prefix.
	dbName := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	dsn := fmt.Sprintf("file:%s?vfs=concretesql", dbName)

	db, err := sql.Open("concretesql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Ping to verify connection
	err = db.Ping()
	require.NoError(t, err)

	// Create Table
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)`)
	require.NoError(t, err)

	// Insert Data
	res, err := db.Exec(`INSERT INTO users (name, email) VALUES (?, ?)`, "Alice", "alice@example.com")
	require.NoError(t, err)
	id, err := res.LastInsertId()
	require.NoError(t, err)
	assert.Greater(t, id, int64(0))

	// Query Data
	row := db.QueryRow(`SELECT name, email FROM users WHERE id = ?`, id)
	var name, email string
	err = row.Scan(&name, &email)
	require.NoError(t, err)
	assert.Equal(t, "Alice", name)
	assert.Equal(t, "alice@example.com", email)

	// Update Data
	_, err = db.Exec(`UPDATE users SET email = ? WHERE id = ?`, "alice_new@example.com", id)
	require.NoError(t, err)

	// Verify Update
	row = db.QueryRow(`SELECT email FROM users WHERE id = ?`, id)
	err = row.Scan(&email)
	require.NoError(t, err)
	assert.Equal(t, "alice_new@example.com", email)

	// Delete Data
	_, err = db.Exec(`DELETE FROM users WHERE id = ?`, id)
	require.NoError(t, err)

	// Verify Delete
	row = db.QueryRow(`SELECT count(*) FROM users WHERE id = ?`, id)
	var count int
	err = row.Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestTransactionModes(t *testing.T) {
	// 1. Setup
	fdbtest.SkipIfFDBUnavailable(t)
	// Use unique name to avoid conflicts
	dbName := fmt.Sprintf("test_tx_modes_%d.db", time.Now().UnixNano())
	dsn := fmt.Sprintf("file:%s?vfs=concretesql", dbName)

	// Clean up at end
	defer func() {
		// Manual cleanup since VFS persists in FDB
		// We can use a raw VFS call or just rely on UUID.
		// UUID/timestamp is enough for test.
	}()

	db1, err := sql.Open("concretesql", dsn)
	require.NoError(t, err)
	defer db1.Close()

	db2, err := sql.Open("concretesql", dsn)
	require.NoError(t, err)
	defer db2.Close()

	// Init DB
	_, err = db1.Exec("CREATE TABLE IF NOT EXISTS foo (id INT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)

	// 2. Test BEGIN IMMEDIATE (Pessimistic)
	t.Log("Testing BEGIN IMMEDIATE")

	// db1 explicitly begins IMMEDIATE
	_, err = db1.Exec("BEGIN IMMEDIATE")
	require.NoError(t, err)

	// db2 try to write -> should fail with BUSY/Locked
	// Note: We need to ensure db2 *attempts* the lock.
	// Simply starting `BEGIN IMMEDIATE` should fail.
	_, err = db2.Exec("BEGIN IMMEDIATE")
	// "database is locked"
	require.Error(t, err)

	// db1 writes
	_, err = db1.Exec("INSERT INTO foo VALUES (1, 'bar')")
	require.NoError(t, err)

	// db1 Commits
	_, err = db1.Exec("COMMIT")
	require.NoError(t, err)

	// 3. Test BEGIN DEFERRED (Optimistic)
	t.Log("Testing BEGIN DEFERRED")

	_, err = db1.Exec("BEGIN DEFERRED")
	require.NoError(t, err)

	_, err = db2.Exec("BEGIN DEFERRED")
	require.NoError(t, err)

	// Both active.
	// db1 writes (upgrades to Reserved)
	_, err = db1.Exec("INSERT INTO foo VALUES (2, 'baz')")
	require.NoError(t, err)

	// db2 writes (tries to upgrade to Reserved) -> Fail because db1 has Reserved
	_, err = db2.Exec("INSERT INTO foo VALUES (3, 'qux')")
	require.Error(t, err)

	db1.Exec("ROLLBACK")
	db2.Exec("ROLLBACK")
}

func TestIntegration_PageSize(t *testing.T) {
	// 1. Setup
	fdbtest.SkipIfFDBUnavailable(t)
	dbName := fmt.Sprintf("test_pagesize_%d.db", time.Now().UnixNano())
	// Use URI to set pageSize
	dsn := fmt.Sprintf("file:%s?vfs=concretesql&pageSize=8192", dbName)

	db, err := sql.Open("concretesql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// 2. Initialize (Should trigger SetPageSize)
	_, err = db.Exec("CREATE TABLE foo (id INT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)

	// 3. Insert and verify
	_, err = db.Exec("INSERT INTO foo VALUES (1, 'large_page')")
	require.NoError(t, err)

	// 4. Verify locally that pageSize was set?
	// We can't easily access the PageStore internal state from here without opening a file directly.
	// But the code path in vfs/file.go logs a warning if mismatch.
	// And we can check FDB directly if we want.

	// Create another connection with DIFFERENT page size to trigger warning path (coverage)
	dsn2 := fmt.Sprintf("file:%s?vfs=concretesql&pageSize=4096", dbName)
	db2, err := sql.Open("concretesql", dsn2)
	require.NoError(t, err)
	defer db2.Close()

	// Just opening it should check CurrentVersion and log warning, but accessing it triggers NewFile more reliably?
	// sql.Open doesn't call Open/NewFile until connection is needed.
	_, err = db2.Exec("SELECT 1")
	require.NoError(t, err)
}

func TestIntegration_PageSize_Pragma(t *testing.T) {
	// 1. Setup
	fdbtest.SkipIfFDBUnavailable(t)
	dbName := fmt.Sprintf("test_pagesize_native_%d.db", time.Now().UnixNano())
	dsn := fmt.Sprintf("file:%s?vfs=concretesql", dbName)

	db, err := sql.Open("concretesql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// 2. Set Page Size via PRAGMA
	_, err = db.Exec("PRAGMA page_size = 8192")
	require.NoError(t, err)

	// Trigger header write (page 1) by creating table
	_, err = db.Exec("CREATE TABLE foo (id INT PRIMARY KEY, val TEXT)")
	require.NoError(t, err)

	// 3. Insert
	_, err = db.Exec("INSERT INTO foo VALUES (1, 'large_page')")
	require.NoError(t, err)

	// 4. Verify locally that pageSize was set
	var ps int
	err = db.QueryRow("PRAGMA page_size").Scan(&ps)
	require.NoError(t, err)
	if ps != 8192 {
		t.Fatalf("Expected page_size 8192, got %d", ps)
	}

	// 5. Verify persistence by reopening
	db.Close()
	db2, err := sql.Open("concretesql", dsn)
	require.NoError(t, err)
	defer db2.Close()

	err = db2.QueryRow("PRAGMA page_size").Scan(&ps)
	require.NoError(t, err)
	if ps != 8192 {
		t.Fatalf("Expected persisted page_size 8192, got %d", ps)
	}
}
