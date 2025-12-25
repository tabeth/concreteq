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
