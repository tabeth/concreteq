package driver_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "github.com/tabeth/concretesql/driver"
)

func TestJSONSupport(t *testing.T) {
	// This test verifies that the underlying SQLite build supports JSON functions.
	// We use the "memory" VFS or just a simple file-based one.
	// But since we want to test concretesql integration, we should try using it if possible,
	// but simple sqlite driver check is enough for the "engine capability" check.
	// Actually, let's just use standard sqlite driver for this check to ensure the *build* has it,
	// as concretesql driver just wraps it.
	// Wait, we need to make sure the driver we registered ("concretesql") works with it.

	// We need a unique DB name to strict isolation
	dbName := "json_test_" + time.Now().Format("20060102150405")
	dsn := "file:" + dbName + "?vfs=concretesql"

	// NOTE: This requires FDB to be running.
	// If we want a pure unit test without FDB dependency for just *binary capability*,
	// we could check `sqlite3_compileoption_used('ENABLE_JSON1')`.
	// But let's try an actual query.

	db, err := sql.Open("concretesql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Check compile options
	var opt int
	err = db.QueryRow("SELECT sqlite_compileoption_used('ENABLE_JSON1')").Scan(&opt)
	// If error happens, table/function might not exist.
	if err != nil {
		t.Logf("Warning: sqlite_compileoption_used failed: %v", err)
	} else {
		t.Logf("ENABLE_JSON1: %v", opt)
	}

	// Create table with JSON
	_, err = db.Exec(`CREATE TABLE json_test (id INTEGER PRIMARY KEY, data JSON)`)
	require.NoError(t, err)

	// Insert JSON
	jsonData := `{"foo": "bar", "baz": 123}`
	_, err = db.Exec(`INSERT INTO json_test (data) VALUES (?)`, jsonData)
	require.NoError(t, err)

	// Query with JSON function
	var val string
	err = db.QueryRow(`SELECT json_extract(data, '$.foo') FROM json_test`).Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "bar", val)
}
