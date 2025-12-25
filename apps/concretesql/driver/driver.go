package driver

import (
	"database/sql"
	"database/sql/driver"
	"log"
	"strings"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	sqlite "github.com/mattn/go-sqlite3"
	"github.com/tabeth/concretesql/store"
	"github.com/tabeth/concretesql/vfs"
	sharedfdb "github.com/tabeth/kiroku-core/libs/fdb"
)

var (
	fdbOnce  sync.Once
	vfsOnce  sync.Once
	database fdb.Database
)

func init() {
	sql.Register("concretesql", &SQLiteDriver{})
}

type SQLiteDriver struct {
	sqlite.SQLiteDriver
}

// Open opens a database connection.
// dsn format: "fdb-connection-string?vfs=concretesql&db=unique-db-name"
// actually, if we want to use FDB VFS, we need to register VFS first.
// And we need to initialize FDB.
func (d *SQLiteDriver) Open(dsn string) (conn driver.Conn, err error) {
	// 1. Initialize FDB if needed
	// Assuming FDB API version is already selected somewhere else in the app?
	// If not, we do it here safely.
	// NOTE: fdb.MustAPIVersion(500) panics if called twice.
	// We should allow app to init, or do it once.
	fdbOnce.Do(func() {
		// Warning: This could race if another part of app sets it.
		// Best practice: App sets it. We assume 500+
		var err error
		database, err = sharedfdb.OpenDB(620)
		if err != nil {
			log.Panicf("failed to open fdb: %v", err)
		}
	})

	vfsOnce.Do(func() {
		if err := vfs.Register("concretesql", database, store.DefaultConfig()); err != nil {
			log.Panicf("failed to register vfs: %v", err)
		}
	})

	// 2. Parse DSN
	// format: [file:]path?vfs=concretesql
	// SQLite driver usually takes a file path.
	// We want to force vfs=concretesql

	// Check if vfs param is present
	if !strings.Contains(dsn, "vfs=concretesql") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		dsn += sep + "vfs=concretesql"
	}

	return d.SQLiteDriver.Open(dsn)
}
