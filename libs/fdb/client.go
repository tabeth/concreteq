package fdb

import (
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var fdbInitOnce sync.Once

// OpenDB ensures the FDB API version is set and opens a database connection.
func OpenDB(apiVersion int) (fdb.Database, error) {
	fdbInitOnce.Do(func() {
		fdb.MustAPIVersion(apiVersion)
	})
	return fdb.OpenDefault()
}
