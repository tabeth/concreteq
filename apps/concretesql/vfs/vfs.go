package vfs

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/psanford/sqlite3vfs"
	"github.com/tabeth/concretesql/store"
)

type FDBVFS struct {
	db     fdb.Database
	fsName string
}

func Register(fsName string, db fdb.Database) error {
	vfs := &FDBVFS{
		db:     db,
		fsName: fsName,
	}
	return sqlite3vfs.RegisterVFS(fsName, vfs)
}

func (v *FDBVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	// Prefix: (fsName, name)
	prefix := tuple.Tuple{v.fsName, name}

	// Create PageStore
	ps := store.NewPageStore(v.db, prefix)
	lm := store.NewLockManager(v.db, prefix)

	f, err := NewFile(name, ps, lm)
	if err != nil {
		// Map FDB errors to SQLite/OS errors if possible?
		// simple error return
		return nil, flags, err
	}
	return f, flags, nil
}

func (v *FDBVFS) Delete(name string, syncDir bool) error {
	_, err := v.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		prefix := tuple.Tuple{v.fsName, name}
		p := prefix.Pack()
		tr.ClearRange(fdb.KeyRange{Begin: fdb.Key(p), End: fdb.Key(append(p, 0xFF))})
		return nil, nil
	})
	return err
}

func (v *FDBVFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	prefix := tuple.Tuple{v.fsName, name}
	exists, err := v.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		p := prefix.Pack()
		rr := r.GetRange(fdb.KeyRange{Begin: fdb.Key(p), End: fdb.Key(append(p, 0xFF))}, fdb.RangeOptions{Limit: 1})
		iter := rr.Iterator()
		return iter.Advance(), nil
	})
	if err != nil {
		return false, err
	}
	return exists.(bool), nil
}

func (v *FDBVFS) FullPathname(name string) string {
	return name
}
