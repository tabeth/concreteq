package store

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/tabeth/kiroku-core/libs/kms"
)

type FDBStore struct {
	db  fdb.Database
	dir directory.DirectorySubspace
	kms kms.KeyManager
}

func NewFDBStore(db fdb.Database) (*FDBStore, error) {
	dir, err := directory.CreateOrOpen(db, []string{"concretedb"}, nil)
	if err != nil {
		return nil, err
	}

	// Initialize KMS
	kmsInstance, err := kms.NewLocalKMS(db)
	if err != nil {
		return nil, err
	}

	return &FDBStore{db: db, dir: dir, kms: kmsInstance}, nil
}
