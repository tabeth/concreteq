package fdbadapter

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/kiroku-core/libs/fdb/directory"
)

// DirectoryProvider abstracts directory operations
type DirectoryProvider interface {
	CreateOrOpen(FDBTransaction, []string, []byte) (FDBDirectorySubspace, error)
	Open(FDBReadTransaction, []string, []byte) (FDBDirectorySubspace, error)
	Create(FDBTransaction, []string, []byte) (FDBDirectorySubspace, error)
	Exists(FDBReadTransaction, []string) (bool, error)
	Remove(FDBTransaction, []string) (bool, error)
	List(FDBReadTransaction, []string, directory.ListOptions) ([]string, error)
}

// FDBDirectorySubspace abstracts directory.DirectorySubspace
type FDBDirectorySubspace interface {
	Pack(t tuple.Tuple) fdb.Key
	Sub(tuple.Tuple) FDBDirectorySubspace
	FDBKey() fdb.Key
	Bytes() []byte
	Unpack(k fdb.KeyConvertible) (tuple.Tuple, error)
	Contains(k fdb.KeyConvertible) bool
	FDBRangeKeys() (fdb.Key, fdb.Key)
	FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable)
}

// FDBDatabase abstracts fdb.Database
type FDBDatabase interface {
	Transact(func(FDBTransaction) (interface{}, error)) (interface{}, error)
	ReadTransact(func(FDBReadTransaction) (interface{}, error)) (interface{}, error)
}

// FDBReadTransaction abstracts fdb.ReadTransaction
type FDBReadTransaction interface {
	Get(fdb.KeyConvertible) FDBFutureByteSlice
	GetRange(fdb.Range, fdb.RangeOptions) FDBRangeResult
}

// FDBTransaction abstracts fdb.Transaction
type FDBTransaction interface {
	FDBReadTransaction
	Set(fdb.KeyConvertible, []byte)
	Clear(fdb.KeyConvertible)
	SetVersionstampedKey(fdb.KeyConvertible, []byte)
	ClearRange(fdb.ExactRange) // ClearRange takes ExactRange in fdb
}

// FDBFutureByteSlice abstracts fdb.FutureByteSlice
type FDBFutureByteSlice interface {
	Get() ([]byte, error)
}

// FDBRangeResult abstracts fdb.RangeResult
type FDBRangeResult interface {
	Iterator() FDBRangeIterator
	GetSliceWithError() ([]fdb.KeyValue, error)
}

// FDBRangeIterator abstracts fdb.RangeIterator
type FDBRangeIterator interface {
	Advance() bool
	Get() (fdb.KeyValue, error)
	MustGet() fdb.KeyValue
}
