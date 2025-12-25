package fdbadapter

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/kiroku-core/libs/fdb/directory"
)

// RealFDBDatabase wraps fdb.Database
type RealFDBDatabase struct {
	db fdb.Database
}

func NewRealFDBDatabase(db fdb.Database) *RealFDBDatabase {
	return &RealFDBDatabase{db: db}
}

func (d *RealFDBDatabase) Transact(f func(FDBTransaction) (interface{}, error)) (interface{}, error) {
	return d.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return f(&RealFDBTransaction{tr: tr})
	})
}

func (d *RealFDBDatabase) ReadTransact(f func(FDBReadTransaction) (interface{}, error)) (interface{}, error) {
	return d.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		return f(&RealFDBReadTransaction{rtr: rtr})
	})
}

// RealFDBReadTransaction wraps fdb.ReadTransaction
type RealFDBReadTransaction struct {
	rtr fdb.ReadTransaction
}

func (t *RealFDBReadTransaction) Get(key fdb.KeyConvertible) FDBFutureByteSlice {
	return t.rtr.Get(key)
}

func (t *RealFDBReadTransaction) GetRange(r fdb.Range, options fdb.RangeOptions) FDBRangeResult {
	return &RealFDBRangeResult{res: t.rtr.GetRange(r, options)}
}

// RealFDBTransaction wraps fdb.Transaction
type RealFDBTransaction struct {
	tr fdb.Transaction
}

func (t *RealFDBTransaction) Get(key fdb.KeyConvertible) FDBFutureByteSlice {
	return t.tr.Get(key)
}

func (t *RealFDBTransaction) GetRange(r fdb.Range, options fdb.RangeOptions) FDBRangeResult {
	return &RealFDBRangeResult{res: t.tr.GetRange(r, options)}
}

func (t *RealFDBTransaction) Set(key fdb.KeyConvertible, value []byte) {
	t.tr.Set(key, value)
}

func (t *RealFDBTransaction) Clear(key fdb.KeyConvertible) {
	t.tr.Clear(key)
}

func (t *RealFDBTransaction) SetVersionstampedKey(key fdb.KeyConvertible, value []byte) {
	t.tr.SetVersionstampedKey(key, value)
}

func (t *RealFDBTransaction) ClearRange(r fdb.ExactRange) {
	t.tr.ClearRange(r)
}

// RealFDBRangeResult wraps fdb.RangeResult
type RealFDBRangeResult struct {
	res fdb.RangeResult
}

func (r *RealFDBRangeResult) Iterator() FDBRangeIterator {
	return r.res.Iterator()
}

func (r *RealFDBRangeResult) GetSliceWithError() ([]fdb.KeyValue, error) {
	return r.res.GetSliceWithError()
}

// RealDirectoryProvider wraps directory.DirectoryLayer
type RealDirectoryProvider struct {
	dl directory.Directory
}

func NewRealDirectoryProvider(dl directory.Directory) *RealDirectoryProvider {
	return &RealDirectoryProvider{dl: dl}
}

func (p *RealDirectoryProvider) CreateOrOpen(tr FDBTransaction, path []string, layer []byte) (FDBDirectorySubspace, error) {
	realTR := tr.(*RealFDBTransaction).tr
	ds, err := p.dl.CreateOrOpen(realTR, path, layer)
	if err != nil {
		return nil, err
	}
	return &RealFDBDirectorySubspace{ds: ds}, nil
}

func (p *RealDirectoryProvider) Open(tr FDBReadTransaction, path []string, layer []byte) (FDBDirectorySubspace, error) {
	var realTR fdb.ReadTransaction
	switch t := tr.(type) {
	case *RealFDBReadTransaction:
		realTR = t.rtr
	case *RealFDBTransaction:
		realTR = t.tr
	default:
		panic("invalid transaction type in RealDirectoryProvider")
	}

	ds, err := p.dl.Open(realTR, path, layer)
	if err != nil {
		return nil, err
	}
	return &RealFDBDirectorySubspace{ds: ds}, nil
}

func (p *RealDirectoryProvider) Create(tr FDBTransaction, path []string, layer []byte) (FDBDirectorySubspace, error) {
	realTR := tr.(*RealFDBTransaction).tr
	ds, err := p.dl.Create(realTR, path, layer)
	if err != nil {
		return nil, err
	}
	return &RealFDBDirectorySubspace{ds: ds}, nil
}

func (p *RealDirectoryProvider) Exists(tr FDBReadTransaction, path []string) (bool, error) {
	var realTR fdb.ReadTransaction
	switch t := tr.(type) {
	case *RealFDBReadTransaction:
		realTR = t.rtr
	case *RealFDBTransaction:
		realTR = t.tr
	default:
		panic("invalid transaction type in RealDirectoryProvider")
	}
	return p.dl.Exists(realTR, path)
}

func (p *RealDirectoryProvider) Remove(tr FDBTransaction, path []string) (bool, error) {
	realTR := tr.(*RealFDBTransaction).tr
	return p.dl.Remove(realTR, path)
}

func (p *RealDirectoryProvider) List(tr FDBReadTransaction, path []string, options directory.ListOptions) ([]string, error) {
	var realTR fdb.ReadTransaction
	switch t := tr.(type) {
	case *RealFDBReadTransaction:
		realTR = t.rtr
	case *RealFDBTransaction:
		realTR = t.tr
	default:
		panic("invalid transaction type in RealDirectoryProvider")
	}
	return p.dl.List(realTR, path, options)
}

// RealFDBDirectorySubspace wraps subspace.Subspace
type RealFDBDirectorySubspace struct {
	ds subspace.Subspace
}

func (s *RealFDBDirectorySubspace) Pack(t tuple.Tuple) fdb.Key {
	return s.ds.Pack(t)
}

func (s *RealFDBDirectorySubspace) Sub(t tuple.Tuple) FDBDirectorySubspace {
	return &RealFDBDirectorySubspace{ds: s.ds.Sub(t)}
}

func (s *RealFDBDirectorySubspace) FDBKey() fdb.Key {
	return s.ds.FDBKey()
}

func (s *RealFDBDirectorySubspace) Bytes() []byte {
	return s.ds.Bytes()
}

func (s *RealFDBDirectorySubspace) Unpack(k fdb.KeyConvertible) (tuple.Tuple, error) {
	return s.ds.Unpack(k)
}

func (s *RealFDBDirectorySubspace) Contains(k fdb.KeyConvertible) bool {
	return s.ds.Contains(k)
}

func (s *RealFDBDirectorySubspace) FDBRangeKeys() (fdb.Key, fdb.Key) {
	k1, k2 := s.ds.FDBRangeKeys()
	return k1.FDBKey(), k2.FDBKey()
}

func (s *RealFDBDirectorySubspace) FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable) {
	return s.ds.FDBRangeKeySelectors()
}
