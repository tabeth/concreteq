package store

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/store/internal/fdbadapter"
	"github.com/tabeth/kiroku-core/libs/fdb/directory"
)

func init() {
	fdb.MustAPIVersion(710)
}

// MockFDBDatabase is a mock for FDBDatabase
type MockFDBDatabase struct {
	TransactFunc     func(func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error)
	ReadTransactFunc func(func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error)
}

func (m *MockFDBDatabase) Transact(f func(fdbadapter.FDBTransaction) (interface{}, error)) (interface{}, error) {
	if m.TransactFunc != nil {
		return m.TransactFunc(f)
	}
	return f(&MockFDBTransaction{})
}

func (m *MockFDBDatabase) ReadTransact(f func(fdbadapter.FDBReadTransaction) (interface{}, error)) (interface{}, error) {
	if m.ReadTransactFunc != nil {
		return m.ReadTransactFunc(f)
	}
	return f(&MockFDBReadTransaction{})
}

// MockFDBReadTransaction is a mock for FDBReadTransaction
type MockFDBReadTransaction struct {
	GetFunc      func(fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice
	GetRangeFunc func(fdb.Range, fdb.RangeOptions) fdbadapter.FDBRangeResult
}

func (m *MockFDBReadTransaction) Get(k fdb.KeyConvertible) fdbadapter.FDBFutureByteSlice {
	if m.GetFunc != nil {
		return m.GetFunc(k)
	}
	return &MockFDBFutureByteSlice{}
}
func (m *MockFDBReadTransaction) GetRange(r fdb.Range, opts fdb.RangeOptions) fdbadapter.FDBRangeResult {
	if m.GetRangeFunc != nil {
		return m.GetRangeFunc(r, opts)
	}
	return &MockFDBRangeResult{}
}

// MockFDBTransaction is a mock for FDBTransaction
type MockFDBTransaction struct {
	MockFDBReadTransaction
	SetFunc                  func(fdb.KeyConvertible, []byte)
	ClearFunc                func(fdb.KeyConvertible)
	ClearRangeFunc           func(fdb.ExactRange)
	SetVersionstampedKeyFunc func(fdb.KeyConvertible, []byte)
}

func (m *MockFDBTransaction) Set(k fdb.KeyConvertible, v []byte) {
	if m.SetFunc != nil {
		m.SetFunc(k, v)
	}
}

func (m *MockFDBTransaction) Clear(k fdb.KeyConvertible) {
	if m.ClearFunc != nil {
		m.ClearFunc(k)
	}
}

func (m *MockFDBTransaction) ClearRange(r fdb.ExactRange) {
	if m.ClearRangeFunc != nil {
		m.ClearRangeFunc(r)
	}
}

func (m *MockFDBTransaction) SetVersionstampedKey(k fdb.KeyConvertible, v []byte) {
	if m.SetVersionstampedKeyFunc != nil {
		m.SetVersionstampedKeyFunc(k, v)
	}
}

// MockFDBFutureByteSlice is a mock for FDBFutureByteSlice
type MockFDBFutureByteSlice struct {
	GetFunc func() ([]byte, error)
}

func (m *MockFDBFutureByteSlice) Get() ([]byte, error) {
	if m.GetFunc != nil {
		return m.GetFunc()
	}
	return nil, nil
}

// MockFDBRangeResult is a mock for FDBRangeResult
type MockFDBRangeResult struct {
	IteratorFunc          func() fdbadapter.FDBRangeIterator
	GetSliceWithErrorFunc func() ([]fdb.KeyValue, error)
}

func (m *MockFDBRangeResult) Iterator() fdbadapter.FDBRangeIterator {
	if m.IteratorFunc != nil {
		return m.IteratorFunc()
	}
	return &MockFDBRangeIterator{}
}

func (m *MockFDBRangeResult) GetSliceWithError() ([]fdb.KeyValue, error) {
	if m.GetSliceWithErrorFunc != nil {
		return m.GetSliceWithErrorFunc()
	}
	it := m.Iterator()
	if it == nil {
		return nil, nil
	}
	var res []fdb.KeyValue
	for it.Advance() {
		kv, err := it.Get()
		if err != nil {
			return nil, err
		}
		res = append(res, kv)
	}
	return res, nil
}

// MockFDBRangeIterator is a mock for FDBRangeIterator
type MockFDBRangeIterator struct {
	AdvanceFunc func() bool
	GetFunc     func() (fdb.KeyValue, error)
	MustGetFunc func() fdb.KeyValue
}

func (m *MockFDBRangeIterator) Advance() bool {
	if m.AdvanceFunc != nil {
		return m.AdvanceFunc()
	}
	return false
}

func (m *MockFDBRangeIterator) Get() (fdb.KeyValue, error) {
	if m.GetFunc != nil {
		return m.GetFunc()
	}
	return fdb.KeyValue{}, nil
}

func (m *MockFDBRangeIterator) MustGet() fdb.KeyValue {
	if m.MustGetFunc != nil {
		return m.MustGetFunc()
	}
	return fdb.KeyValue{}
}

// MockDirectoryProvider is a mock for DirectoryProvider
type MockDirectoryProvider struct {
	CreateOrOpenFunc func(fdbadapter.FDBTransaction, []string, []byte) (fdbadapter.FDBDirectorySubspace, error)
	OpenFunc         func(fdbadapter.FDBReadTransaction, []string, []byte) (fdbadapter.FDBDirectorySubspace, error)
	CreateFunc       func(fdbadapter.FDBTransaction, []string, []byte) (fdbadapter.FDBDirectorySubspace, error)
	ExistsFunc       func(fdbadapter.FDBReadTransaction, []string) (bool, error)
	RemoveFunc       func(fdbadapter.FDBTransaction, []string) (bool, error)
	ListFunc         func(fdbadapter.FDBReadTransaction, []string, directory.ListOptions) ([]string, error)
}

func (m *MockDirectoryProvider) CreateOrOpen(tr fdbadapter.FDBTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
	if m.CreateOrOpenFunc != nil {
		return m.CreateOrOpenFunc(tr, path, layer)
	}
	return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
}

func (m *MockDirectoryProvider) Open(tr fdbadapter.FDBReadTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
	if m.OpenFunc != nil {
		return m.OpenFunc(tr, path, layer)
	}
	return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
}

func (m *MockDirectoryProvider) Create(tr fdbadapter.FDBTransaction, path []string, layer []byte) (fdbadapter.FDBDirectorySubspace, error) {
	if m.CreateFunc != nil {
		return m.CreateFunc(tr, path, layer)
	}
	return &MockFDBDirectorySubspace{path: stringSliceToTuple(path)}, nil
}

func (m *MockDirectoryProvider) Exists(tr fdbadapter.FDBReadTransaction, path []string) (bool, error) {
	if m.ExistsFunc != nil {
		return m.ExistsFunc(tr, path)
	}
	return true, nil
}

func (m *MockDirectoryProvider) Remove(tr fdbadapter.FDBTransaction, path []string) (bool, error) {
	if m.RemoveFunc != nil {
		return m.RemoveFunc(tr, path)
	}
	return true, nil
}

func (m *MockDirectoryProvider) List(tr fdbadapter.FDBReadTransaction, path []string, opts directory.ListOptions) ([]string, error) {
	if m.ListFunc != nil {
		return m.ListFunc(tr, path, opts)
	}
	return nil, nil
}

// MockFDBDirectorySubspace is a mock for FDBDirectorySubspace
type MockFDBDirectorySubspace struct {
	path                     tuple.Tuple
	PackFunc                 func(tuple.Tuple) fdb.Key
	SubFunc                  func(tuple.Tuple) fdbadapter.FDBDirectorySubspace
	FDBKeyFunc               func() fdb.Key
	BytesFunc                func() []byte
	UnpackFunc               func(fdb.KeyConvertible) (tuple.Tuple, error)
	ContainsFunc             func(fdb.KeyConvertible) bool
	FDBRangeKeysFunc         func() (fdb.Key, fdb.Key)
	FDBRangeKeySelectorsFunc func() (fdb.Selectable, fdb.Selectable)
}

func (m *MockFDBDirectorySubspace) Pack(t tuple.Tuple) fdb.Key {
	if m.PackFunc != nil {
		return m.PackFunc(t)
	}
	return fdb.Key(append(m.path.Pack(), t.Pack()...))
}

func (m *MockFDBDirectorySubspace) Sub(t tuple.Tuple) fdbadapter.FDBDirectorySubspace {
	if m.SubFunc != nil {
		return m.SubFunc(t)
	}
	newPath := make(tuple.Tuple, len(m.path)+len(t))
	copy(newPath, m.path)
	copy(newPath[len(m.path):], t)
	return &MockFDBDirectorySubspace{path: newPath}
}

func (m *MockFDBDirectorySubspace) FDBKey() fdb.Key {
	if m.FDBKeyFunc != nil {
		return m.FDBKeyFunc()
	}
	return fdb.Key(m.path.Pack())
}

func (m *MockFDBDirectorySubspace) Bytes() []byte {
	if m.BytesFunc != nil {
		return m.BytesFunc()
	}
	return m.path.Pack()
}

func (m *MockFDBDirectorySubspace) Unpack(k fdb.KeyConvertible) (tuple.Tuple, error) {
	if m.UnpackFunc != nil {
		return m.UnpackFunc(k)
	}
	return nil, nil
}

func (m *MockFDBDirectorySubspace) Contains(k fdb.KeyConvertible) bool {
	if m.ContainsFunc != nil {
		return m.ContainsFunc(k)
	}
	return true
}

func (m *MockFDBDirectorySubspace) FDBRangeKeys() (fdb.Key, fdb.Key) {
	if m.FDBRangeKeysFunc != nil {
		return m.FDBRangeKeysFunc()
	}
	return nil, nil
}

func (m *MockFDBDirectorySubspace) FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable) {
	return fdb.FirstGreaterOrEqual(m.FDBKey()), fdb.FirstGreaterOrEqual(append(m.FDBKey(), 0xFF))
}

func stringSliceToTuple(path []string) tuple.Tuple {
	t := make(tuple.Tuple, len(path))
	for i, s := range path {
		t[i] = s
	}
	return t
}
