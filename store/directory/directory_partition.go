package directory

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type directoryPartition struct {
	directoryLayer
	parentDirectoryLayer directoryLayer
}

func (dp directoryPartition) Sub(el ...tuple.TupleElement) subspace.Subspace {
	panic("cannot open subspace in the root of a directory partition")
}

func (dp directoryPartition) Bytes() []byte {
	panic("cannot get key for the root of a directory partition")
}

func (dp directoryPartition) Pack(t tuple.Tuple) fdb.Key {
	panic("cannot pack keys using the root of a directory partition")
}

func (dp directoryPartition) PackWithVersionstamp(t tuple.Tuple) (fdb.Key, error) {
	panic("cannot pack keys using the root of a directory partition")
}

func (dp directoryPartition) Unpack(k fdb.KeyConvertible) (tuple.Tuple, error) {
	panic("cannot unpack keys using the root of a directory partition")
}

func (dp directoryPartition) Contains(k fdb.KeyConvertible) bool {
	panic("cannot check whether a key belongs to the root of a directory partition")
}

func (dp directoryPartition) FDBKey() fdb.Key {
	panic("cannot use the root of a directory partition as a key")
}

func (dp directoryPartition) FDBRangeKeys() (fdb.KeyConvertible, fdb.KeyConvertible) {
	panic("cannot get range for the root of a directory partition")
}

func (dp directoryPartition) FDBRangeKeySelectors() (fdb.Selectable, fdb.Selectable) {
	panic("cannot get range for the root of a directory partition")
}

func (dp directoryPartition) GetLayer() []byte {
	return []byte("partition")
}

func (dp directoryPartition) getLayerForPath(path []string) directoryLayer {
	if len(path) == 0 {
		return dp.parentDirectoryLayer
	}
	return dp.directoryLayer
}

func (dp directoryPartition) MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return moveTo(t, dp.parentDirectoryLayer, dp.path, newAbsolutePath)
}

func (dp directoryPartition) Remove(t fdb.Transactor, path []string) (bool, error) {
	dl := dp.getLayerForPath(path)
	return dl.Remove(t, dl.partitionSubpath(dp.path, path))
}

func (dp directoryPartition) Exists(rt fdb.ReadTransactor, path []string) (bool, error) {
	dl := dp.getLayerForPath(path)
	return dl.Exists(rt, dl.partitionSubpath(dp.path, path))
}

func (dp directoryPartition) List(rt fdb.ReadTransactor, path []string, options ListOptions) ([]string, error) {
	dl := dp.getLayerForPath(path)
	return dl.List(rt, dl.partitionSubpath(dp.path, path), options)
}
