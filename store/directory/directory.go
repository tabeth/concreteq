package directory

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

const (
	_SUBDIRS int = 0

	// []int32{1,0,0} by any other name
	_MAJORVERSION int32 = 1
	_MINORVERSION int32 = 0
	_MICROVERSION int32 = 0
)

var (
	// ErrDirAlreadyExists is returned when trying to create a directory while it already exists.
	ErrDirAlreadyExists = errors.New("the directory already exists")

	// ErrDirNotExists is returned when opening or listing a directory that does not exist.
	ErrDirNotExists = errors.New("the directory does not exist")

	// ErrParentDirDoesNotExist is returned when opening a directory and one or more
	// parent directories in the path do not exist.
	ErrParentDirDoesNotExist = errors.New("the parent directory does not exist")
)

// ListOptions specifies the options for a paginated list operation.
type ListOptions struct {
	Limit int
	After string
}

// Directory represents a subspace of keys in a FoundationDB database,
// identified by a hierarchical path.
type Directory interface {
	CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)
	Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error)
	Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error)
	CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error)
	Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error)
	MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error)
	Remove(t fdb.Transactor, path []string) (bool, error)
	Exists(rt fdb.ReadTransactor, path []string) (bool, error)
	List(rt fdb.ReadTransactor, path []string, options ListOptions) ([]string, error)
	GetLayer() []byte
	GetPath() []string
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func moveTo(t fdb.Transactor, dl directoryLayer, path, newAbsolutePath []string) (DirectorySubspace, error) {
	partition_len := len(dl.path)

	if !stringsEqual(newAbsolutePath[:partition_len], dl.path) {
		return nil, errors.New("cannot move between partitions")
	}

	return dl.Move(t, path[partition_len:], newAbsolutePath[partition_len:])
}

var root = NewDirectoryLayer(subspace.FromBytes([]byte{0xFE}), subspace.AllKeys(), false)

func CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.CreateOrOpen(t, path, layer)
}

func Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.Open(rt, path, layer)
}

func Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return root.Create(t, path, layer)
}

func Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error) {
	return root.Move(t, oldPath, newPath)
}

func Exists(rt fdb.ReadTransactor, path []string) (bool, error) {
	return root.Exists(rt, path)
}

func List(rt fdb.ReadTransactor, path []string, options ListOptions) ([]string, error) {
	return root.List(rt, path, options)
}

func Root() Directory {
	return root
}
