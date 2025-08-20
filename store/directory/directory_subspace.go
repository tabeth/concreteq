package directory

import (
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

// DirectorySubspace represents a Directory that may also be used as a Subspace
// to store key/value pairs. Subdirectories of a root directory (as returned by
// Root or NewDirectoryLayer) are DirectorySubspaces, and provide all methods of
// the Directory and subspace.Subspace interfaces.
type DirectorySubspace interface {
	subspace.Subspace
	Directory
}

type directorySubspace struct {
	subspace.Subspace
	dl    directoryLayer
	path  []string
	layer []byte
}

// String implements the fmt.Stringer interface and returns human-readable
// string representation of this object.
func (ds directorySubspace) String() string {
	var path string
	if len(ds.path) > 0 {
		path = "(" + strings.Join(ds.path, ",") + ")"
	} else {
		path = "nil"
	}
	return fmt.Sprintf("DirectorySubspace(%s, %s)", path, fdb.Printable(ds.Bytes()))
}

func (d directorySubspace) CreateOrOpen(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.CreateOrOpen(t, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) Create(t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.Create(t, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) CreatePrefix(t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error) {
	return d.dl.CreatePrefix(t, d.dl.partitionSubpath(d.path, path), layer, prefix)
}

func (d directorySubspace) Open(rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.Open(rt, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) MoveTo(t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return moveTo(t, d.dl, d.path, newAbsolutePath)
}

func (d directorySubspace) Move(t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error) {
	return d.dl.Move(t, d.dl.partitionSubpath(d.path, oldPath), d.dl.partitionSubpath(d.path, newPath))
}

func (d directorySubspace) Remove(t fdb.Transactor, path []string) (bool, error) {
	return d.dl.Remove(t, d.dl.partitionSubpath(d.path, path))
}

func (d directorySubspace) Exists(rt fdb.ReadTransactor, path []string) (bool, error) {
	return d.dl.Exists(rt, d.dl.partitionSubpath(d.path, path))
}

func (d directorySubspace) List(rt fdb.ReadTransactor, path []string, options ListOptions) ([]string, error) {
	return d.dl.List(rt, d.dl.partitionSubpath(d.path, path), options)
}

func (d directorySubspace) GetLayer() []byte {
	return d.layer
}

func (d directorySubspace) GetPath() []string {
	return d.path
}
