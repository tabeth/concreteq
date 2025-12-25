package main

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"io"
	"sync"
	"unsafe"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/psanford/sqlite3vfs"
	"github.com/tabeth/concretesql/store"
	"github.com/tabeth/concretesql/vfs"
)

// Global map to store File objects
var (
	fileMap    = make(map[int]*vfs.File)
	fileMapMu  sync.Mutex
	nextFileID = 1
)

func registerFile(f *vfs.File) int {
	fileMapMu.Lock()
	defer fileMapMu.Unlock()
	id := nextFileID
	nextFileID++
	fileMap[id] = f
	return id
}

func getFile(id int) *vfs.File {
	fileMapMu.Lock()
	defer fileMapMu.Unlock()
	return fileMap[id]
}

func removeFile(id int) {
	fileMapMu.Lock()
	defer fileMapMu.Unlock()
	delete(fileMap, id)
}

// Helper to convert Go errors to SQLite codes
func toSQLiteError(err error) C.int {
	if err == nil {
		return 0 // SQLITE_OK
	}
	if err == io.EOF {
		return 101 // SQLITE_DONE (sometimes used for EOF?) Or SQLITE_IOERR_SHORT_READ
		// SQLite expects Read to return SQLITE_IOERR_SHORT_READ if not enough data
		// But our ReadAt wraps to strict size.
		return 10 // SQLITE_IOERR
	}
	// Simple mapping
	return 10 // SQLITE_IOERR
}

//export go_vfs_open
func go_vfs_open(name *C.char, fileOut *unsafe.Pointer, flags C.int, outFlags *C.int) C.int {
	goName := C.GoString(name)

	// We need to access the FDBVFS instance.
	// Since we only have one (created in Init), we can use a global or re-create partial context?
	// vfs.Open needs a DB connection.
	// In `InitConcreteSQLVFS` we opened FDB.
	// We should store that global DB handle.

	if globalDB == nil {
		// Should not happen if Init called
		return 14 // SQLITE_CANTOPEN
	}

	// Logic from vfs.go: Open
	// Prefix: (fsName, name)
	prefix := tuple.Tuple{"concretesql", goName}

	// Create PageStore & LockManager
	ps := store.NewPageStore(*globalDB, prefix, store.DefaultConfig())
	lm := store.NewLockManager(*globalDB, prefix, store.DefaultConfig())

	// PageSize?
	// We can parse generic URI logic if we want, or rely on PRAGMA (preferred).
	pageSize := 0

	// Create File
	f, err := vfs.NewFile(goName, ps, lm, pageSize)
	if err != nil {
		return toSQLiteError(err)
	}

	id := registerFile(f)

	// We pass ID as void*
	// safe because int fits in pointer (usually, Cgo Handle pattern is better but simple map ID works)
	*fileOut = unsafe.Pointer(uintptr(id))

	if outFlags != nil {
		*outFlags = flags // We claim we support whatever flag asked
	}

	return 0 // SQLITE_OK
}

var globalDB *fdb.Database

func init() {
	// We defer FDB init to the Extension Init C call, but we need to store the result?
	// Actually InitConcreteSQLVFS calls fdb.MustOpenDefault.
	// We need to capture it there.
	// But main() in Go is not called like a library.
	// init() is called.
	// We can't init FDB here because `MustAPIVersion` might panic if called multiple times or too early?
	// Safe to assume we set globalDB in InitConcreteSQLVFS?
}

//export go_vfs_delete
func go_vfs_delete(name *C.char, syncDir C.int) C.int {
	goName := C.GoString(name)
	if globalDB == nil {
		return 10
	}

	// Logic from vfs.go: Delete
	_, err := globalDB.Transact(func(tr fdb.Transaction) (interface{}, error) {
		prefix := tuple.Tuple{"concretesql", goName}
		p := prefix.Pack()
		kStart := fdb.Key(p)
		kEnd := fdb.Key(append(p, 0xFF))
		tr.ClearRange(fdb.KeyRange{Begin: kStart, End: kEnd})
		return nil, nil
	})
	return toSQLiteError(err)
}

//export go_vfs_access
func go_vfs_access(name *C.char, flags C.int, pResOut *C.int) C.int {
	goName := C.GoString(name)
	if globalDB == nil {
		return 10
	}

	prefix := tuple.Tuple{"concretesql", goName}
	exists, err := globalDB.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		p := prefix.Pack()
		kStart := fdb.Key(p)
		kEnd := fdb.Key(append(p, 0xFF))
		rr := r.GetRange(fdb.KeyRange{Begin: kStart, End: kEnd}, fdb.RangeOptions{Limit: 1})
		iter := rr.Iterator()
		return iter.Advance(), nil
	})

	if err != nil {
		return toSQLiteError(err)
	}

	ex := exists.(bool)
	if ex {
		*pResOut = 1
	} else {
		*pResOut = 0
	}
	return 0
}

//export go_vfs_full_pathname
func go_vfs_full_pathname(name *C.char, nOut C.int, zOut *C.char) C.int {
	// Just copy name to zOut
	goName := C.GoString(name)
	cStr := C.CString(goName)
	defer C.free(unsafe.Pointer(cStr))

	// strncpy
	C.strncpy(zOut, cStr, C.size_t(nOut))
	return 0
}

// --- File Methods ---

func getFileFromPtr(p unsafe.Pointer) *vfs.File {
	id := int(uintptr(p))
	return getFile(id)
}

//export go_file_close
func go_file_close(p unsafe.Pointer) C.int {
	f := getFileFromPtr(p)
	if f != nil {
		f.Close()
		id := int(uintptr(p))
		removeFile(id)
	}
	return 0
}

//export go_file_read
func go_file_read(p unsafe.Pointer, buf unsafe.Pointer, iAmt C.int, iOfst C.longlong) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 10
	}

	goBuf := make([]byte, int(iAmt))
	n, err := f.ReadAt(goBuf, int64(iOfst))

	// SQLite expects exact read. If < iAmt, return IOERR_SHORT_READ & Set zero
	if n > 0 {
		C.memcpy(buf, unsafe.Pointer(&goBuf[0]), C.size_t(n))
	}

	if n < int(iAmt) {
		// Zero out rest?
		// SQLite docs: "If xRead() returns SQLITE_IOERR_SHORT_READ it must also output zeros into the unread portion of the buffer."
		// We will just return SHORT_READ error, `buf` is user memory.
		// Let's memset the rest.
		// Actually safer to zero the whole things if err?
		// n is what we read.
		offset := uintptr(buf) + uintptr(n)
		rem := int(iAmt) - n
		if rem > 0 {
			C.memset(unsafe.Pointer(offset), 0, C.size_t(rem))
		}
		return 2 // SQLITE_IOERR_SHORT_READ
	}

	if err != nil && err != io.EOF {
		return toSQLiteError(err)
	}
	return 0
}

//export go_file_write
func go_file_write(p unsafe.Pointer, buf unsafe.Pointer, iAmt C.int, iOfst C.longlong) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 10
	}

	// Copy into Go slice
	goBuf := C.GoBytes(buf, iAmt)

	_, err := f.WriteAt(goBuf, int64(iOfst))
	return toSQLiteError(err)
}

//export go_file_truncate
func go_file_truncate(p unsafe.Pointer, size C.longlong) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 10
	}
	return toSQLiteError(f.Truncate(int64(size)))
}

//export go_file_sync
func go_file_sync(p unsafe.Pointer, flags C.int) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 10
	}
	// Flags: SQLITE_SYNC_NORMAL vs FULL. We ignore for now.
	return toSQLiteError(f.Sync(sqlite3vfs.SyncType(flags)))
}

//export go_file_file_size
func go_file_file_size(p unsafe.Pointer, pSize *C.longlong) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 10
	}
	sz, err := f.FileSize()
	if err == nil {
		*pSize = C.longlong(sz)
	}
	return toSQLiteError(err)
}

//export go_file_lock
func go_file_lock(p unsafe.Pointer, lock C.int) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 10
	}
	return toSQLiteError(f.Lock(sqlite3vfs.LockType(lock)))
}

//export go_file_unlock
func go_file_unlock(p unsafe.Pointer, lock C.int) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 10
	}
	return toSQLiteError(f.Unlock(sqlite3vfs.LockType(lock)))
}

//export go_file_check_reserved_lock
func go_file_check_reserved_lock(p unsafe.Pointer, pResOut *C.int) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 10
	}
	res, err := f.CheckReservedLock()
	if err != nil {
		return toSQLiteError(err)
	}
	if res {
		*pResOut = 1
	} else {
		*pResOut = 0
	}
	return 0
}

//export go_file_sector_size
func go_file_sector_size(p unsafe.Pointer) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 4096
	}
	return C.int(f.SectorSize())
}

//export go_file_device_characteristics
func go_file_device_characteristics(p unsafe.Pointer) C.int {
	f := getFileFromPtr(p)
	if f == nil {
		return 0
	}
	return C.int(f.DeviceCharacteristics())
}
