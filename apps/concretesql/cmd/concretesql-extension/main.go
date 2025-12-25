package main

/*
#cgo CFLAGS: -I.
#include <string.h>
#include <stdlib.h>
#include "sqlite3ext.h"

// function implemented in extension_init.c
int register_concretesql_vfs(const char *name);
*/
import "C"

import (
	"log"
	"unsafe"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	// We need access to internals of sqlite3vfs or we replicate the mapping logic.
	// Since sqlite3vfs registers via C.sqlite3_vfs_register (which is statically linked),
	// we CANNOT use its RegisterVFS method if we want to register to HOST sqlite.
	//
	// However, we CAN reuse its VFS implementation struct `sqlite3vfs.SQLiteVFS` if it's exported? No, it's hidden.
	// But `sqlite3vfs.RegisterVFS` takes a `VFS` interface.
	// We need to implement a C-proxy here that calls `sqlite3_vfs_register` from `pApi`.
)

func main() {
	// Required for buildmode=c-shared, but ignored.
}

//export InitConcreteSQLVFS
func InitConcreteSQLVFS(db *C.sqlite3, pzErrMsg **C.char, pApi *C.sqlite3_api_routines) C.int {
	// 1. Initialize FDB
	// We assume 620.
	// If multiple extensions use FDB, this might panic?
	// Use try-catch or Once.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from FDB init panic: %v", r)
		}
	}()
	fdb.MustAPIVersion(620)
	fdbDB := fdb.MustOpenDefault()
	globalDB = &fdbDB

	// 2. Create VFS instance
	// (Handled via C bridge callbacks)

	// 3. Register VFS
	// This is the tricky part. We need to call pApi->vfs_register.
	// We need to construct a C sqlite3_vfs struct and pass it.
	// We can't easily access `sqlite3_vfs` without C definition.
	// And we need function pointers back to Go.

	// Implementing a full VFS shim in CGO here is roughly 200 lines of boilerplate.
	// For this PoC, we will implement a minimal C shim that calls back to Go functions.

	cName := C.CString("concretesql")
	defer C.free(unsafe.Pointer(cName))
	res := C.register_concretesql_vfs(cName)
	return res
}

// TODO: Implement VFS interface methods here and export them via Gateway?
// Or we need `sqlite3vfs` to support using external API.
//
// Strategy Pivot:
// Writing a full VFS shim in 10 minutes is risky.
// Can we "Patch" `github.com/psanford/sqlite3vfs` to use our API pointer?
// Not easily.
//
// Plan B:
// Copy the core logic of `FDBVFS` (Open, Read, Write) which we already have.
// Use a C-file in this package to define the `sqlite3_vfs` struct and functions.
// These C functions call exported Go functions.
//
// Let's create `vfs_bridge.c` and `vfs_bridge.go`.
