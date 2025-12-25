package vfs

import (
	"github.com/psanford/sqlite3vfs"
)

// Check interface compliance
var _ sqlite3vfs.File = &CheckFile{}

type CheckFile struct{}

func (f *CheckFile) Close() error                                           { return nil }
func (f *CheckFile) ReadAt(p []byte, off int64) (n int, err error)          { return 0, nil }
func (f *CheckFile) WriteAt(p []byte, off int64) (n int, err error)         { return 0, nil }
func (f *CheckFile) Truncate(size int64) error                              { return nil }
func (f *CheckFile) Sync(flags sqlite3vfs.SyncType) error                   { return nil }
func (f *CheckFile) FileSize() (int64, error)                               { return 0, nil }
func (f *CheckFile) Lock(elock sqlite3vfs.LockType) error                   { return nil }
func (f *CheckFile) Unlock(elock sqlite3vfs.LockType) error                 { return nil }
func (f *CheckFile) CheckReservedLock() (bool, error)                       { return false, nil }
func (f *CheckFile) SectorSize() int64                                      { return 0 }
func (f *CheckFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic { return 0 }

// If this compiles, then FileControl IS NOT required.
// If I add it and it compiles, it might be optional or I'm adding a method that isn't part of the interface (which is fine, but won't be called).
// I need to know if the interface *includes* it.
// Actually, if I add it to the struct, it doesn't hurt. The question is: will the library call it?
// I can check if the library has a `FileControl` op constant.
