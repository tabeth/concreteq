package vfs

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/psanford/sqlite3vfs"
	"github.com/tabeth/concretesql/store"
)

// OpCode for SQLITE_FCNTL_PRAGMA
const SQLITE_FCNTL_PRAGMA = 14

type File struct {
	name      string
	pageStore *store.PageStore
	mu        sync.Mutex

	// Real distributed locking via LockManager
	lockManager *store.LockManager
	lock        sqlite3vfs.LockType

	baseVersion int64
	fileSize    int64
	pageSize    int
	dirtyPages  map[int][]byte
	requestID   string
}

func NewFile(name string, ps *store.PageStore, lm *store.LockManager, initPageSize int) (*File, error) {
	fmt.Printf("NewFile: %s\n", name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fetch current version on open
	state, err := ps.CurrentVersion(ctx)
	if err != nil {
		fmt.Printf("CurrentVersion failed: %v\n", err)
		return nil, err
	}

	pageSize := state.PageSize
	// If existing DB has no page size (or default), logic handles it.
	// state.PageSize defaults to DefaultPageSize inside CurrentVersion if missing in meta.
	// But if it's a NEW DB (Version 0, Size 0), maybe we want to set it?

	if state.Version == 0 && state.Size == 0 {
		// New Database
		if initPageSize > 0 {
			pageSize = initPageSize
			// Set it immediately
			if err := ps.SetPageSize(ctx, pageSize); err != nil {
				return nil, err
			}
		}
	} else {
		// Existing Database
		if initPageSize > 0 && initPageSize != pageSize {
			log.Printf("Warning: Configured pageSize %d ignores existing DB pageSize %d", initPageSize, pageSize)
		}
	}

	return &File{
		name:        name,
		pageStore:   ps,
		lockManager: lm,
		baseVersion: state.Version,
		fileSize:    state.Size,
		pageSize:    pageSize,
		dirtyPages:  make(map[int][]byte),
	}, nil
}

func (f *File) Close() error {
	return nil
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// SQLite typically reads in page sizes.
	// We calculate which page touches this range.
	// Warning: ReadAt might cross page boundaries (unlikely in SQLite main db file, but possible).

	// For PoC robustness, we handle arbitrary reads by reading all affected pages.
	pageID := int(off / int64(f.pageSize))
	pageOffset := int(off % int64(f.pageSize))

	// Assuming read fits in one page for now?
	// standard SQLite reads are exactly PageSize usually, except header.
	// Let's implement full logic.

	totalRead := 0
	for totalRead < len(p) {
		remaining := len(p) - totalRead

		// Check dirty cache first
		data, ok := f.dirtyPages[pageID]
		if !ok {
			// Fetch from store
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var errRead error
			data, errRead = f.pageStore.ReadPage(ctx, f.baseVersion, pageID)
			cancel()
			if errRead != nil {
				return totalRead, errRead
			}
		}

		if data == nil {
			// Zero filled if page doesn't exist
			data = make([]byte, f.pageSize)
		}

		// Copy relevant part
		available := len(data) - pageOffset
		if available > remaining {
			available = remaining
		}

		copy(p[totalRead:], data[pageOffset:pageOffset+available])

		totalRead += available
		pageID++
		pageOffset = 0 // Next page starts at 0
	}

	return totalRead, nil
}

func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Same logic as ReadAt, but we update dirtyPages.
	// Note: We might need to Read-Before-Write if we are doing partial page writes.

	pageID := int(off / int64(f.pageSize))
	pageOffset := int(off % int64(f.pageSize))

	// Intercept SQLite Header Write (Page 0, Offset 0-100)
	// SQLite writes 100 bytes header. Page Size is at offset 16 (2 bytes, big-endian).
	// If we are writing to Page 0, we should check if pageSize changed.
	if pageID == 0 {
		// Snoop the data to find pageSize
		// data being written to file is `p`.
		headerOff := int64(16)
		if off <= headerOff && off+int64(len(p)) >= headerOff+2 {
			// Extract bytes
			rel := headerOff - off
			b1 := p[rel]
			b2 := p[rel+1]
			val := binary.BigEndian.Uint16([]byte{b1, b2})

			newPageSize := int(val)
			if val == 1 {
				newPageSize = 65536
			}

			// Validate power of two and range
			isValid := newPageSize >= 512 && newPageSize <= 65536 && (newPageSize&(newPageSize-1)) == 0

			if isValid && newPageSize != f.pageSize {
				log.Printf("Detected PageSize change from %d to %d", f.pageSize, newPageSize)
				// Persist it!
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := f.pageStore.SetPageSize(ctx, newPageSize); err != nil {
					cancel()
					return 0, err
				}
				cancel()
				f.pageSize = newPageSize
			}
		}
	}

	totalWritten := 0
	for totalWritten < len(p) {
		remaining := len(p) - totalWritten

		// We ALWAYS need the existing page data if we are doing partial write.
		// unless we are writing a FULL page.

		isFullPageWrite := (pageOffset == 0 && remaining >= f.pageSize)

		var data []byte
		var ok bool

		if !isFullPageWrite {
			// Retrieve existing data
			data, ok = f.dirtyPages[pageID]
			if !ok {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				var errRead error
				data, errRead = f.pageStore.ReadPage(ctx, f.baseVersion, pageID)
				cancel()
				if errRead != nil {
					return totalWritten, errRead
				}
				if data == nil {
					data = make([]byte, f.pageSize)
				}
				// Store in buffer immediately so we modify it
				// Important: COPY it if it came from store, though ReadPage likely returns fresh slice.
			}
		} else {
			data = make([]byte, f.pageSize)
		}

		// Write to data
		available := len(data) - pageOffset
		if available > remaining {
			available = remaining
		}

		copy(data[pageOffset:pageOffset+available], p[totalWritten:])

		// Update dirty map
		f.dirtyPages[pageID] = data

		totalWritten += available
		pageID++
		pageOffset = 0
	}

	// Update fileSize if we extended the file
	currentEnd := int64(off) + int64(totalWritten)
	if currentEnd > f.fileSize {
		f.fileSize = currentEnd
	}

	return totalWritten, nil
}

// Truncate handles file value truncation.
func (f *File) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fileSize = size
	return nil
}

func (f *File) Sync(flags sqlite3vfs.SyncType) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// If dirty pages empty, we might still need to update version if we changed size?
	// But usually size change comes with page writes.
	// However, if we only truncated, dirtyPages might be empty but size changed.
	// Truncate updates f.fileSize immediately.
	// We should probably always commit if fileSize changed OR dirtyPages > 0.
	// But let's stick to dirtyPages check for now, assuming Truncate also marks something dirty or Sync handles it.
	// Actually Truncate just updates generic size.
	// For correctness, we should check if we have pending changes.

	if len(f.dirtyPages) == 0 {
		return nil
	}

	// Allocate new version
	// In a real system, we'd use a unique ID generator (Snowflake) or FDB atomic add.
	// Here: simplistic sequential version based on current time or just increment.
	// Issue: Multiple writers.
	// Solution: "Optimistic Concurrency" on Version Key.
	// But we are single writer (EXCLUSIVE lock).

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Generate new version ID
	// We can just use nanoseconds to be mostly unique and strictly increasing.
	newVersion := time.Now().UnixNano()
	if newVersion <= f.baseVersion {
		newVersion = f.baseVersion + 1
	}

	// 2. Write pages
	if err := f.pageStore.WritePages(ctx, newVersion, f.dirtyPages); err != nil {
		return err
	}

	// 3. Commit version
	// Check if baseVersion has changed? (Conflict detection)
	// If we hold Exclusive Lock, nobody else should have changed it?
	// In distributed FDB land, we should probably check.

	state, err := f.pageStore.CurrentVersion(ctx)
	if err != nil {
		return err
	}
	currentV := state.Version
	if currentV != f.baseVersion {
		// Optimistic Locking Failure!
		// Someone else committed?
		// For PoC, allow "Last Writer Wins" or "Error".
		log.Printf("Warning: Version mismatch. Expected %d, got %d. Overwriting...", f.baseVersion, currentV)
	}

	if err := f.pageStore.SetVersionAndSize(ctx, newVersion, f.fileSize, f.requestID, f.baseVersion); err != nil {
		return err
	}

	// Reset requestID after successful sync (it was consumed)
	f.requestID = ""

	// 4. Update state
	f.baseVersion = newVersion
	f.dirtyPages = make(map[int][]byte)

	return nil
}

func (f *File) FileSize() (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fileSize, nil
}

// Lock implements the locking protocol.
func (f *File) Lock(elock sqlite3vfs.LockType) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Call Distributed Lock Manager
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // 10s wait for lock
	defer cancel()

	if err := f.lockManager.Lock(ctx, elock, f.baseVersion); err != nil {
		return err
	}

	// Update view of the world if we just acquired a SHARED lock (or upgraded/re-acquired)
	// This ensures we are reading the latest committed version.
	// Only do this if we are transitioning from UNLOCKED state.
	if elock >= sqlite3vfs.LockShared && f.lock < sqlite3vfs.LockShared {
		state, err := f.pageStore.CurrentVersion(ctx)
		if err != nil {
			// If we fail to read version, we should probably fail the lock?
			// Or just log warning? Failing lock is safer.
			// But we already acquired lock in LockManager. We should unlock?
			f.lockManager.Unlock(elock) // Best effort revert?
			return err
		}
		f.baseVersion = state.Version
		f.fileSize = state.Size
		// Also update pageSize if it changed (and is valid)
		if state.PageSize > 0 && state.PageSize != f.pageSize {
			f.pageSize = state.PageSize
		}
	}

	f.lock = elock
	return nil
}

func (f *File) Unlock(elock sqlite3vfs.LockType) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.lockManager.Unlock(elock); err != nil {
		return err
	}

	f.lock = elock
	return nil
}

func (f *File) CheckReservedLock() (bool, error) {
	// Check local or remote? calling CheckReservedLock usually means "is there a reserved lock anywhere?"
	// Used by SQLite to check if other connection has reserved lock.
	return f.lockManager.CheckReserved() // This calls FDB
}

func (f *File) SectorSize() int64 {
	return int64(f.pageSize)
}

func (f *File) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.DeviceCharacteristic(0)
}

// FileControl handles custom opcodes, specifically PRAGMA interception
func (f *File) FileControl(op int, arg interface{}) (bool, error) {
	if op == SQLITE_FCNTL_PRAGMA {
		// arg is *[]string typically: [pragma_name, pragma_value]
		// But in sqlite3vfs it might be different.
		// wait, the library signature: FileControl(op int, arg interface{}) (bool, error)
		// For FCNTL_PRAGMA, SQLite passes a char** (array of strings).
		// psanford/sqlite3vfs likely marshals this.
		// Let's check what arg is.

		// If arg is a pointer to a string slice?
		if args, ok := arg.(*[]string); ok && args != nil && len(*args) >= 2 {
			key := (*args)[1] // pragma name
			// value might be empty-string if no value provided

			if key == "concrete_request_id" {
				if len(*args) > 2 {
					val := (*args)[2]
					f.mu.Lock()
					f.requestID = val
					f.mu.Unlock()
					log.Printf("Set RequestID: %s", val)
				}
				// Return true to indicate we handled it?
				// SQLite PRAGMA handling usually returns valid SQLITE_OK if handled.
				return true, nil
			}
		}
	}
	return false, nil
}
