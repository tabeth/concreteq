package vfs

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/psanford/sqlite3vfs"
	"github.com/tabeth/concretesql/store"
)

type File struct {
	name      string
	pageStore *store.PageStore
	mu        sync.Mutex

	// Real distributed locking via LockManager
	lockManager *store.LockManager
	lock        sqlite3vfs.LockType

	baseVersion int64
	fileSize    int64
	dirtyPages  map[int][]byte
}

func NewFile(name string, ps *store.PageStore, lm *store.LockManager) (*File, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fetch current version on open
	v, size, err := ps.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}

	return &File{
		name:        name,
		pageStore:   ps,
		lockManager: lm,
		baseVersion: v,
		fileSize:    size,
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
	pageID := int(off / store.PageSize)
	pageOffset := int(off % store.PageSize)

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
			data = make([]byte, store.PageSize)
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

	pageID := int(off / store.PageSize)
	pageOffset := int(off % store.PageSize)

	totalWritten := 0
	for totalWritten < len(p) {
		remaining := len(p) - totalWritten

		// We ALWAYS need the existing page data if we are doing partial write.
		// unless we are writing a FULL page.

		isFullPageWrite := (pageOffset == 0 && remaining >= store.PageSize)

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
					data = make([]byte, store.PageSize)
				}
				// Store in buffer immediately so we modify it
				// Important: COPY it if it came from store, though ReadPage likely returns fresh slice.
			}
		} else {
			data = make([]byte, store.PageSize)
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

	currentV, _, err := f.pageStore.CurrentVersion(ctx)
	if err != nil {
		return err
	}
	if currentV != f.baseVersion {
		// Optimistic Locking Failure!
		// Someone else committed?
		// For PoC, allow "Last Writer Wins" or "Error".
		log.Printf("Warning: Version mismatch. Expected %d, got %d. Overwriting...", f.baseVersion, currentV)
	}

	if err := f.pageStore.SetVersionAndSize(ctx, newVersion, f.fileSize); err != nil {
		return err
	}

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

	if err := f.lockManager.Lock(ctx, elock); err != nil {
		return err
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
	return store.PageSize
}

func (f *File) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.DeviceCharacteristic(0)
}
