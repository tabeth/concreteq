package store

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/psanford/sqlite3vfs"
)

// LockLevel approximates SQLite lock levels
type LockLevel int

const (
	LockNone      LockLevel = 0
	LockShared    LockLevel = 1
	LockReserved  LockLevel = 2
	LockPending   LockLevel = 3
	LockExclusive LockLevel = 4
)

// LockManager handles distributed locking on FDB.
type LockManager struct {
	db              fdb.Database
	lockSubspace    subspace.Subspace
	id              string
	currentLevel    LockLevel
	heartbeatCancel context.CancelFunc
	heartbeatWg     sync.WaitGroup
	config          Config
}

func NewLockManager(db fdb.Database, prefix tuple.Tuple, config Config) *LockManager {
	// Locks stored under (Prefix, "locks")
	// Keys:
	// "reserved" -> OwnerID
	// "pending" -> OwnerID
	// "exclusive" -> OwnerID
	// ("shared", OwnerID) -> timestamp
	return &LockManager{
		db:           db,
		lockSubspace: subspace.FromBytes(prefix.Pack()).Sub("locks"),
		id:           uuid.New().String(),
		currentLevel: LockNone,
		config:       config,
	}
}

// Lock attempts to upgrade the lock to the requested level.
// It blocks (with polling) until successful or error.
// Lock attempts to upgrade the lock to the requested level.
// It blocks (with polling) until successful or error.
// Lock attempts to upgrade the lock to the requested level.
// It blocks (with polling) until successful or error.
// For SHARED locks, snapshotVersion explicitly declares what version the reader is using.
func (lm *LockManager) Lock(ctx context.Context, level sqlite3vfs.LockType, snapshotVersion int64) error {

	target := LockLevel(level)

	// Poll interval
	ticker := time.NewTicker(lm.config.LockPollInterval)
	defer ticker.Stop()

	// State machine loop: Upgrade step-by-step
	for lm.currentLevel < target {
		var nextStep LockLevel
		if lm.currentLevel < LockShared {
			nextStep = LockShared
		} else if lm.currentLevel < LockReserved && target >= LockReserved {
			nextStep = LockReserved
		} else {
			nextStep = LockExclusive
		}

		// Retry loop for the current step
		stepSuccess := false
		for !stepSuccess {
			// Check context
			if err := ctx.Err(); err != nil {
				return err
			}

			// Try to acquire
			// Try to acquire
			ok, err := lm.tryLock(nextStep, snapshotVersion)

			if err != nil {
				return err
			}
			if ok {
				lm.currentLevel = nextStep
				lm.startHeartbeat() // Refresh/Start heartbeat logic
				stepSuccess = true
			} else {
				// Wait and retry
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
					// Retry
				}
			}
		}
	}

	return nil
}

func (lm *LockManager) startHeartbeat() {
	lm.stopHeartbeat() // Ensure no previous heartbeat running

	ctx, cancel := context.WithCancel(context.Background())
	lm.heartbeatCancel = cancel
	lm.heartbeatWg.Add(1)

	go func() {
		defer lm.heartbeatWg.Done()
		ticker := time.NewTicker(lm.config.LockHeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lm.refreshLease()
			}
		}
	}()
}

func (lm *LockManager) stopHeartbeat() {
	if lm.heartbeatCancel != nil {
		lm.heartbeatCancel()
		lm.heartbeatWg.Wait()
		lm.heartbeatCancel = nil
	}
}

func (lm *LockManager) refreshLease() {
	// Refresh all held locks
	_, err := lm.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		now := time.Now().UnixNano()
		expiry := now + lm.config.LockLeaseDuration.Nanoseconds()

		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, uint64(expiry))

		// Always refresh Shared if we have it
		if lm.currentLevel >= LockShared {
			// We need to preserve the Version if it exists.
			// However, we don't store the version in local state easily (except maybe we should?).
			// Or we just read the old value to get the version?
			// Reading inside refresh adds latency.
			// Let's rely on checking the key.
			key := lm.lockSubspace.Pack(tuple.Tuple{"shared", lm.id})
			existing := tr.Get(key).MustGet()

			if len(existing) == 16 {
				// Preserve version
				// New expiry + Old Version
				newVal := make([]byte, 16)
				binary.BigEndian.PutUint64(newVal[0:8], uint64(expiry))
				copy(newVal[8:16], existing[8:16])
				tr.Set(key, newVal)
			} else {
				// Setup default or overwrite?
				// If we are Shared, we SHOULD have a version.
				// But for backwards compat or non-versioned locks?
				// Just update expiry.
				tr.Set(key, val)
			}
		}

		// Refresh others if we own them
		if lm.currentLevel >= LockReserved {
			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"reserved"}), val)
		}
		if lm.currentLevel >= LockPending {
			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"pending"}), val)
		}
		if lm.currentLevel == LockExclusive {
			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"}), val)
		}
		return nil, nil
	})

	if err != nil {
		// Log warning?
	}
}

// isLocked checks if a lock key is present and valid (not expired)
// Returns ownerID (if available/stored) and validity.
// Current impl stores Expiry as value for single-slots (Reserved/Pending/Exclusive)
// For Shared: Key=("shared", OwnerID), Value=Expiry
// isLocked checks if a lock key is present and valid (not expired)
func (lm *LockManager) isLocked(val []byte) bool {
	if len(val) == 0 {
		return false // Not locked
	}

	if len(val) == 8 || len(val) == 16 {
		// First 8 bytes are expiry
		expiry := int64(binary.BigEndian.Uint64(val[0:8]))
		return time.Now().UnixNano() < expiry
	}

	// Assume legacy/invalid is valid lock to be safe?
	return true
}

// tryLock attempts a single state transition
func (lm *LockManager) tryLock(target LockLevel, snapshotVersion int64) (bool, error) {

	// Implementation based on SQLite "The Locking Protocol"

	// NONE -> SHARED
	if lm.currentLevel < LockShared && target == LockShared {
		// "To obtain a SHARED lock... obtain a PENDING lock... check for EXCLUSIVE."
		// Actually: "The SHARED lock is acquired if there is no PENDING lock and no EXCLUSIVE lock."

		_, err := lm.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			// Check Pending & Exclusive
			pending := tr.Get(lm.lockSubspace.Pack(tuple.Tuple{"pending"})).MustGet()
			if lm.isLocked(pending) {
				return nil, errors.New("blocked")
			}
			exclusive := tr.Get(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"})).MustGet()
			if lm.isLocked(exclusive) {
				return nil, errors.New("blocked")
			}

			// Acquire Shared
			now := time.Now().UnixNano()
			expiry := now + lm.config.LockLeaseDuration.Nanoseconds()

			// Store Expiry (8) + Version (8)
			val := make([]byte, 16)
			binary.BigEndian.PutUint64(val[0:8], uint64(expiry))
			binary.BigEndian.PutUint64(val[8:16], uint64(snapshotVersion))

			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"shared", lm.id}), val)
			return nil, nil
		})

		if err != nil {
			return false, nil // Blocked, retry
		}
		return true, nil
	}

	// SHARED -> RESERVED
	if lm.currentLevel == LockShared && target == LockReserved {
		// "A process with a SHARED lock can acquire a RESERVED lock if no other process has a RESERVED lock."
		_, err := lm.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			reserved := tr.Get(lm.lockSubspace.Pack(tuple.Tuple{"reserved"})).MustGet()
			if lm.isLocked(reserved) {
				// If reserved by US?
				// We don't store owner in value anymore, we store Expiry.
				// But we know we only upgrade Shared->Reserved.
				// If we see a valid RESERVED, it must be SOMEONE ELSE because we track our own level in `lm.currentLevel`.
				// However, what if we are recovering?
				// `lm.currentLevel` says we are Shared. So if Reserved is present, it's not us (unless we crashed and restarted with same ID... unlikely with UUID).
				return nil, errors.New("blocked")
			}
			// Acquire Reserved
			now := time.Now().UnixNano()
			expiry := now + lm.config.LockLeaseDuration.Nanoseconds()
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(expiry))

			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"reserved"}), val)
			return nil, nil
		})
		if err != nil {
			return false, nil
		}
		return true, nil
	}

	// SHARED -> EXCLUSIVE (Automatic escalation)
	// Must upgrade to RESERVED first.
	if lm.currentLevel == LockShared && target == LockExclusive {
		ok, err := lm.tryLock(LockReserved, snapshotVersion)
		if err != nil {

			return false, err
		}
		if !ok {
			return false, nil // Blocked on Reserved
		}
		// Success acquiring Reserved. Update internal state.
		lm.currentLevel = LockReserved
		// Fallthrough to RESERVED -> EXCLUSIVE
	}

	// RESERVED -> EXCLUSIVE (now handles default path from Shared->Reserved->Exclusive)
	if lm.currentLevel == LockReserved && target == LockExclusive {
		// 1. Acquire PENDING
		// "A process with a RESERVED lock can acquire a PENDING lock."
		// "This prevents new SHARED locks."

		_, err := lm.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			now := time.Now().UnixNano()
			expiry := now + lm.config.LockLeaseDuration.Nanoseconds()
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(expiry))

			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"pending"}), val)
			return nil, nil
		})
		if err != nil {
			return false, err
		}

		// 2. Wait for all *other* SHARED locks to clear.
		// We are holding PENDING, so nobody new can enter.

		// Check SHARED locks
		ready, err := lm.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
			rr := r.GetRange(lm.lockSubspace.Sub("shared"), fdb.RangeOptions{})
			iter := rr.Iterator()
			for iter.Advance() {
				kv, err := iter.Get()
				if err != nil {
					return false, err
				}

				// Parse OwnerID from key: ("shared", OwnerID)
				t, err := lm.lockSubspace.Sub("shared").Unpack(kv.Key)
				if err != nil {
					continue
				}
				if len(t) > 0 {
					owner := t[0].(string)
					if owner != lm.id {
						if lm.isLocked(kv.Value) {
							return false, nil // Other shared lock exists and valid
						}
						// If expired, assume dead and ignore
					}
				}
			}
			return true, nil
		})
		if err != nil {
			return false, err
		}

		if !ready.(bool) {
			return false, nil // Still waiting for shared locks to drain
		}

		// 3. Acquire EXCLUSIVE
		_, err = lm.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			now := time.Now().UnixNano()
			expiry := now + lm.config.LockLeaseDuration.Nanoseconds()
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(expiry))

			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"}), val)

			// Clear Shared, Reserved, Pending for this owner upon acquiring Exclusive
			tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"shared", lm.id}))

			// We might not own Reserved/Pending if we jumped?
			// But if we own them, we should clear them.
			// Since we don't store ID in Reserved/Pending value anymore, we can't check ownership easily in FDB values.
			// However, local `currentLevel` logic ensures we likely own them if we are upgrading.
			// Or we just clear them anyway?
			// If we clear them, and we didn't own them... we might clear someone else's Reserved?
			// Reserved is single-slot. If we are getting Exclusive, we MUST own Reserved (or have stolen it).
			// So yes, clear them.
			tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"reserved"}))
			tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"pending"}))

			return nil, nil
		})

		if err != nil {
			return false, nil
		}

		return true, nil
	}

	// SHARED -> EXCLUSIVE (Direct upgrade? usually goes via Reserved)
	// If we receive this, we assume the caller knows what they are doing.
	// But SQLite usually steps through.

	return false, nil
}

// Unlock releases locks down to the requested level.
func (lm *LockManager) Unlock(level sqlite3vfs.LockType) error {
	target := LockLevel(level)

	if target == LockNone {
		// Release ALL locks for this ID
		_, err := lm.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"shared", lm.id}))

			// Clear Reserved/Pending/Exclusive ONLY if we own them?
			// Since we don't store ID in value, we rely on local state `lm.currentLevel`.
			// If `lm.currentLevel` >= Reserved, we own it.
			if lm.currentLevel >= LockReserved {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"reserved"}))
			}
			if lm.currentLevel >= LockPending {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"pending"}))
			}
			if lm.currentLevel == LockExclusive {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"}))
			}

			return nil, nil
		})
		if err != nil {
			return err
		}
		lm.currentLevel = LockNone
		lm.stopHeartbeat()
		return nil
	}

	// Shared state maintenance if downgrading from Exclusive -> Shared
	if lm.currentLevel > LockShared && target == LockShared {
		_, err := lm.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			// We keep Shared lock
			// We release Reserved, Pending, Exclusive
			if lm.currentLevel >= LockReserved {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"reserved"}))
			}
			if lm.currentLevel >= LockPending {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"pending"}))
			}
			if lm.currentLevel == LockExclusive {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"}))
			}
			// Ensure Shared lock is set (it should be, but just in case)
			// We need to restore it with SOME version.
			// Ideally we shouldn't be downgrading without knowing our version.
			// But Unlock() doesn't take version.
			// AND we are downgrading from Exclusive. We probably want to keep the version we had when we were Shared?
			// But we cleared it.
			// For now, let's just allow it (Version 0?) or reuse current?
			// Realistically, SQLite downgrades after a commit. It essentially "is done" with the transaction but keeps Shared for potentially new read.
			// Usually it downgrades to NONE.
			// If it downgrades to Shared, it might be starting a new read?
			// Let's use 0 (safest - "oldest possible" so valid for everything, but might block vacuum? No, 0 means "reading genesis". Safe.)
			// Actually 0 might block Vacuum from deleting version 1.
			// Maybe use Current Time as version? No.
			// If we don't know, we shouldn't claim a version.
			// Or we should read the current version?
			// Let's use 0 for now. It assumes "I might need old data".

			now := time.Now().UnixNano()
			expiry := now + lm.config.LockLeaseDuration.Nanoseconds()
			val := make([]byte, 16)
			binary.BigEndian.PutUint64(val[0:8], uint64(expiry))
			binary.BigEndian.PutUint64(val[8:16], uint64(0))

			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"shared", lm.id}), val)
			return nil, nil

		})
		if err != nil {
			return err
		}
		lm.currentLevel = LockShared
		// We still need heartbeat for Shared!
		// heartbeat is already running, refreshLease() handles Shared.
		return nil
	}

	return nil
}

func (lm *LockManager) CheckReserved() (bool, error) {
	res, err := lm.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		reserved := r.Get(lm.lockSubspace.Pack(tuple.Tuple{"reserved"})).MustGet()
		return lm.isLocked(reserved), nil
	})
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}

// ParseExpiry helps tests parse the expiry time
// ParseExpiry helps tests parse the expiry time
func ParseExpiry(val []byte) int64 {
	if len(val) == 16 {
		return int64(binary.BigEndian.Uint64(val[0:8]))
	}
	if len(val) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(val))
}

// Kill stops the heartbeat without releasing locks.
// This is used for testing "client crash" scenarios.
func (lm *LockManager) Kill() {
	lm.stopHeartbeat()
}
