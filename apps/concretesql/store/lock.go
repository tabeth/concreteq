package store

import (
	"context"
	"errors"
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
	db           fdb.Database
	lockSubspace subspace.Subspace
	id           string
	currentLevel LockLevel
}

func NewLockManager(db fdb.Database, prefix tuple.Tuple) *LockManager {
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
	}
}

// Lock attempts to upgrade the lock to the requested level.
// It blocks (with polling) until successful or error.
func (lm *LockManager) Lock(ctx context.Context, level sqlite3vfs.LockType) error {
	// SQLite transitions: NONE -> SHARED -> RESERVED -> EXCLUSIVE
	// (Pending is intermediate, usually handled internally by LockReserved->LockExclusive)

	// Since we are simulating blocking locks, we might need a loop.

	target := LockLevel(level)

	// Simple retry loop
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ok, err := lm.tryLock(target)
			if err != nil {
				return err
			}
			if ok {
				lm.currentLevel = target
				return nil
			}
			// Wait and retry
		}
	}
}

// tryLock attempts a single state transition
func (lm *LockManager) tryLock(target LockLevel) (bool, error) {
	// Implementation based on SQLite "The Locking Protocol"

	// NONE -> SHARED
	if lm.currentLevel < LockShared && target == LockShared {
		// "To obtain a SHARED lock... obtain a PENDING lock... check for EXCLUSIVE."
		// Actually: "The SHARED lock is acquired if there is no PENDING lock and no EXCLUSIVE lock."

		_, err := lm.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			// Check Pending & Exclusive
			pending := tr.Get(lm.lockSubspace.Pack(tuple.Tuple{"pending"})).MustGet()
			if len(pending) > 0 {
				return nil, errors.New("blocked")
			}
			exclusive := tr.Get(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"})).MustGet()
			if len(exclusive) > 0 {
				return nil, errors.New("blocked")
			}

			// Acquire Shared
			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"shared", lm.id}), []byte(time.Now().String()))
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
			if len(reserved) > 0 && string(reserved) != lm.id {
				return nil, errors.New("blocked")
			}
			// Acquire Reserved
			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"reserved"}), []byte(lm.id))
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
		ok, err := lm.tryLock(LockReserved)
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
			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"pending"}), []byte(lm.id))
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
						return false, nil // Other shared lock exists
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
			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"}), []byte(lm.id))
			// Clear Reserved, Pending? FDB state machine logic.
			// Usually we keep them or clear them.
			// Cleanup: We don't need Reserved/Pending if we have Exclusive.
			// But leaving them prevents others.
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

			// Clear Reserved/Pending/Exclusive ONLY if we own them
			reserved := tr.Get(lm.lockSubspace.Pack(tuple.Tuple{"reserved"})).MustGet()
			if string(reserved) == lm.id {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"reserved"}))
			}
			pending := tr.Get(lm.lockSubspace.Pack(tuple.Tuple{"pending"})).MustGet()
			if string(pending) == lm.id {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"pending"}))
			}
			exclusive := tr.Get(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"})).MustGet()
			if string(exclusive) == lm.id {
				tr.Clear(lm.lockSubspace.Pack(tuple.Tuple{"exclusive"}))
			}

			return nil, nil
		})
		if err != nil {
			return err
		}
		lm.currentLevel = LockNone
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
			tr.Set(lm.lockSubspace.Pack(tuple.Tuple{"shared", lm.id}), []byte("downgrade"))
			return nil, nil
		})
		if err != nil {
			return err
		}
		lm.currentLevel = LockShared
		return nil
	}

	return nil
}

func (lm *LockManager) CheckReserved() (bool, error) {
	res, err := lm.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		reserved := r.Get(lm.lockSubspace.Pack(tuple.Tuple{"reserved"})).MustGet()
		return len(reserved) > 0, nil
	})
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}
