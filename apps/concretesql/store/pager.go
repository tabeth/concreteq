package store

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	DefaultPageSize = 4096
	MaxTxBytes      = 9 * 1024 * 1024 // 9MB safety limit
)

type PageStore struct {
	db       fdb.Database
	subspace subspace.Subspace
}

func NewPageStore(db fdb.Database, prefix tuple.Tuple) *PageStore {
	// Correctly create a subspace from the tuple
	return &PageStore{
		db:       db,
		subspace: subspace.FromBytes(prefix.Pack()),
	}
}

type DBState struct {
	Version  int64
	Size     int64
	PageSize int
}

// CurrentVersion reads the current active version ID, file size, and page size of the database.
func (ps *PageStore) CurrentVersion(ctx context.Context) (DBState, error) {
	res, err := ps.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		val := r.Get(ps.subspace.Pack(tuple.Tuple{"meta", "version"})).MustGet()
		sizeBytes := r.Get(ps.subspace.Pack(tuple.Tuple{"meta", "size"})).MustGet()
		pageSizeBytes := r.Get(ps.subspace.Pack(tuple.Tuple{"meta", "pagesize"})).MustGet()

		var s DBState
		if len(val) > 0 {
			s.Version = int64(binary.BigEndian.Uint64(val))
		}
		if len(sizeBytes) > 0 {
			s.Size = int64(binary.BigEndian.Uint64(sizeBytes))
		}
		if len(pageSizeBytes) > 0 {
			s.PageSize = int(binary.BigEndian.Uint64(pageSizeBytes))
		} else {
			s.PageSize = DefaultPageSize // Fallback for existing DBs
		}
		return s, nil
	})
	if err != nil {
		return DBState{}, err
	}
	s := res.(DBState)
	return s, nil
}

// SetVersionAndSize updates both the version and the file size atomically.
// It also handles Idempotency if requestID is provided.
func (ps *PageStore) SetVersionAndSize(ctx context.Context, version int64, size int64, requestID string, baseVersion int64) error {
	_, err := ps.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Idempotency Check
		if requestID != "" {
			// Key: ("tx_map", requestID)
			ik := ps.subspace.Pack(tuple.Tuple{"tx_map", requestID})
			val := tr.Get(ik).MustGet()
			if len(val) > 0 {
				// Already committed!
				// We should ideally return the version we wrote, but returning nil error is enough for "Success".
				return nil, nil
			}
		}

		// 2. Optimistic Concurrency Check (ABA Protection)
		// Read current version
		vk := ps.subspace.Pack(tuple.Tuple{"meta", "version"})
		curVal := tr.Get(vk).MustGet()
		var currentVersion int64
		if len(curVal) > 0 {
			currentVersion = int64(binary.BigEndian.Uint64(curVal))
		}

		// If baseVersion check is requested (baseVersion >= 0 or similar convention? logic implies strict check)
		// Assuming baseVersion is what the potential writer *saw* when they started.
		if currentVersion != baseVersion {
			return nil, errors.New("conflict: database version changed")
		}

		// 3. Write New State
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(version))
		tr.Set(vk, buf)

		bufSize := make([]byte, 8)
		binary.BigEndian.PutUint64(bufSize, uint64(size))
		tr.Set(ps.subspace.Pack(tuple.Tuple{"meta", "size"}), bufSize)

		// 4. Store Idempotency Key
		if requestID != "" {
			ik := ps.subspace.Pack(tuple.Tuple{"tx_map", requestID})
			// Store expiry or just timestamp? Plan said "CommitVersion" or "LeaseExpiry".
			// Let's store timestamp for GC.
			// Expiry = Now + 1 Hour
			expiry := time.Now().Add(1 * time.Hour).UnixNano()
			exBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(exBuf, uint64(expiry))
			tr.Set(ik, exBuf)
		}

		return nil, nil
	})
	return err
}

// GarbageCollectTxMap cleans up old idempotency keys
func (ps *PageStore) GarbageCollectTxMap(ctx context.Context) error {
	_, err := ps.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Scan all tx_map

		r := ps.subspace.Sub("tx_map")
		// GetRange
		rr := tr.GetRange(r, fdb.RangeOptions{})
		iter := rr.Iterator()

		now := time.Now().UnixNano()

		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}
			if len(kv.Value) == 8 {
				expiry := int64(binary.BigEndian.Uint64(kv.Value))
				if now > expiry {
					tr.Clear(kv.Key)
				}
			}
		}
		return nil, nil
	})
	return err
}

// ReadPage reads the page content for a specific PageID valid at the given snapshot version.
// It searches for the latest version V of the page such that V <= snapshotVersion.
func (ps *PageStore) ReadPage(ctx context.Context, snapshotVersion int64, pageID int) ([]byte, error) {
	data, err := ps.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		// Keys are stored as: ("data", PageID, VersionID)
		// We want the range [("data", PageID, 0), ("data", PageID, snapshotVersion + 1))
		// And we want the last key in that range (highest version).

		begin := ps.subspace.Pack(tuple.Tuple{"data", pageID})
		end := ps.subspace.Pack(tuple.Tuple{"data", pageID, snapshotVersion + 1})

		// Reverse scan, limit 1
		rr := r.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Reverse: true, Limit: 1})
		iter := rr.Iterator()
		if iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}
			return kv.Value, nil
		}
		// Not found means the page was never written, so return nil (empty).
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	return data.([]byte), nil
}

// WritePages writes a batch of pages for a *specific target version*.
// This typically happens during a transaction.
// Pre-condition: These pages belong to 'targetVersion'.
// If targetVersion is committed later, these pages become visible.
func (ps *PageStore) WritePages(ctx context.Context, targetVersion int64, pages map[int][]byte) error {
	// We must batch these writes because 'pages' map could be larger than 10MB.
	type kv struct {
		k fdb.Key
		v []byte
	}

	var batch []kv
	var batchSize int

	commit := func(b []kv) error {
		_, err := ps.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			for _, item := range b {
				tr.Set(item.k, item.v)
			}
			return nil, nil
		})
		return err
	}

	for pageID, content := range pages {
		// Key: ("data", PageID, VersionID)
		key := ps.subspace.Pack(tuple.Tuple{"data", pageID, targetVersion})

		// Optimization: If content is nil or empty, do we delete?
		// SQLite might truncate. For now, let's assume valid data.
		// If we supported truncation/deletion, we might need a "tombstone" or just not writing it if it's beyond EOF.
		// But for VFS Write(), we usually get 4096 bytes.

		batch = append(batch, kv{k: key, v: content})
		batchSize += len(key) + len(content)

		if batchSize >= MaxTxBytes {
			if err := commit(batch); err != nil {
				return err
			}
			batch = nil
			batchSize = 0
		}
	}

	if len(batch) > 0 {
		return commit(batch)
	}

	return nil
}

// Vacuum cleans up pages that are no longer reachable.
// It removes:
// 1. Pages with VersionID > CurrentVersion (Failed transactions / Crashes)
// 2. Pages with VersionID < Current minVersion (Old history) -- implicit if we just keep latest.
// For PoC: We will aggressively prune "shadowed" pages.
// Rule: For a given PageID, if we have versions V1, V2, V3 where V1 < V2 < V3 <= CurrentVersion.
// Then V1 is obsolete because V2 shadows it.
// V2 is valid for snapshots between V2 and V3.
// If minActiveVersion > V2, then V2 is also obsolete? No, V2 is needed for current state.
// We only delete V1 if we are sure nobody reads V < V2.
// Vacuum cleans up pages that are no longer reachable.
// Vacuum cleans up pages that are no longer reachable.
// It performs a chunked scan to avoid transaction timeouts.
func (ps *PageStore) Vacuum(ctx context.Context) error {
	state, err := ps.CurrentVersion(ctx)
	if err != nil {
		return err
	}
	currentVersion := state.Version

	// 1. Determine Minimum Active Version from Shared Locks
	// Default to currentVersion if no readers.
	// If there are readers with Version < currentVersion, we must preserve pages for them.
	minActiveVersion := currentVersion

	// Function to scan shared locks
	// We need to access locks subspace. PageStore has "db" and "subspace" (prefix).
	// LockManager uses prefix + "locks".
	// We reconstruct that path.
	lockSharedSubspace := ps.subspace.Sub("locks", "shared")

	_, err = ps.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		rr := r.GetRange(lockSharedSubspace, fdb.RangeOptions{})
		iter := rr.Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			// Value format: [Expiry 8b][Version 8b]
			if len(kv.Value) == 16 {
				expiry := int64(binary.BigEndian.Uint64(kv.Value[0:8]))
				if time.Now().UnixNano() < expiry {
					// Active Lock
					ver := int64(binary.BigEndian.Uint64(kv.Value[8:16]))
					// Version 0 means "reading genesis" or "unknown/safe".
					// If 0, we must preserve EVERYTHING from 0? That prevents any GC.
					// Let's assume 0 is valid and implies full history retention if active.
					if ver < minActiveVersion {
						minActiveVersion = ver
					}
				}
			}
		}
		return nil, nil
	})
	if err != nil {
		return err
	}

	// Manual range construction
	begin := ps.subspace.Sub("data").FDBKey()
	// end is prefix + 0xFF
	end := make([]byte, len(begin)+1)
	copy(end, begin)
	end[len(begin)] = 0xFF

	// State carried across chunks
	var lastKeptKey fdb.Key
	var lastKeptPageID int = -1

	// Iterator key
	currentKeySelector := fdb.FirstGreaterOrEqual(begin)

	for {
		// Run chunk
		// We need to return: nextSelector, done, newLastKeptKey, newLastKeptPageID, error
		// But we can update variables in loop if we use a closure?
		// No, FDB transaction retries. We must not mutate external state until success.

		type chunkResult struct {
			nextSel    fdb.KeySelector
			done       bool
			keptKey    fdb.Key
			keptPageID int
		}

		res, err := ps.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			// Read limited number of keys
			// Use SelectorRange
			r := fdb.SelectorRange{
				Begin: currentKeySelector,
				End:   fdb.FirstGreaterOrEqual(fdb.Key(end)),
			}
			iter := tr.GetRange(r, fdb.RangeOptions{Limit: 1000}).Iterator()

			// Local state for this transaction
			lKeptKey := lastKeptKey
			lKeptPageID := lastKeptPageID

			count := 0
			var lastSeenKey fdb.Key

			for iter.Advance() {
				count++
				kv, err := iter.Get()
				if err != nil {
					return nil, err
				}
				lastSeenKey = kv.Key

				// Parse Key
				t, err := ps.subspace.Unpack(kv.Key)
				if err != nil {
					continue
				} // Should not happen in "data" subspace
				if len(t) != 3 {
					continue
				}

				// t: ("data", PageID, VersionID)
				pageID := int(t[1].(int64))
				ver := t[2].(int64)

				if pageID != lKeptPageID {
					// New Page detected.
					// Previous page's last kept key is effectively the "Tail" (latest valid).
					// We don't delete it.
					// Reset state for new page
					lKeptPageID = pageID
					lKeptKey = nil
				}

				if ver > currentVersion {
					// Future version -> Garbage
					tr.Clear(kv.Key)
				} else {
					// Valid version candidate
					// Rules:
					// 1. We must keep the version `V` that satisfies `V <= minActiveVersion` and is the *latest* such version.
					//    (This is the version the active reader sees).
					// 2. We must keep the version `V` that satisfies `V <= currentVersion` and is the *latest* such version.
					//    (This is the current state).

					// Actually, simplified rule:
					// A version `V` is needed if there exists a Snapshot S (where S is minActiveVersion OR currentVersion)
					// such that `V` is the visible version for S.
					// `V` is visible for S if `V <= S` and there is no `V'` such that `V < V' <= S`.

					// Since we iterate versions, detecting this "next" relationship is hard in a single pass unless we look ahead or keep multiple candidates.
					// But we only care about `minActiveVersion` and `currentVersion`.
					// Wait, if we have multiple readers at different older versions, we need `minActive` to be the *minimum* of them.
					// If we preserve up to `minActive`, do we cover everyone?
					// Yes, because if we keep `V` for `minActive`, and some other reader is at `minActive + 5`,
					// and we have `V` (at active) and `V_new` (at active+5).
					// Reader(active) needs `V`.
					// Reader(active+5) needs `V_new`.
					// So `minActive` logic alone isn't enough if there are intermediate versions.
					// BUT `Vacuum` logic usually is: "Delete V if it is shadowed by V_next AND V_next <= minActive".
					// If V_next <= minActive, then ALL active readers (who are >= minActive) will see V_next (or something newer).
					// None of them will see V.
					// So the condition is: Can we delete `V`?
					// Yes IF there exists `V_next` > `V` AND `V_next` <= `minActiveVersion`.

					// Here: `ver` is the version we are looking at.
					// `lKeptKey` is the previous version we kept (older than `ver`).
					// We are iterating keys. FDB iteration order for integers?
					// Key structure: ("data", PageID, VersionID).
					// Tuple packing of integers preserves order.
					// So we see Version 1, then Version 2, then Version 3.

					// When we see `ver` (say V2):
					// `lKeptKey` points to V1.
					// `lKeptPageID` is PageID.
					// Can we delete V1?
					// Yes, if V2 <= minActiveVersion.
					// If V2 > minActiveVersion, then Reader(minActive) needs V1 (because they don't see V2).

					// So logic:
					// If `ver` (current scanned) <= minActiveVersion:
					//     It shadows `lKeptKey`. `lKeptKey` is obsolete. Delete `lKeptKey`.
					//     `ver` becomes `lKeptKey`.
					// If `ver` > minActiveVersion:
					//     It does NOT shadow `lKeptKey` for the slowest reader.
					//     We must KEEP `lKeptKey`.
					//     But do we keep `ver`?
					//     Yes, `ver` is valid for newer readers (or current).
					//     So `lKeptKey` remains kept.
					//     `ver` ALSO becomes a kept key?
					//     Actually, if we have a chain V1, V2, V3.
					//     minActive = V1.
					//     Scan V1. Kept = V1.
					//     Scan V2. V2 > minActive.
					//     V2 shadows V1 for *new* guys, but not for minActive.
					//     So we don't delete V1.
					//     We keep V2? Yes.
					//     Update `lKeptKey` = V2?
					//     If we update `lKeptKey` to V2, and then see V3.
					//     V3 > minActive.
					//     Does V3 shadow V2?
					//     Yes, for everyone >= V3.
					//     Does anyone need V2?
					//     Readers between V2 and V3.
					//     We only track `minActive`. We assume *conservative* approach:
					//     If we only track MIN, we assume there *might* be readers anywhere between min and current.
					//     So we must keep ALL versions > minActiveVersion.
					//     And we must keep ONE version <= minActiveVersion (the latest one).

					// 2. It is needed if it is the LATEST version <= minActive.
					// We can only know if it is the latest if we see the *next* one.
					// But we are processing strictly in order.
					// So, when we are at `ver` (V2), we look at `lKeptKey` (V1).
					// If V2 <= minActive:
					//    V2 shadows V1. V1 is not needed. Delete V1.
					//    V2 becomes the candidate for "Latest <= minActive".
					// If V2 > minActive:
					//    V2 cannot shadow V1 for the minActive reader.
					//    V1 is verified as "Latest <= minActive".
					//    We keep V1 (already kept).
					//    V2 is also kept (Rule 1).

					if lKeptKey != nil {
						// Check if current `ver` shadows `lKeptKey` SAFELY.
						// Safely means `ver` <= minActiveVersion.
						if ver <= minActiveVersion {
							// Safe to delete old one
							tr.Clear(lKeptKey)
						} else {
							// Not safe. `lKeptKey` is preserved.
							// And `ver` is preserved (will be set as lKeptKey below).
						}
					}

					lKeptKey = kv.Key
					lKeptPageID = pageID
				}

			}

			done := count < 1000
			var nextSel fdb.KeySelector
			if !done {
				nextSel = fdb.FirstGreaterThan(lastSeenKey)
			}

			return chunkResult{nextSel: nextSel, done: done, keptKey: lKeptKey, keptPageID: lKeptPageID}, nil
		})

		if err != nil {
			return err
		}

		cRes := res.(chunkResult)
		lastKeptKey = cRes.keptKey
		lastKeptPageID = cRes.keptPageID
		currentKeySelector = cRes.nextSel

		if cRes.done {
			break
		}
	}

	return nil
}

// SetPageSize stores the page size in metadata. Should be called only once on creation.
func (ps *PageStore) SetPageSize(ctx context.Context, pageSize int) error {
	_, err := ps.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(pageSize))
		tr.Set(ps.subspace.Pack(tuple.Tuple{"meta", "pagesize"}), buf)
		return nil, nil
	})
	return err
}
