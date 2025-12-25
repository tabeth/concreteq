package store

import (
	"context"
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	PageSize   = 4096
	MaxTxBytes = 9 * 1024 * 1024 // 9MB safety limit
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
	Version int64
	Size    int64
}

// CurrentVersion reads the current active version ID and file size of the database.
func (ps *PageStore) CurrentVersion(ctx context.Context) (int64, int64, error) {
	res, err := ps.db.ReadTransact(func(r fdb.ReadTransaction) (interface{}, error) {
		val := r.Get(ps.subspace.Pack(tuple.Tuple{"meta", "version"})).MustGet()
		sizeBytes := r.Get(ps.subspace.Pack(tuple.Tuple{"meta", "size"})).MustGet()

		var s DBState
		if len(val) > 0 {
			s.Version = int64(binary.BigEndian.Uint64(val))
		}
		if len(sizeBytes) > 0 {
			s.Size = int64(binary.BigEndian.Uint64(sizeBytes))
		}
		return s, nil
	})
	if err != nil {
		return 0, 0, err
	}
	s := res.(DBState)
	return s.Version, s.Size, nil
}

// SetVersionAndSize updates both the version and the file size atomically.
func (ps *PageStore) SetVersionAndSize(ctx context.Context, version int64, size int64) error {
	_, err := ps.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(version))
		tr.Set(ps.subspace.Pack(tuple.Tuple{"meta", "version"}), buf)

		bufSize := make([]byte, 8)
		binary.BigEndian.PutUint64(bufSize, uint64(size))
		tr.Set(ps.subspace.Pack(tuple.Tuple{"meta", "size"}), bufSize)
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
func (ps *PageStore) Vacuum(ctx context.Context) error {
	currentVersion, _, err := ps.CurrentVersion(ctx)
	if err != nil {
		return err
	}

	// We scan ALL data keys. This is expensive but necessary for full vacuum.
	// Optimization: Store "WrittenPages" index per version.
	// For PoC: Scan ("data") range.

	// Range scan of ("data", ...)
	// Keys: ("data", PageID, VersionID)

	// 1. Identify valid latest version for each page <= currentVersion
	// 2. Delete anything > currentVersion
	// 3. Delete anything shadowed?

	// To do this efficiently without loading everything:
	// We can iterate page by page? No, PageIDs are sparse.

	// Simply Iterate all keys.
	// Keep track of which PageID we are seeing.

	// Strategy:
	// Iterate keys.
	// keys for PageID P will be sorted by VersionID (because tuple packing?).
	// tuple packing of (int, int) -> (PageID, VersionID).
	// Yes, sorted by PageID, then VersionID.

	// So for each PageID, we will see P/V1, P/V2, P/V3...

	// But wait! We need to delete FUTURE versions too.

	// Let's iterate.

	begin := ps.subspace.Pack(tuple.Tuple{"data"})
	end := ps.subspace.Pack(tuple.Tuple{"data"}).FDBKey() // Exclusive end?
	// Usually append 0xFF to get prefix range.
	end = append(begin, 0xFF)

	// We need to do this in chunks/transactionally.
	// FDB 5s limit applies to Vacuum too!

	// We'll use a cursor-based approach if needed, but for PoC we try one huge scan or batch it.

	// Simplified Vacuum for PoC: Just delete Future Versions (> CurrentVersion).
	// This fixes the specific "Crash Recovery" risk mentioned by user.
	// Shadow pruning is optimization.

	_, err = ps.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Naive scan for Future Versions
		// Optimize: Do we have a separate index for "Dirty Versions"?
		// No.

		// Full scan
		rr := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{})
		iter := rr.Iterator()
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			// Unpack
			t, err := ps.subspace.Unpack(kv.Key)
			if err != nil {
				continue
			}
			// t: ("data", PageID, VersionID)
			if len(t) != 3 {
				continue
			}

			ver := t[2].(int64)

			if ver > currentVersion {
				tr.Clear(kv.Key)
			}
		}
		return nil, nil
	})

	return err
}
