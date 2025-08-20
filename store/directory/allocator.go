package directory

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

var oneBytes = []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var allocatorMutex = sync.Mutex{}

type highContentionAllocator struct {
	counters, recent subspace.Subspace
}

func newHCA(s subspace.Subspace) highContentionAllocator {
	var hca highContentionAllocator

	hca.counters = s.Sub(0)
	hca.recent = s.Sub(1)

	return hca
}

func windowSize(start int64) int64 {
	if start < 255 {
		return 64
	}
	if start < 65535 {
		return 1024
	}
	return 8192
}

func (hca highContentionAllocator) allocate(tr fdb.Transaction, s subspace.Subspace) (subspace.Subspace, error) {
	for {
		rr := tr.Snapshot().GetRange(hca.counters, fdb.RangeOptions{Limit: 1, Reverse: true})
		kvs, err := rr.GetSliceWithError()
		if err != nil {
			return nil, err
		}

		var start int64
		var window int64

		if len(kvs) == 1 {
			t, err := hca.counters.Unpack(kvs[0].Key)
			if err != nil {
				return nil, err
			}
			start = t[0].(int64)
		}

		windowAdvanced := false
		for {
			allocatorMutex.Lock()

			if windowAdvanced {
				tr.ClearRange(fdb.KeyRange{hca.counters, hca.counters.Sub(start)})
				tr.Options().SetNextWriteNoWriteConflictRange()
				tr.ClearRange(fdb.KeyRange{hca.recent, hca.recent.Sub(start)})
			}

			tr.Add(hca.counters.Sub(start), oneBytes)
			countFuture := tr.Snapshot().Get(hca.counters.Sub(start))

			allocatorMutex.Unlock()

			countStr, err := countFuture.Get()
			if err != nil {
				return nil, err
			}

			var count int64
			if countStr == nil {
				count = 0
			} else {
				err = binary.Read(bytes.NewBuffer(countStr), binary.LittleEndian, &count)
				if err != nil {
					return nil, err
				}
			}

			window = windowSize(start)
			if count*2 < window {
				break
			}

			start += window
			windowAdvanced = true
		}

		for {
			candidate := rand.Int63n(window) + start
			key := hca.recent.Sub(candidate)

			allocatorMutex.Lock()

			latestCounter := tr.Snapshot().GetRange(hca.counters, fdb.RangeOptions{Limit: 1, Reverse: true})
			candidateValue := tr.Get(key)
			tr.Options().SetNextWriteNoWriteConflictRange()
			tr.Set(key, []byte(""))

			allocatorMutex.Unlock()

			kvs, err = latestCounter.GetSliceWithError()
			if err != nil {
				return nil, err
			}
			if len(kvs) > 0 {
				t, err := hca.counters.Unpack(kvs[0].Key)
				if err != nil {
					return nil, err
				}
				currentStart := t[0].(int64)
				if currentStart > start {
					break
				}
			}

			v, err := candidateValue.Get()
			if err != nil {
				return nil, err
			}
			if v == nil {
				tr.AddWriteConflictKey(key)
				return s.Sub(candidate), nil
			}
		}
	}
}
