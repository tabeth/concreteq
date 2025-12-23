package fdbtest

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	sharedfdb "github.com/tabeth/kiroku-core/libs/fdb"
)

var (
	fdbAvailable      bool
	fdbAvailabilityMu sync.Mutex
	fdbCheckOnce      sync.Once
)

// SkipIfFDBUnavailable skips the test if FDB is not running or unreachable.
func SkipIfFDBUnavailable(t testing.TB) {
	t.Helper()
	fdbCheckOnce.Do(func() {
		checkFDBAvailability(t)
	})
	if !fdbAvailable {
		t.Skip("Skipping test because FoundationDB is not available")
	}
}

// checkFDBAvailability attempts to connect to FDB and perform a read operation with a timeout.
func checkFDBAvailability(t testing.TB) {
	fdbAvailabilityMu.Lock()
	defer fdbAvailabilityMu.Unlock()

	// If we've already determined it's available, no need to check again (though Once covers this, this is for clarity)
	if fdbAvailable {
		return
	}

	fmt.Println("Checking FoundationDB cluster availability...")

	clusterFile := os.Getenv("FDB_CLUSTER_FILE")
	if clusterFile == "" {
		// Fallback to default if not set
	}

	// Try to open DB
	db, err := sharedfdb.OpenDB(710)
	if err != nil {
		fmt.Printf("WARNING: Failed to open FoundationDB: %v\n", err)
		return
	}

	// Create a channel to signal completion
	done := make(chan bool)

	go func() {
		// Attempt a simple read. We use proper transaction management.
		_, err := db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			_, err := rtr.Get(fdb.Key("nonexistent_key_for_check")).Get()
			return nil, err
		})
		if err == nil {
			done <- true
		} else {
			// Just swallow error, we only care if it succeeds
			done <- false
		}
	}()

	// Wait for success or timeout
	select {
	case success := <-done:
		if success {
			fmt.Println("FoundationDB cluster is available. Proceeding with tests.")
			fdbAvailable = true
		} else {
			fmt.Println("WARNING: FoundationDB read check failed.")
		}
	case <-time.After(2 * time.Second):
		fmt.Println("WARNING: FoundationDB connection timed out (checkFDBAvailability).")
	}
}

// ForceCheck forces a re-check of availability (useful if FDB is started mid-run, though rare in tests)
func ForceCheck(t testing.TB) {
	fdbCheckOnce = sync.Once{}
	fdbAvailable = false
	checkFDBAvailability(t)
}
