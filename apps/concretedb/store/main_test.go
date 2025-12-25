package store

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var (
	testDB fdb.Database
	fdbCmd *exec.Cmd
)

const (
	testPort        = 4692
	testDir         = ".fdb_test_store"
	clusterContents = "test:test@127.0.0.1:4692"
)

func TestMain(m *testing.M) {
	// Check if we should use existing FDB
	if os.Getenv("USE_EXISTING_FDB") != "" {
		log.Println("Using existing FDB for store tests")
		fdb.MustAPIVersion(710)
		os.Exit(m.Run())
	}

	// 1. Prepare test directory
	if err := os.RemoveAll(testDir); err != nil {
		log.Printf("Failed to clean test dir: %v", err)
	}
	if err := os.MkdirAll(testDir, 0755); err != nil {
		log.Fatalf("Failed to create test dir: %v", err)
	}

	// 2. Create Cluster File
	clusterFile := filepath.Join(testDir, "fdb.cluster")
	if err := os.WriteFile(clusterFile, []byte(clusterContents), 0644); err != nil {
		log.Fatalf("Failed to write cluster file: %v", err)
	}

	// 3. Start FDB Server
	fdbCmd = exec.Command("/usr/sbin/fdbserver",
		"-p", fmt.Sprintf("127.0.0.1:%d", testPort),
		"-d", filepath.Join(testDir, "data"),
		"-L", filepath.Join(testDir, "logs"),
		"-C", clusterFile,
	)
	if err := fdbCmd.Start(); err != nil {
		log.Fatalf("Failed to start fdbserver: %v", err)
	}
	log.Printf("Started store fdbserver on port %d with pid %d", testPort, fdbCmd.Process.Pid)

	// Ensure cleanup happens even if init fails
	defer func() {
		if fdbCmd.Process != nil {
			fdbCmd.Process.Kill()
		}
		os.RemoveAll(testDir)
	}()

	// 4. Initialize Database
	time.Sleep(2 * time.Second)

	initCmd := exec.Command("fdbcli", "-C", clusterFile, "--exec", "configure new single memory")
	if out, err := initCmd.CombinedOutput(); err != nil {
		log.Fatalf("Failed to initialize FDB: %v\nOutput: %s", err, string(out))
	}
	log.Println("FDB initialized successfully for store tests")

	// 5. Connect
	os.Setenv("FDB_CLUSTER_FILE", clusterFile)

	// 6. Run Tests
	code := m.Run()

	// 7. Explicit Cleanup
	fdbCmd.Process.Kill()
	os.RemoveAll(testDir)
	os.Exit(code)
}
