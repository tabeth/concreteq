package store

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	// Helper for cleanup
	cleanup := func() {
		if fdbCmd != nil && fdbCmd.Process != nil {
			fdbCmd.Process.Kill()
		}
		os.RemoveAll(testDir)
	}

	// 1. Prepare test directory
	if err := os.RemoveAll(testDir); err != nil {
		log.Printf("Failed to clean test dir: %v", err)
	}
	if err := os.MkdirAll(testDir, 0755); err != nil {
		log.Printf("Failed to create test dir: %v", err)
		return // Exit without running tests
	}

	// 2. Create Cluster File
	clusterFile := filepath.Join(testDir, "fdb.cluster")
	if err := os.WriteFile(clusterFile, []byte(clusterContents), 0644); err != nil {
		log.Printf("Failed to write cluster file: %v", err)
		cleanup()
		return
	}

	// 3. Start FDB Server
	// Ensure no orphaned process is holding the port
	// We can't easily kill process by port cross-platform in Go standard lib without pkill/lsof
	// But we try to rely on unique port.

	fdbCmd = exec.Command("/usr/sbin/fdbserver",
		"-p", fmt.Sprintf("127.0.0.1:%d", testPort),
		"-d", filepath.Join(testDir, "data"),
		"-L", filepath.Join(testDir, "logs"),
		"-C", clusterFile,
	)
	if err := fdbCmd.Start(); err != nil {
		log.Printf("Failed to start fdbserver: %v", err)
		cleanup()
		return
	}
	log.Printf("Started store fdbserver on port %d with pid %d", testPort, fdbCmd.Process.Pid)

	// 4. Initialize Database
	// Wait a bit for server to be ready to accept connections
	time.Sleep(3 * time.Second)

	// Try initializing. If it fails (e.g. database already exists because of orphan process reusing port), we might need to handle it.
	initCmd := exec.Command("fdbcli", "-C", clusterFile, "--exec", "configure new single memory")
	if out, err := initCmd.CombinedOutput(); err != nil {
		output := string(out)
		if filepath.Base(os.Args[0]) == "store.test" && (strings.Contains(output, "Database already exists") || strings.Contains(output, "already configured")) {
			// This might happen if 'testDir' wasn't fully cleaned but server started?
			// Or if fdbcli connected to an existing server.
			// If it's already configured, we might be okay to proceed?
			log.Printf("FDB already configured (warning): %v", output)
		} else {
			log.Printf("Failed to initialize FDB: %v\nOutput: %s", err, output)
			cleanup()
			os.Exit(1)
		}
	}
	log.Println("FDB initialized successfully for store tests")

	// 5. Connect
	os.Setenv("FDB_CLUSTER_FILE", clusterFile)

	// 6. Run Tests
	code := m.Run()

	// 7. Cleanup
	cleanup()
	os.Exit(code)
}
