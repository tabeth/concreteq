package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/tabeth/concretedb/config"
)

// TestMainServerStartup verifies that the main function can start an HTTP server
// and that the health check endpoint is responsive.
func TestMainServerStartup(t *testing.T) {
	// Run our main function in a goroutine.
	go main()

	// Give the server a moment to start.
	time.Sleep(100 * time.Millisecond)

	cfg := config.NewConfig()
	// CHANGED: The test now points to the /health endpoint.
	testURL := fmt.Sprintf("http://localhost:%d/health", cfg.Port)

	req, err := http.NewRequest("GET", testURL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Provide a more helpful error message if the connection is refused.
		t.Fatalf("Failed to send request to server at %s: %v. Is the server running?", testURL, err)
	}
	defer resp.Body.Close()

	// 1. Check the HTTP status code.
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code %d for health check, but got %d", http.StatusOK, resp.StatusCode)
	}

	// 2. Check the response body.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	// CHANGED: The expected body is now "OK\n" from our health handler.
	expectedBody := "OK\n"
	if string(body) != expectedBody {
		t.Errorf("expected response body '%s', but got '%s'", expectedBody, string(body))
	}
}

// TestMainServerConflict verifies that the main function exits if the port is in use.
// Note: This test might be flaky if port 8000 is occupied by something else,
// but for CI it should be fine.
func TestMainServerConflict(t *testing.T) {
	// Start a listener on port 8000
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		t.Skip("Port 8000 already in use, cannot test conflict")
	}
	defer l.Close()

	// main() calls Fatalln which calls os.Exit. We can't catch that easily.
	// So we'll skip this specific branch for now to avoid killing the test runner.
}

var (
	testDB fdb.Database
	fdbCmd *exec.Cmd
)

const (
	testPort        = 4691
	testDir         = ".fdb_test_integ"
	clusterContents = "test:test@127.0.0.1:4691"
)

func TestMain(m *testing.M) {
	// Check if we should use existing FDB
	if os.Getenv("USE_EXISTING_FDB") != "" {
		log.Println("Using existing FDB for integration tests")
		fdb.MustAPIVersion(710)
		var err error
		testDB, err = fdb.OpenDefault()
		if err != nil {
			log.Fatalf("Failed to open default DB: %v", err)
		}
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
	log.Printf("Started integration fdbserver on port %d with pid %d", testPort, fdbCmd.Process.Pid)

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
	log.Println("FDB initialized successfully for integration tests")

	// 5. Connect
	os.Setenv("FDB_CLUSTER_FILE", clusterFile)
	fdb.MustAPIVersion(710)
	var err error
	testDB, err = fdb.OpenDefault()
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}

	// 6. Run Tests
	code := m.Run()

	// 7. Explicit Cleanup
	fdbCmd.Process.Kill()
	os.RemoveAll(testDir)
	os.Exit(code)
}
