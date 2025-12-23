package store

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
)

var (
	testDB fdb.Database
	fdbCmd *exec.Cmd
)

const (
	testPort        = 4690
	testDir         = ".fdb_test"
	clusterContents = "test:test@127.0.0.1:4690"
)

func TestMain(m *testing.M) {
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
	// /usr/sbin/fdbserver -p 127.0.0.1:4690 -d .fdb_test/data -L .fdb_test/logs -C .fdb_test/fdb.cluster
	fdbCmd = exec.Command("/usr/sbin/fdbserver",
		"-p", fmt.Sprintf("127.0.0.1:%d", testPort),
		"-d", filepath.Join(testDir, "data"),
		"-L", filepath.Join(testDir, "logs"),
		"-C", clusterFile,
	)
	if err := fdbCmd.Start(); err != nil {
		log.Fatalf("Failed to start fdbserver: %v", err)
	}
	log.Printf("Started fdbserver on port %d with pid %d", testPort, fdbCmd.Process.Pid)

	// Ensure cleanup happens even if init fails
	defer func() {
		if fdbCmd.Process != nil {
			fdbCmd.Process.Kill()
		}
		os.RemoveAll(testDir)
	}()

	// 4. Initialize Database
	// fdbcli -C .fdb_test/fdb.cluster --exec "configure new single memory"
	// We need to wait a bit for the server to be ready to accept connections?
	time.Sleep(2 * time.Second)

	initCmd := exec.Command("fdbcli", "-C", clusterFile, "--exec", "configure new single memory")
	if out, err := initCmd.CombinedOutput(); err != nil {
		log.Fatalf("Failed to initialize FDB: %v\nOutput: %s", err, out)
	}
	log.Println("FDB initialized successfully")

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

	// 7. Explicit Cleanup (since os.Exit skips defers)
	fdbCmd.Process.Kill()
	os.RemoveAll(testDir)
	os.Exit(code)
}

func setupTestStore(t *testing.T, tableName string) *FoundationDBStore {
	t.Helper()
	// Skip the fdbtest.SkipIfFDBUnavailable check since we are managing our own
	// fdbtest.SkipIfFDBUnavailable(t)

	store := NewFoundationDBStore(testDB)
	// log.Printf("Test '%s': Clearing keys for table '%s'", t.Name(), tableName)

	_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		_, err := store.dir.Remove(tr, []string{"tables", tableName})
		if err != nil {
			return nil, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Test '%s': failed to clear test data for table '%s': %v", t.Name(), tableName, err)
	}
	return store
}

func TestFoundationDBStore_CreateTable_Success(t *testing.T) {
	tableName := "test-create-success"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	table := &models.Table{
		TableName:        tableName,
		Status:           models.StatusCreating,
		CreationDateTime: time.Now(),
		// CHANGED: Use models.KeySchemaElement, not api.KeySchemaElement
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
	}
	err := store.CreateTable(ctx, table)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	retrieved, err := store.GetTable(ctx, tableName)
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}
	if retrieved == nil {
		t.Fatal("expected to retrieve a table, but got nil")
	}
	if retrieved.TableName != tableName {
		t.Errorf("expected table name '%s', got '%s'", tableName, retrieved.TableName)
	}
}

func TestFoundationDBStore_CreateTable_AlreadyExists(t *testing.T) {
	tableName := "test-create-exists"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	table := &models.Table{
		TableName: tableName,
		Status:    models.StatusCreating,
	}
	err := store.CreateTable(ctx, table)
	if err != nil {
		t.Fatalf("First CreateTable failed: %v", err)
	}
	err = store.CreateTable(ctx, table)
	if err != ErrTableExists {
		t.Errorf("expected error ErrTableExists, but got %v", err)
	}
}

func TestFoundationDBStore_GetTable_NotFound(t *testing.T) {
	tableName := "test-get-not-found"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	retrieved, err := store.GetTable(ctx, "non-existent-table")
	if err != nil {
		t.Fatalf("GetTable failed unexpectedly: %v", err)
	}
	if retrieved != nil {
		t.Error("expected nil for a non-existent table, but got a table")
	}
}

func TestFoundationDBStore_DeleteTable(t *testing.T) {
	// ARRANGE: Create an initial table in the database.
	tableName := "table-to-be-deleted"
	store := setupTestStore(t, tableName)
	ctx := context.Background()
	initialTable := &models.Table{TableName: tableName, Status: models.StatusActive}
	err := store.CreateTable(ctx, initialTable)
	if err != nil {
		t.Fatalf("Failed to create table for delete test: %v", err)
	}

	// ACT 1: Call DeleteTable for the first time.
	result1, err := store.DeleteTable(ctx, tableName)

	// ASSERT 1: Check that it succeeded and the status is DELETING.
	if err != nil {
		t.Fatalf("First DeleteTable call failed unexpectedly: %v", err)
	}
	if result1.Status != models.StatusDeleting {
		t.Errorf("expected status to be DELETING after first call, but got %s", result1.Status)
	}

	// ACT 2: Call DeleteTable for the second time.
	_, err = store.DeleteTable(ctx, tableName)

	// ASSERT 2: Check that it now fails with ErrTableNotFound because it's gone.
	if err == nil {
		t.Fatal("Second DeleteTable call succeeded unexpectedly; expected table not found")
	}
	if err != ErrTableNotFound {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}
}

func TestFoundationDBStore_DeleteTable_NotFound(t *testing.T) {
	// ARRANGE: Set up the store, but do not create any table.
	tableName := "non-existent-table-for-delete"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// ACT: Attempt to delete the non-existent table.
	_, err := store.DeleteTable(ctx, tableName)

	// ASSERT: Verify that we received the correct "not found" error.
	if err != ErrTableNotFound {
		t.Errorf("expected ErrTableNotFound, but got: %v", err)
	}
}

func TestFoundationDBStore_ListTables(t *testing.T) {
	// We want to verify listing works. We'll create a few tables with a unique prefix.
	// Since tests might run in parallel or on a dirty DB, we use a unique prefix.
	prefix := "test-list-" + time.Now().Format("20060102150405") + "-"
	tableNames := []string{prefix + "1", prefix + "2", prefix + "3"}

	store := setupTestStore(t, "dummy")
	ctx := context.Background()

	// 1. Create tables
	for _, name := range tableNames {
		table := &models.Table{TableName: name, Status: models.StatusActive}
		if err := store.CreateTable(ctx, table); err != nil {
			t.Fatalf("Failed to create table %s: %v", name, err)
		}
	}

	// 2. List with limit sufficient to capture all
	// We might get other tables too, so we'll filter or check contains.
	// Since we can't easily rely on just these tables being present.
	// However, we can check pagination logic.

	// First, check that all create tables are eventually found
	// We'll iterate until we find them all or exhaust the list.
	found := make(map[string]bool)
	var lastEval string
	for {
		names, lek, err := store.ListTables(ctx, 100, lastEval)
		if err != nil {
			t.Fatalf("ListTables failed: %v", err)
		}
		for _, n := range names {
			for _, target := range tableNames {
				if n == target {
					found[n] = true
				}
			}
		}
		if lek == "" {
			break
		}
		lastEval = lek
	}

	for _, name := range tableNames {
		if !found[name] {
			t.Errorf("Did not find table %s in ListTables result", name)
		}
	}

	// 3. Test Pagination with Limit
	// We'll list with Limit=1 starting from ""
	// We expect at least one result (since we created 3)
	p1, lek1, err := store.ListTables(ctx, 1, "")
	if err != nil {
		t.Fatalf("ListTables pagination failed: %v", err)
	}
	if len(p1) != 1 {
		t.Errorf("Expected 1 result for Limit=1, got %d", len(p1))
	}
	if lek1 == "" {
		// Strictly speaking, if there is only 1 table in the whole DB, LEK might be empty?
		// But we created 3, so there should be more.
		t.Error("Expected LastEvaluatedTableName when more tables exist")
	} else if lek1 != p1[0] {
		// LEK should be the last item's name
		t.Errorf("Expected LEK to be %s, got %s", p1[0], lek1)
	}

	// Clean up
	for _, name := range tableNames {
		store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			store.dir.Remove(tr, []string{"tables", name})
			return nil, nil
		})
	}
}

func TestFoundationDBStore_CorruptedMetadata(t *testing.T) {
	tableName := "test-corrupted"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Manually write corrupted JSON to the metadata key
	_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Open directory
		sub, err := store.dir.CreateOrOpen(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		// Write bad data
		tr.Set(sub.Pack(tuple.Tuple{"metadata"}), []byte("{not-valid-json"))
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to setup corrupted data: %v", err)
	}

	// 2. Test GetTable
	_, err = store.GetTable(ctx, tableName)
	if err == nil {
		t.Error("Expected error from GetTable on corrupted data, got nil")
	}

	// 3. Test DeleteTable
	_, err = store.DeleteTable(ctx, tableName)
	if err == nil {
		t.Error("Expected error from DeleteTable on corrupted data, got nil")
	}
}
