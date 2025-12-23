package store

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/tabeth/concretedb/models"
)

var fdbAvailable bool
var testDB fdb.Database

func TestMain(m *testing.M) {
	log.Println("Setting up FoundationDB for tests...")
	fdb.MustAPIVersion(710)

	var err error
	testDB, err = fdb.OpenDefault()
	if err != nil {
		log.Printf("WARNING: could not open FoundationDB handle: %v", err)
	} else {
		// Check connectivity with a timeout
		done := make(chan error)
		go func() {
			_, err := testDB.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
				rtr.Get(fdb.Key("healthcheck")).Get() // Force a read
				return nil, nil
			})
			done <- err
		}()

		select {
		case err := <-done:
			if err != nil {
				log.Printf("WARNING: could not connect to FoundationDB: %v", err)
			} else {
				log.Println("Successfully connected to FoundationDB.")
				fdbAvailable = true
			}
		case <-time.After(2 * time.Second):
			log.Println("WARNING: FoundationDB connection timed out.")
		}
	}

	exitCode := m.Run()
	os.Exit(exitCode)
}

func setupTestStore(t *testing.T, tableName string) *FoundationDBStore {
	t.Helper()
	if !fdbAvailable {
		t.Skip("FoundationDB is not available/reachable. Skipping test.")
	}
	store := NewFoundationDBStore(testDB)
	log.Printf("Test '%s': Clearing keys for table '%s'", t.Name(), tableName)

	_, err := store.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := store.ss.Pack(tuple.Tuple{tableName})
		tr.Clear(key)
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

	// ACT 2: Call DeleteTable for the second time to test idempotency.
	result2, err := store.DeleteTable(ctx, tableName)

	// ASSERT 2: Check that it still succeeded and the status is still DELETING.
	if err != nil {
		t.Fatalf("Second DeleteTable call failed unexpectedly: %v", err)
	}
	if result2.Status != models.StatusDeleting {
		t.Errorf("expected status to be DELETING after second call, but got %s", result2.Status)
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
