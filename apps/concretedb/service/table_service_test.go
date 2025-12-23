package service

import (
	"context"
	"errors"
	"testing"

	"github.com/tabeth/concretedb/models"
	st "github.com/tabeth/concretedb/store"
)

// mockStore is a mock implementation of the st.Store interface for testing.
type mockStore struct {
	CreateTableFunc func(ctx context.Context, table *models.Table) error
	GetTableFunc    func(ctx context.Context, tableName string) (*models.Table, error)
	DeleteTableFunc func(ctx context.Context, tableName string) (*models.Table, error) // <-- ADD THIS
}

func (m *mockStore) CreateTable(ctx context.Context, table *models.Table) error {
	return m.CreateTableFunc(ctx, table)
}

func (m *mockStore) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	return m.GetTableFunc(ctx, tableName)
}

// Add this method to satisfy the st.Store interface
func (m *mockStore) DeleteTable(ctx context.Context, tableName string) (*models.Table, error) {
	return m.DeleteTableFunc(ctx, tableName)
}

func TestTableService_DeleteTable_NotFound(t *testing.T) {
	mock := &mockStore{
		DeleteTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
			// Mock the storage layer returning a "not found" error
			return nil, st.ErrTableNotFound
		},
	}
	service := NewTableService(mock)

	_, err := service.DeleteTable(context.Background(), "non-existent-table")

	var apiErr *models.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected an *models.APIError, but got %T", err)
	}

	if apiErr.Type != "ResourceNotFoundException" {
		t.Errorf("expected error type ResourceNotFoundException, got %s", apiErr.Type)
	}
}

func TestTableService_CreateTable_Success(t *testing.T) {
	mock := &mockStore{
		CreateTableFunc: func(ctx context.Context, table *models.Table) error {
			return nil // Mock success
		},
	}
	service := NewTableService(mock)

	table := &models.Table{
		TableName: "test-service-table",
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}

	resp, err := service.CreateTable(context.Background(), table)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if resp.TableName != "test-service-table" {
		t.Errorf("unexpected table name: got %s, want %s", resp.TableName, "test-service-table")
	}
	if resp.Status != models.StatusCreating {
		t.Errorf("unexpected table status: got %s, want %s", resp.Status, "CREATING")
	}
}

func TestTableService_CreateTable_ValidationError(t *testing.T) {
	service := NewTableService(&mockStore{}) // Store won't be called

	// Test with an invalid models.Table (empty name)
	table := &models.Table{TableName: ""}

	_, err := service.CreateTable(context.Background(), table)

	// CHANGED: We now check for our new models.APIError type.
	var apiErr *models.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected an *models.APIError, but got %T", err)
	}

	if apiErr.Type != "ValidationException" {
		t.Errorf("expected error type ValidationException, got %s", apiErr.Type)
	}
}

func TestTableService_CreateTable_ResourceInUse(t *testing.T) {
	mock := &mockStore{
		CreateTableFunc: func(ctx context.Context, table *models.Table) error {
			return st.ErrTableExists // Mock that the table already exists
		},
	}
	service := NewTableService(mock)

	table := &models.Table{
		TableName: "existing-table",
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	}

	_, err := service.CreateTable(context.Background(), table)

	var apiErr *models.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected an *models.APIError, but got %T", err)
	}

	if apiErr.Type != "ResourceInUseException" {
		t.Errorf("expected error type ResourceInUseException, got %s", apiErr.Type)
	}
}
