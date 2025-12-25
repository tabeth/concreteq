package ttl

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/tabeth/concretedb/models"
)

// mockTableService is a local mock for testing TTLWorker
type mockTableService struct {
	ListTablesFunc         func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error)
	DescribeTimeToLiveFunc func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error)
	GetTableFunc           func(ctx context.Context, tableName string) (*models.Table, error)
	ScanFunc               func(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error)
	DeleteItemFunc         func(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error)

	// Unused methods
	CreateTableFunc         func(ctx context.Context, table *models.Table) (*models.Table, error)
	DeleteTableFunc         func(ctx context.Context, tableName string) (*models.Table, error)
	UpdateTableFunc         func(ctx context.Context, req *models.UpdateTableRequest) (*models.Table, error)
	CreateGlobalTableFunc   func(ctx context.Context, req *models.CreateGlobalTableRequest) (*models.CreateGlobalTableResponse, error)
	UpdateGlobalTableFunc   func(ctx context.Context, req *models.UpdateGlobalTableRequest) (*models.UpdateGlobalTableResponse, error)
	DescribeGlobalTableFunc func(ctx context.Context, globalTableName string) (*models.DescribeGlobalTableResponse, error)
	ListGlobalTablesFunc    func(ctx context.Context, req *models.ListGlobalTablesRequest) (*models.ListGlobalTablesResponse, error)
	PutItemFunc             func(ctx context.Context, request *models.PutItemRequest) (*models.PutItemResponse, error)
	GetItemFunc             func(ctx context.Context, request *models.GetItemRequest) (*models.GetItemResponse, error)
	UpdateItemFunc          func(ctx context.Context, request *models.UpdateItemRequest) (*models.UpdateItemResponse, error)
	QueryFunc               func(ctx context.Context, input *models.QueryRequest) (*models.QueryResponse, error)
	BatchGetItemFunc        func(ctx context.Context, input *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error)
	BatchWriteItemFunc      func(ctx context.Context, input *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error)
	TransactGetItemsFunc    func(ctx context.Context, input *models.TransactGetItemsRequest) (*models.TransactGetItemsResponse, error)
	TransactWriteItemsFunc  func(ctx context.Context, input *models.TransactWriteItemsRequest) (*models.TransactWriteItemsResponse, error)
	ListStreamsFunc         func(ctx context.Context, input *models.ListStreamsRequest) (*models.ListStreamsResponse, error)
	DescribeStreamFunc      func(ctx context.Context, input *models.DescribeStreamRequest) (*models.DescribeStreamResponse, error)
	GetShardIteratorFunc    func(ctx context.Context, input *models.GetShardIteratorRequest) (*models.GetShardIteratorResponse, error)
	GetRecordsFunc          func(ctx context.Context, input *models.GetRecordsRequest) (*models.GetRecordsResponse, error)
	UpdateTimeToLiveFunc    func(ctx context.Context, req *models.UpdateTimeToLiveRequest) (*models.UpdateTimeToLiveResponse, error)
	// Backup
	CreateBackupFunc           func(ctx context.Context, request *models.CreateBackupRequest) (*models.CreateBackupResponse, error)
	DeleteBackupFunc           func(ctx context.Context, request *models.DeleteBackupRequest) (*models.DeleteBackupResponse, error)
	ListBackupsFunc            func(ctx context.Context, request *models.ListBackupsRequest) (*models.ListBackupsResponse, error)
	DescribeBackupFunc         func(ctx context.Context, request *models.DescribeBackupRequest) (*models.DescribeBackupResponse, error)
	RestoreTableFromBackupFunc func(ctx context.Context, request *models.RestoreTableFromBackupRequest) (*models.RestoreTableFromBackupResponse, error)
}

func (m *mockTableService) ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
	if m.ListTablesFunc != nil {
		return m.ListTablesFunc(ctx, limit, exclusiveStartTableName)
	}
	return nil, "", nil
}

func (m *mockTableService) DescribeTimeToLive(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
	if m.DescribeTimeToLiveFunc != nil {
		return m.DescribeTimeToLiveFunc(ctx, req)
	}
	return nil, nil
}

func (m *mockTableService) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	if m.GetTableFunc != nil {
		return m.GetTableFunc(ctx, tableName)
	}
	return nil, nil
}

func (m *mockTableService) Scan(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error) {
	if m.ScanFunc != nil {
		return m.ScanFunc(ctx, input)
	}
	return &models.ScanResponse{}, nil
}

func (m *mockTableService) DeleteItem(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error) {
	if m.DeleteItemFunc != nil {
		return m.DeleteItemFunc(ctx, request)
	}
	return &models.DeleteItemResponse{}, nil
}

// Stubs for unused methods to satisfy interface
func (m *mockTableService) CreateTable(ctx context.Context, table *models.Table) (*models.Table, error) {
	return nil, nil
}
func (m *mockTableService) DeleteTable(ctx context.Context, tableName string) (*models.Table, error) {
	return nil, nil
}
func (m *mockTableService) UpdateTable(ctx context.Context, req *models.UpdateTableRequest) (*models.Table, error) {
	return nil, nil
}
func (m *mockTableService) CreateGlobalTable(ctx context.Context, req *models.CreateGlobalTableRequest) (*models.CreateGlobalTableResponse, error) {
	return nil, nil
}
func (m *mockTableService) UpdateGlobalTable(ctx context.Context, req *models.UpdateGlobalTableRequest) (*models.UpdateGlobalTableResponse, error) {
	return nil, nil
}
func (m *mockTableService) DescribeGlobalTable(ctx context.Context, globalTableName string) (*models.DescribeGlobalTableResponse, error) {
	return nil, nil
}
func (m *mockTableService) ListGlobalTables(ctx context.Context, req *models.ListGlobalTablesRequest) (*models.ListGlobalTablesResponse, error) {
	return nil, nil
}
func (m *mockTableService) PutItem(ctx context.Context, request *models.PutItemRequest) (*models.PutItemResponse, error) {
	return nil, nil
}
func (m *mockTableService) GetItem(ctx context.Context, request *models.GetItemRequest) (*models.GetItemResponse, error) {
	return nil, nil
}
func (m *mockTableService) UpdateItem(ctx context.Context, request *models.UpdateItemRequest) (*models.UpdateItemResponse, error) {
	return nil, nil
}
func (m *mockTableService) Query(ctx context.Context, input *models.QueryRequest) (*models.QueryResponse, error) {
	return nil, nil
}
func (m *mockTableService) BatchGetItem(ctx context.Context, input *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error) {
	return nil, nil
}
func (m *mockTableService) BatchWriteItem(ctx context.Context, input *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error) {
	return nil, nil
}
func (m *mockTableService) TransactGetItems(ctx context.Context, input *models.TransactGetItemsRequest) (*models.TransactGetItemsResponse, error) {
	return nil, nil
}
func (m *mockTableService) TransactWriteItems(ctx context.Context, input *models.TransactWriteItemsRequest) (*models.TransactWriteItemsResponse, error) {
	return nil, nil
}
func (m *mockTableService) ListStreams(ctx context.Context, input *models.ListStreamsRequest) (*models.ListStreamsResponse, error) {
	return nil, nil
}
func (m *mockTableService) DescribeStream(ctx context.Context, input *models.DescribeStreamRequest) (*models.DescribeStreamResponse, error) {
	return nil, nil
}
func (m *mockTableService) GetShardIterator(ctx context.Context, input *models.GetShardIteratorRequest) (*models.GetShardIteratorResponse, error) {
	return nil, nil
}
func (m *mockTableService) GetRecords(ctx context.Context, input *models.GetRecordsRequest) (*models.GetRecordsResponse, error) {
	return nil, nil
}
func (m *mockTableService) UpdateTimeToLive(ctx context.Context, req *models.UpdateTimeToLiveRequest) (*models.UpdateTimeToLiveResponse, error) {
	return nil, nil
}

func (m *mockTableService) CreateBackup(ctx context.Context, request *models.CreateBackupRequest) (*models.CreateBackupResponse, error) {
	if m.CreateBackupFunc != nil {
		return m.CreateBackupFunc(ctx, request)
	}
	return nil, nil
}
func (m *mockTableService) DeleteBackup(ctx context.Context, request *models.DeleteBackupRequest) (*models.DeleteBackupResponse, error) {
	if m.DeleteBackupFunc != nil {
		return m.DeleteBackupFunc(ctx, request)
	}
	return nil, nil
}
func (m *mockTableService) ListBackups(ctx context.Context, request *models.ListBackupsRequest) (*models.ListBackupsResponse, error) {
	if m.ListBackupsFunc != nil {
		return m.ListBackupsFunc(ctx, request)
	}
	return nil, nil
}
func (m *mockTableService) DescribeBackup(ctx context.Context, request *models.DescribeBackupRequest) (*models.DescribeBackupResponse, error) {
	if m.DescribeBackupFunc != nil {
		return m.DescribeBackupFunc(ctx, request)
	}
	return nil, nil
}
func (m *mockTableService) RestoreTableFromBackup(ctx context.Context, request *models.RestoreTableFromBackupRequest) (*models.RestoreTableFromBackupResponse, error) {
	if m.RestoreTableFromBackupFunc != nil {
		return m.RestoreTableFromBackupFunc(ctx, request)
	}
	return nil, nil
}

func TestLifecycle(t *testing.T) {
	mock := &mockTableService{}
	worker := NewTTLWorker(mock, 100*time.Millisecond)
	worker.Start()
	time.Sleep(10 * time.Millisecond) // Let it start
	worker.Stop()
	// Just verifies no panic or hang
}

func TestScanAndCleanup_ErrorsAndEdgeCases(t *testing.T) {
	// 1. ListTables Error
	t.Run("ListTablesError", func(t *testing.T) {
		mock := &mockTableService{
			ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
				return nil, "", models.New("InternalError", "fail")
			},
		}
		worker := NewTTLWorker(mock, time.Hour)
		worker.scanAndCleanup()
		// Should return gracefully
	})

	// 2. DescribeTimeToLive Error
	t.Run("DescribeTimeToLiveError", func(t *testing.T) {
		mock := &mockTableService{
			ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
				return []string{"t1"}, "", nil
			},
			DescribeTimeToLiveFunc: func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
				return nil, models.New("InternalError", "fail")
			},
		}
		worker := NewTTLWorker(mock, time.Hour)
		worker.scanAndCleanup()
	})

	// 3. TTL Disabled
	t.Run("TTLDisabled", func(t *testing.T) {
		mock := &mockTableService{
			ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
				return []string{"t1"}, "", nil
			},
			DescribeTimeToLiveFunc: func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
				return &models.DescribeTimeToLiveResponse{
					TimeToLiveDescription: models.TimeToLiveDescription{TimeToLiveStatus: "DISABLED"},
				}, nil
			},
		}
		worker := NewTTLWorker(mock, time.Hour)
		worker.scanAndCleanup() // Should do nothing
	})

	// 4. GetTable Error
	t.Run("GetTableError", func(t *testing.T) {
		mock := &mockTableService{
			ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
				return []string{"t1"}, "", nil
			},
			DescribeTimeToLiveFunc: func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
				return &models.DescribeTimeToLiveResponse{
					TimeToLiveDescription: models.TimeToLiveDescription{
						TimeToLiveStatus: "ENABLED",
						AttributeName:    "ttl",
					},
				}, nil
			},
			GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
				return nil, models.New("InternalError", "fail")
			},
		}
		worker := NewTTLWorker(mock, time.Hour)
		worker.scanAndCleanup()
	})

	// 5. Scan Error
	t.Run("ScanError", func(t *testing.T) {
		mock := &mockTableService{
			ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
				return []string{"t1"}, "", nil
			},
			DescribeTimeToLiveFunc: func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
				return &models.DescribeTimeToLiveResponse{
					TimeToLiveDescription: models.TimeToLiveDescription{
						TimeToLiveStatus: "ENABLED",
						AttributeName:    "ttl",
					},
				}, nil
			},
			GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
				return &models.Table{TableName: tableName}, nil
			},
			ScanFunc: func(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error) {
				return nil, models.New("InternalError", "fail")
			},
		}
		worker := NewTTLWorker(mock, time.Hour)
		worker.scanAndCleanup()
	})

	// 6. DeleteItem Error
	t.Run("DeleteItemError", func(t *testing.T) {
		past := time.Now().Unix() - 100
		pastStr := strconv.FormatInt(past, 10)
		mock := &mockTableService{
			ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
				return []string{"t1"}, "", nil
			},
			DescribeTimeToLiveFunc: func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
				return &models.DescribeTimeToLiveResponse{
					TimeToLiveDescription: models.TimeToLiveDescription{
						TimeToLiveStatus: "ENABLED",
						AttributeName:    "ttl",
					},
				}, nil
			},
			GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
				return &models.Table{KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}}, nil
			},
			ScanFunc: func(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error) {
				return &models.ScanResponse{
					Items: []map[string]models.AttributeValue{
						{"pk": {S: new(string)}, "ttl": {N: &pastStr}},
					},
				}, nil
			},
			DeleteItemFunc: func(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error) {
				return nil, models.New("InternalError", "fail")
			},
		}
		worker := NewTTLWorker(mock, time.Hour)
		worker.scanAndCleanup() // Should log error and continue
	})

	// 7. Pagination
	t.Run("Pagination", func(t *testing.T) {
		pages := 0
		mock := &mockTableService{
			ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
				if exclusiveStartTableName == "" {
					return []string{"t1"}, "last-t1", nil
				}
				return []string{"t2"}, "", nil
			},
			DescribeTimeToLiveFunc: func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
				return &models.DescribeTimeToLiveResponse{
					TimeToLiveDescription: models.TimeToLiveDescription{TimeToLiveStatus: "DISABLED"},
				}, nil
			},
		}
		// Wrap to count calls
		origList := mock.ListTablesFunc
		mock.ListTablesFunc = func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
			pages++
			return origList(ctx, limit, exclusiveStartTableName)
		}

		worker := NewTTLWorker(mock, time.Hour)
		worker.scanAndCleanup()
		if pages != 2 {
			t.Errorf("Expected 2 pages of tables, got %d", pages)
		}
	})

	// 8. Invalid TTL Values
	t.Run("InvalidTTLValues", func(t *testing.T) {
		mock := &mockTableService{
			ListTablesFunc: func(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
				return []string{"t1"}, "", nil
			},
			DescribeTimeToLiveFunc: func(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
				return &models.DescribeTimeToLiveResponse{
					TimeToLiveDescription: models.TimeToLiveDescription{
						TimeToLiveStatus: "ENABLED",
						AttributeName:    "ttl",
					},
				}, nil
			},
			GetTableFunc: func(ctx context.Context, tableName string) (*models.Table, error) {
				return &models.Table{}, nil
			},
			ScanFunc: func(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error) {
				// Inject it for this scope if we could, but we can't easily.
				// But we can assert logic:
				// item1: no ttl attr
				// item2: ttl attr not Number
				// item3: ttl attr invalid number string
				notNum := "not-a-number"
				return &models.ScanResponse{
					Items: []map[string]models.AttributeValue{
						{"pk": {S: new(string)}},                      // No TTL
						{"pk": {S: new(string)}, "ttl": {S: &notNum}}, // Wrong Type
						{"pk": {S: new(string)}, "ttl": {N: &notNum}}, // Invalid Number
					},
				}, nil
			},
			DeleteItemFunc: func(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error) {
				t.Error("Should not attempt to delete items with invalid TTL")
				return nil, nil
			},
		}
		worker := NewTTLWorker(mock, time.Hour)
		worker.scanAndCleanup()
	})
}
