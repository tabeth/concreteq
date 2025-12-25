package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store"
)

// TableServicer defines the interface for our table service.
// The API layer will depend on this interface.
type TableServicer interface {
	CreateTable(ctx context.Context, table *models.Table) (*models.Table, error)
	DeleteTable(ctx context.Context, tableName string) (*models.Table, error)
	ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error)
	GetTable(ctx context.Context, tableName string) (*models.Table, error)
	UpdateTable(ctx context.Context, req *models.UpdateTableRequest) (*models.Table, error)

	CreateGlobalTable(ctx context.Context, req *models.CreateGlobalTableRequest) (*models.CreateGlobalTableResponse, error)
	UpdateGlobalTable(ctx context.Context, req *models.UpdateGlobalTableRequest) (*models.UpdateGlobalTableResponse, error)
	DescribeGlobalTable(ctx context.Context, globalTableName string) (*models.DescribeGlobalTableResponse, error)
	ListGlobalTables(ctx context.Context, req *models.ListGlobalTablesRequest) (*models.ListGlobalTablesResponse, error)

	// TTL Operations
	UpdateTimeToLive(ctx context.Context, req *models.UpdateTimeToLiveRequest) (*models.UpdateTimeToLiveResponse, error)
	DescribeTimeToLive(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error)

	// Item Operations
	PutItem(ctx context.Context, request *models.PutItemRequest) (*models.PutItemResponse, error)
	GetItem(ctx context.Context, request *models.GetItemRequest) (*models.GetItemResponse, error)
	DeleteItem(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error)
	UpdateItem(ctx context.Context, request *models.UpdateItemRequest) (*models.UpdateItemResponse, error)
	Scan(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error)
	Query(ctx context.Context, input *models.QueryRequest) (*models.QueryResponse, error)
	BatchGetItem(ctx context.Context, input *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error)
	BatchWriteItem(ctx context.Context, input *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error)
	TransactGetItems(ctx context.Context, input *models.TransactGetItemsRequest) (*models.TransactGetItemsResponse, error)
	TransactWriteItems(ctx context.Context, input *models.TransactWriteItemsRequest) (*models.TransactWriteItemsResponse, error)

	// Stream Operations
	ListStreams(ctx context.Context, input *models.ListStreamsRequest) (*models.ListStreamsResponse, error)
	DescribeStream(ctx context.Context, input *models.DescribeStreamRequest) (*models.DescribeStreamResponse, error)
	GetShardIterator(ctx context.Context, input *models.GetShardIteratorRequest) (*models.GetShardIteratorResponse, error)
	GetRecords(ctx context.Context, input *models.GetRecordsRequest) (*models.GetRecordsResponse, error)

	// Backup Operations
	CreateBackup(ctx context.Context, req *models.CreateBackupRequest) (*models.CreateBackupResponse, error)
	DeleteBackup(ctx context.Context, req *models.DeleteBackupRequest) (*models.DeleteBackupResponse, error)
	ListBackups(ctx context.Context, req *models.ListBackupsRequest) (*models.ListBackupsResponse, error)
	DescribeBackup(ctx context.Context, req *models.DescribeBackupRequest) (*models.DescribeBackupResponse, error)
	RestoreTableFromBackup(ctx context.Context, req *models.RestoreTableFromBackupRequest) (*models.RestoreTableFromBackupResponse, error)
}

// TableService contains the business logic for table operations.
type TableService struct {
	store store.Store
}

// NewTableService creates a new TableService.
func NewTableService(store store.Store) *TableService {
	return &TableService{store: store}
}

// CreateTable validates the request, creates a new table, persists it,
// and returns the persisted table .
// It now accepts a `*models.Table` instead of an API request object.
func (s *TableService) CreateTable(ctx context.Context, table *models.Table) (*models.Table, error) {
	if err := validateTable(table); err != nil {
		return nil, err
	}

	// Default to StatusCreating
	// Later creating will be queued up. For now it's done in a single, long request.
	// Handling the queued version will later by the default, with a flag introduced to make it atomic.
	if table.Status == "" {
		table.Status = models.StatusCreating
	}

	if err := s.store.CreateTable(ctx, table); err != nil {
		if errors.Is(err, store.ErrTableExists) {
			// Translate storage error to a specific API error
			return nil, models.New("ResourceInUseException", fmt.Sprintf("Table already exists: %s", table.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to persist table: %v", err))
	}

	return table, nil
}

// Add the DeleteTable method to the TableService struct
// At some point in the future, DeleteTable will use a SQS interface which will queue up the work that's needed for deletion.
// For now the deletion will happen entirely in the request-response, so it can take a while.
func (s *TableService) DeleteTable(ctx context.Context, tableName string) (*models.Table, error) {
	if tableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	// The storage layer will handle the transaction of finding and updating the status.
	deletedTable, err := s.store.DeleteTable(ctx, tableName)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			// Translate storage error to a specific API error
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", tableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to delete table: %v", err))
	}

	return deletedTable, nil
}

// ListTables retrieves a list of table names.
func (s *TableService) ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
	// Simple validation
	if limit < 0 {
		return nil, "", models.New("ValidationException", "Limit must be non-negative")
	}

	tableNames, lastEvaluatedTableName, err := s.store.ListTables(ctx, limit, exclusiveStartTableName)
	if err != nil {
		return nil, "", models.New("InternalFailure", fmt.Sprintf("failed to list tables: %v", err))
	}

	return tableNames, lastEvaluatedTableName, nil
}

// UpdateTable updates a table's metadata.
func (s *TableService) UpdateTable(ctx context.Context, req *models.UpdateTableRequest) (*models.Table, error) {
	if req.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	table, err := s.store.UpdateTable(ctx, req)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", req.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to update table: %v", err))
	}

	return table, nil
}

// GetTable retrieves a table's metadata by name.
func (s *TableService) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	if tableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	table, err := s.store.GetTable(ctx, tableName)
	if err != nil {
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to get table: %v", err))
	}
	if table == nil {
		return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", tableName))
	}

	return table, nil
}

// Item Operations

// PutItem writes an item to the table.
func (s *TableService) PutItem(ctx context.Context, request *models.PutItemRequest) (*models.PutItemResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if len(request.Item) == 0 {
		return nil, models.New("ValidationException", "Item cannot be empty")
	}

	attributes, err := s.store.PutItem(ctx, request.TableName, request.Item, request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, request.ReturnValues)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", request.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to put item: %v", err))
	}
	resp := &models.PutItemResponse{Attributes: attributes}
	if request.ReturnConsumedCapacity != "" && request.ReturnConsumedCapacity != "NONE" {
		size := EstimateItemSize(request.Item)
		units := CalculateWriteCapacity(size)
		resp.ConsumedCapacity = BuildConsumedCapacity(request.TableName, units, false)
	}
	if request.ReturnItemCollectionMetrics != "" && request.ReturnItemCollectionMetrics != "NONE" {
		resp.ItemCollectionMetrics = BuildItemCollectionMetrics(request.TableName, request.Item)
	}
	return resp, nil
}

// GetItem retrieves an item by key.
func (s *TableService) GetItem(ctx context.Context, request *models.GetItemRequest) (*models.GetItemResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if len(request.Key) == 0 {
		return nil, models.New("ValidationException", "Key cannot be empty")
	}

	item, err := s.store.GetItem(ctx, request.TableName, request.Key, request.ProjectionExpression, request.ExpressionAttributeNames, request.ConsistentRead)
	if err != nil {
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to get item: %v", err))
	}
	// Note: Item not found just returns nil, nil in DynamoDB GetItem (with empty Item in response),
	// unless we specifically want to distinguish. The store returns nil, nil.
	// The API handler will map this to an empty Item field if nil.
	resp := &models.GetItemResponse{Item: item}
	if request.ReturnConsumedCapacity != "" && request.ReturnConsumedCapacity != "NONE" {
		size := EstimateItemSize(item)
		units := CalculateReadCapacity(size, request.ConsistentRead)
		resp.ConsumedCapacity = BuildConsumedCapacity(request.TableName, units, true)
	}
	return resp, nil
}

// DeleteItem deletes an item by key.
func (s *TableService) DeleteItem(ctx context.Context, request *models.DeleteItemRequest) (*models.DeleteItemResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if len(request.Key) == 0 {
		return nil, models.New("ValidationException", "Key cannot be empty")
	}

	attributes, err := s.store.DeleteItem(ctx, request.TableName, request.Key, request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, request.ReturnValues)
	if err != nil {
		// DeleteItem in DynamoDB is idempotent.
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", request.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to delete item: %v", err))
	}
	resp := &models.DeleteItemResponse{Attributes: attributes}
	if request.ReturnConsumedCapacity != "" && request.ReturnConsumedCapacity != "NONE" {
		// Minimum 1 WCU for delete
		resp.ConsumedCapacity = BuildConsumedCapacity(request.TableName, 1.0, false)
	}
	if request.ReturnItemCollectionMetrics != "" && request.ReturnItemCollectionMetrics != "NONE" {
		resp.ItemCollectionMetrics = BuildItemCollectionMetrics(request.TableName, request.Key)
	}
	return resp, nil
}

// validateTable performs business rule validation on the internal db model. In general all table validation
// should occur here. Types of table validaiton that might happen here is schema validation,
// presence of various appropriate values, and others.
func validateTable(table *models.Table) error {
	if table.TableName == "" {
		return models.New("ValidationException", "TableName cannot be empty")
	}
	if len(table.KeySchema) == 0 {
		return models.New("ValidationException", "KeySchema cannot be empty")
	}
	return nil
}

// UpdateItem updates an item.
func (s *TableService) UpdateItem(ctx context.Context, request *models.UpdateItemRequest) (*models.UpdateItemResponse, error) {
	if request.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if len(request.Key) == 0 {
		return nil, models.New("ValidationException", "Key cannot be empty")
	}

	// Call the store
	attributes, err := s.store.UpdateItem(ctx, request.TableName, request.Key, request.UpdateExpression, request.ConditionExpression, request.ExpressionAttributeNames, request.ExpressionAttributeValues, request.ReturnValues)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", request.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to update item: %v", err))
	}

	resp := &models.UpdateItemResponse{Attributes: attributes}
	if request.ReturnConsumedCapacity != "" && request.ReturnConsumedCapacity != "NONE" {
		// UpdateItem consumes WCU based on the new item size.
		// Since we don't have the final item here, we approximate with 1.0 or based on Attributes if ALL_NEW.
		// For now 1.0 as approximation.
		resp.ConsumedCapacity = BuildConsumedCapacity(request.TableName, 1.0, false)
	}
	if request.ReturnItemCollectionMetrics != "" && request.ReturnItemCollectionMetrics != "NONE" {
		resp.ItemCollectionMetrics = BuildItemCollectionMetrics(request.TableName, request.Key)
	}
	return resp, nil
}

// Scan retrieves items from the table.
func (s *TableService) Scan(ctx context.Context, input *models.ScanRequest) (*models.ScanResponse, error) {
	if input.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	// Call the store
	items, lastKey, err := s.store.Scan(ctx, input.TableName, input.FilterExpression, input.ProjectionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, input.Limit, input.ExclusiveStartKey, input.ConsistentRead)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", input.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to scan table: %v", err))
	}

	resp := &models.ScanResponse{
		Items:            items,
		Count:            int32(len(items)),
		ScannedCount:     int32(len(items)), // For now same as Count
		LastEvaluatedKey: lastKey,
	}

	if input.ReturnConsumedCapacity != "" && input.ReturnConsumedCapacity != "NONE" {
		totalSize := 0
		for _, itm := range items {
			totalSize += EstimateItemSize(itm)
		}
		units := CalculateReadCapacity(totalSize, input.ConsistentRead)
		resp.ConsumedCapacity = BuildConsumedCapacity(input.TableName, units, true)
	}

	return resp, nil
}

// Query retrieves items based on key conditions (partition key equality).
func (s *TableService) Query(ctx context.Context, input *models.QueryRequest) (*models.QueryResponse, error) {
	if input.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}
	if input.KeyConditionExpression == "" {
		return nil, models.New("ValidationException", "KeyConditionExpression cannot be empty")
	}

	// Call the store
	items, lastKey, err := s.store.Query(ctx, input.TableName, input.IndexName, input.KeyConditionExpression, input.FilterExpression, input.ProjectionExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues, input.Limit, input.ExclusiveStartKey, input.ConsistentRead)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", input.TableName))
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to query table: %v", err))
	}

	val := int32(len(items))

	resp := &models.QueryResponse{
		Items:            items,
		Count:            val,
		ScannedCount:     val,
		LastEvaluatedKey: lastKey,
	}

	if input.ReturnConsumedCapacity != "" && input.ReturnConsumedCapacity != "NONE" {
		totalSize := 0
		for _, itm := range items {
			totalSize += EstimateItemSize(itm)
		}
		units := CalculateReadCapacity(totalSize, input.ConsistentRead)
		resp.ConsumedCapacity = BuildConsumedCapacity(input.TableName, units, true)
	}

	return resp, nil
}

// BatchGetItem retrieves items from multiple tables.
func (s *TableService) BatchGetItem(ctx context.Context, input *models.BatchGetItemRequest) (*models.BatchGetItemResponse, error) {
	if len(input.RequestItems) == 0 {
		return nil, models.New("ValidationException", "RequestItems cannot be empty")
	}

	responses, unprocessed, err := s.store.BatchGetItem(ctx, input.RequestItems)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", "One or more requested tables not found")
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to batch get items: %v", err))
	}

	resp := &models.BatchGetItemResponse{
		Responses:       responses,
		UnprocessedKeys: unprocessed,
	}

	if input.ReturnConsumedCapacity != "" && input.ReturnConsumedCapacity != "NONE" {
		for tableName, items := range responses {
			totalSize := 0
			for _, itm := range items {
				totalSize += EstimateItemSize(itm)
			}
			// BatchGetItem consistency is per-table in RequestItems
			consistent := false
			if tableReq, ok := input.RequestItems[tableName]; ok {
				consistent = tableReq.ConsistentRead
			}
			units := CalculateReadCapacity(totalSize, consistent)
			resp.ConsumedCapacity = append(resp.ConsumedCapacity, *BuildConsumedCapacity(tableName, units, true))
		}
	}

	return resp, nil
}

// BatchWriteItem writes/deletes items in multiple tables.
func (s *TableService) BatchWriteItem(ctx context.Context, input *models.BatchWriteItemRequest) (*models.BatchWriteItemResponse, error) {
	if len(input.RequestItems) == 0 {
		return nil, models.New("ValidationException", "RequestItems cannot be empty")
	}

	// Basic validation of item count (DynamoDB limit is 25)
	count := 0
	for _, reqs := range input.RequestItems {
		count += len(reqs)
	}
	if count > 25 {
		return nil, models.New("ValidationException", "Member must have length less than or equal to 25")
	}

	unprocessed, err := s.store.BatchWriteItem(ctx, input.RequestItems)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", "One or more requested tables not found")
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to batch write items: %v", err))
	}

	resp := &models.BatchWriteItemResponse{
		UnprocessedItems: unprocessed,
	}

	if input.ReturnConsumedCapacity != "" && input.ReturnConsumedCapacity != "NONE" {
		for tableName, reqs := range input.RequestItems {
			totalSize := 0
			for _, req := range reqs {
				if req.PutRequest != nil {
					totalSize += EstimateItemSize(req.PutRequest.Item)
				} else {
					totalSize += 1024 // Approximate delete as 1KB
				}
			}
			units := CalculateWriteCapacity(totalSize)
			resp.ConsumedCapacity = append(resp.ConsumedCapacity, *BuildConsumedCapacity(tableName, units, false))
		}
	}

	if input.ReturnItemCollectionMetrics != "" && input.ReturnItemCollectionMetrics != "NONE" {
		resp.ItemCollectionMetrics = make(map[string][]models.ItemCollectionMetrics)
		for tableName, reqs := range input.RequestItems {
			for _, req := range reqs {
				var key map[string]models.AttributeValue
				if req.PutRequest != nil {
					key = req.PutRequest.Item
				} else {
					key = req.DeleteRequest.Key
				}
				resp.ItemCollectionMetrics[tableName] = append(resp.ItemCollectionMetrics[tableName], *BuildItemCollectionMetrics(tableName, key))
			}
		}
	}

	return resp, nil
}

func (s *TableService) TransactGetItems(ctx context.Context, input *models.TransactGetItemsRequest) (*models.TransactGetItemsResponse, error) {
	if len(input.TransactItems) == 0 {
		return nil, models.New("ValidationException", "TransactItems cannot be empty")
	}

	responses, err := s.store.TransactGetItems(ctx, input.TransactItems)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", "One or more requested tables not found")
		}
		// Pass through existing APIErrors
		return nil, err
	}

	resp := &models.TransactGetItemsResponse{
		Responses: responses,
	}

	if input.ReturnConsumedCapacity != "" && input.ReturnConsumedCapacity != "NONE" {
		tableUnits := make(map[string]float64)
		for i, r := range responses {
			if r.Item != nil {
				size := EstimateItemSize(r.Item)
				consistent := input.TransactItems[i].Get.ConsistentRead
				units := CalculateReadCapacity(size, consistent)
				tableUnits[input.TransactItems[i].Get.TableName] += units
			}
		}
		for tableName, units := range tableUnits {
			resp.ConsumedCapacity = append(resp.ConsumedCapacity, *BuildConsumedCapacity(tableName, units, true))
		}
	}

	return resp, nil
}

func (s *TableService) TransactWriteItems(ctx context.Context, input *models.TransactWriteItemsRequest) (*models.TransactWriteItemsResponse, error) {
	if len(input.TransactItems) == 0 {
		return nil, models.New("ValidationException", "TransactItems cannot be empty")
	}

	// Basic validation of item count (DynamoDB limit is 25)
	if len(input.TransactItems) > 25 {
		return nil, models.New("ValidationException", "TransactItems must have length less than or equal to 25")
	}

	err := s.store.TransactWriteItems(ctx, input.TransactItems, input.ClientRequestToken)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", "One or more requested tables not found")
		}
		return nil, err
	}

	resp := &models.TransactWriteItemsResponse{}

	if input.ReturnConsumedCapacity != "" && input.ReturnConsumedCapacity != "NONE" {
		tableUnits := make(map[string]float64)
		for _, item := range input.TransactItems {
			var tableName string
			var size int
			if item.Put != nil {
				tableName = item.Put.TableName
				size = EstimateItemSize(item.Put.Item)
			} else if item.Delete != nil {
				tableName = item.Delete.TableName
				size = 1024
			} else if item.Update != nil {
				tableName = item.Update.TableName
				size = 1024
			} else if item.ConditionCheck != nil {
				tableName = item.ConditionCheck.TableName
				size = 1024
			}
			tableUnits[tableName] += CalculateWriteCapacity(size)
		}
		for tableName, units := range tableUnits {
			resp.ConsumedCapacity = append(resp.ConsumedCapacity, *BuildConsumedCapacity(tableName, units, false))
		}
	}

	if input.ReturnItemCollectionMetrics != "" && input.ReturnItemCollectionMetrics != "NONE" {
		resp.ItemCollectionMetrics = make(map[string][]models.ItemCollectionMetrics)
		for _, item := range input.TransactItems {
			var tableName string
			var key map[string]models.AttributeValue
			if item.Put != nil {
				tableName = item.Put.TableName
				key = item.Put.Item
			} else if item.Delete != nil {
				tableName = item.Delete.TableName
				key = item.Delete.Key
			} else if item.Update != nil {
				tableName = item.Update.TableName
				key = item.Update.Key
			} else if item.ConditionCheck != nil {
				tableName = item.ConditionCheck.TableName
				key = item.ConditionCheck.Key
			}
			resp.ItemCollectionMetrics[tableName] = append(resp.ItemCollectionMetrics[tableName], *BuildItemCollectionMetrics(tableName, key))
		}
	}

	return resp, nil
}

// Stream Operations

func (s *TableService) ListStreams(ctx context.Context, input *models.ListStreamsRequest) (*models.ListStreamsResponse, error) {
	limit := int(input.Limit)
	if limit <= 0 {
		limit = 100
	}

	streams, lastArn, err := s.store.ListStreams(ctx, input.TableName, limit, input.ExclusiveStartStreamArn)
	if err != nil {
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to list streams: %v", err))
	}

	return &models.ListStreamsResponse{
		Streams:                streams,
		LastEvaluatedStreamArn: lastArn,
	}, nil
}

func (s *TableService) DescribeStream(ctx context.Context, input *models.DescribeStreamRequest) (*models.DescribeStreamResponse, error) {
	if input.StreamArn == "" {
		return nil, models.New("ValidationException", "StreamArn cannot be empty")
	}
	limit := int(input.Limit)
	if limit <= 0 {
		limit = 100
	}

	desc, err := s.store.DescribeStream(ctx, input.StreamArn, limit, input.ExclusiveStartShardId)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", "Stream not found")
		}
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to describe stream: %v", err))
	}

	return &models.DescribeStreamResponse{
		StreamDescription: *desc,
	}, nil
}

func (s *TableService) GetShardIterator(ctx context.Context, input *models.GetShardIteratorRequest) (*models.GetShardIteratorResponse, error) {
	if input.StreamArn == "" || input.ShardId == "" || input.ShardIteratorType == "" {
		return nil, models.New("ValidationException", "StreamArn, ShardId, and ShardIteratorType are required")
	}

	iter, err := s.store.GetShardIterator(ctx, input.StreamArn, input.ShardId, input.ShardIteratorType, input.SequenceNumber)
	if err != nil {
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to get shard iterator: %v", err))
	}

	return &models.GetShardIteratorResponse{
		ShardIterator: iter,
	}, nil
}

func (s *TableService) GetRecords(ctx context.Context, input *models.GetRecordsRequest) (*models.GetRecordsResponse, error) {
	if input.ShardIterator == "" {
		return nil, models.New("ValidationException", "ShardIterator cannot be empty")
	}
	limit := int(input.Limit)
	if limit <= 0 {
		limit = 1000
	}

	records, nextIter, err := s.store.GetRecords(ctx, input.ShardIterator, limit)
	if err != nil {
		return nil, models.New("InternalFailure", fmt.Sprintf("failed to get records: %v", err))
	}

	if records == nil {
		records = []models.Record{}
	}

	return &models.GetRecordsResponse{
		Records:           records,
		NextShardIterator: nextIter,
	}, nil
}

// Global Table Operations

func (s *TableService) CreateGlobalTable(ctx context.Context, req *models.CreateGlobalTableRequest) (*models.CreateGlobalTableResponse, error) {
	if req.GlobalTableName == "" {
		return nil, models.New("ValidationException", "GlobalTableName cannot be empty")
	}
	if len(req.ReplicationGroup) == 0 {
		return nil, models.New("ValidationException", "ReplicationGroup cannot be empty")
	}

	desc, err := s.store.CreateGlobalTable(ctx, req)
	if err != nil {
		if errors.Is(err, store.ErrTableExists) {
			return nil, models.New("ResourceInUseException", fmt.Sprintf("Global Table already exists: %s", req.GlobalTableName))
		}
		return nil, mapStoreError(err, "failed to create global table")
	}
	return &models.CreateGlobalTableResponse{GlobalTableDescription: *desc}, nil
}

func (s *TableService) UpdateGlobalTable(ctx context.Context, req *models.UpdateGlobalTableRequest) (*models.UpdateGlobalTableResponse, error) {
	if req.GlobalTableName == "" {
		return nil, models.New("ValidationException", "GlobalTableName cannot be empty")
	}

	desc, err := s.store.UpdateGlobalTable(ctx, req)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Global Table not found: %s", req.GlobalTableName))
		}
		return nil, mapStoreError(err, "failed to update global table")
	}
	return &models.UpdateGlobalTableResponse{GlobalTableDescription: *desc}, nil
}

func (s *TableService) DescribeGlobalTable(ctx context.Context, globalTableName string) (*models.DescribeGlobalTableResponse, error) {
	if globalTableName == "" {
		return nil, models.New("ValidationException", "GlobalTableName cannot be empty")
	}

	desc, err := s.store.DescribeGlobalTable(ctx, globalTableName)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Global Table not found: %s", globalTableName))
		}
		return nil, mapStoreError(err, "failed to describe global table")
	}
	return &models.DescribeGlobalTableResponse{GlobalTableDescription: *desc}, nil
}

func (s *TableService) ListGlobalTables(ctx context.Context, req *models.ListGlobalTablesRequest) (*models.ListGlobalTablesResponse, error) {
	tables, last, err := s.store.ListGlobalTables(ctx, req.Limit, req.ExclusiveStartGlobalTableName)
	if err != nil {
		return nil, mapStoreError(err, "failed to list global tables")
	}
	return &models.ListGlobalTablesResponse{
		GlobalTables:                 tables,
		LastEvaluatedGlobalTableName: last,
	}, nil
}

// TTL Operations

func (s *TableService) UpdateTimeToLive(ctx context.Context, req *models.UpdateTimeToLiveRequest) (*models.UpdateTimeToLiveResponse, error) {
	if req.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	spec, err := s.store.UpdateTimeToLive(ctx, req)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", req.TableName))
		}
		return nil, mapStoreError(err, "failed to update TTL")
	}

	return &models.UpdateTimeToLiveResponse{
		TimeToLiveSpecification: *spec,
	}, nil
}

func (s *TableService) DescribeTimeToLive(ctx context.Context, req *models.DescribeTimeToLiveRequest) (*models.DescribeTimeToLiveResponse, error) {
	if req.TableName == "" {
		return nil, models.New("ValidationException", "TableName cannot be empty")
	}

	desc, err := s.store.DescribeTimeToLive(ctx, req.TableName)
	if err != nil {
		if errors.Is(err, store.ErrTableNotFound) {
			return nil, models.New("ResourceNotFoundException", fmt.Sprintf("Table not found: %s", req.TableName))
		}
		return nil, mapStoreError(err, "failed to describe TTL")
	}

	return &models.DescribeTimeToLiveResponse{
		TimeToLiveDescription: *desc,
	}, nil
}

func mapStoreError(err error, msg string) error {
	if errors.Is(err, store.ErrTableNotFound) {
		return models.New("ResourceNotFoundException", msg+": not found")
	}
	if errors.Is(err, store.ErrTableExists) {
		return models.New("ResourceInUseException", msg+": already exists")
	}
	return models.New("InternalFailure", fmt.Sprintf("%s: %v", msg, err))
}
