package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/store"
)

// stringPtr helper (duplicated or shared if exported, but local is safe)
func strPtr(s string) *string {
	return &s
}

func TestTableService_Streams(t *testing.T) {
	mock := &mockStore{
		ListStreamsFunc: func(ctx context.Context, tableName string, limit int, startArn string) ([]models.StreamSummary, string, error) {
			return []models.StreamSummary{{StreamArn: "arn"}}, "", nil
		},
		DescribeStreamFunc: func(ctx context.Context, arn string, limit int, startShard string) (*models.StreamDescription, error) {
			return &models.StreamDescription{StreamArn: arn}, nil
		},
		GetShardIteratorFunc: func(ctx context.Context, arn string, shardId string, iterType string, seq string) (string, error) {
			return "iterator", nil
		},
		GetRecordsFunc: func(ctx context.Context, iter string, limit int) ([]models.Record, string, error) {
			return []models.Record{}, "next", nil
		},
	}
	service := NewTableService(mock)

	// ListStreams
	streams, err := service.ListStreams(context.Background(), &models.ListStreamsRequest{})
	assert.NoError(t, err)
	assert.Len(t, streams.Streams, 1)

	// DescribeStream
	desc, err := service.DescribeStream(context.Background(), &models.DescribeStreamRequest{StreamArn: "arn"})
	assert.NoError(t, err)
	assert.Equal(t, "arn", desc.StreamDescription.StreamArn)

	// GetShardIterator
	iter, err := service.GetShardIterator(context.Background(), &models.GetShardIteratorRequest{StreamArn: "arn", ShardId: "shard", ShardIteratorType: "TRIM_HORIZON"})
	assert.NoError(t, err)
	assert.Equal(t, "iterator", iter.ShardIterator)

	// GetRecords
	recs, err := service.GetRecords(context.Background(), &models.GetRecordsRequest{ShardIterator: "iterator"})
	assert.NoError(t, err)
	assert.NotNil(t, recs.Records)
}

func TestTableService_BatchMethods(t *testing.T) {
	mock := &mockStore{
		BatchGetItemFunc: func(ctx context.Context, items map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
			return map[string][]map[string]models.AttributeValue{"t": {{}}}, nil, nil
		},
		BatchWriteItemFunc: func(ctx context.Context, items map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error) {
			return nil, nil
		},
	}
	service := NewTableService(mock)

	// BatchGetItem
	bg, err := service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{RequestItems: map[string]models.KeysAndAttributes{"t": {Keys: []map[string]models.AttributeValue{{"pk": {S: strPtr("1")}}}}}})
	assert.NoError(t, err)
	assert.NotEmpty(t, bg.Responses)

	// BatchWriteItem
	bw, err := service.BatchWriteItem(context.Background(), &models.BatchWriteItemRequest{RequestItems: map[string][]models.WriteRequest{"t": {{PutRequest: &models.PutRequest{Item: map[string]models.AttributeValue{"pk": {S: strPtr("1")}}}}}}})
	assert.NoError(t, err)
	assert.Empty(t, bw.UnprocessedItems)
}

func TestTableService_TransactMethods(t *testing.T) {
	mock := &mockStore{
		TransactGetItemsFunc: func(ctx context.Context, items []models.TransactGetItem) ([]models.ItemResponse, error) {
			return []models.ItemResponse{{Item: map[string]models.AttributeValue{"pk": {S: strPtr("1")}}}}, nil
		},
		TransactWriteItemsFunc: func(ctx context.Context, items []models.TransactWriteItem, token string) error {
			return nil
		},
	}
	service := NewTableService(mock)

	// TransactGetItems
	tg, err := service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{TransactItems: []models.TransactGetItem{{Get: models.GetItemRequest{TableName: "t", Key: map[string]models.AttributeValue{"pk": {S: strPtr("1")}}}}}})
	assert.NoError(t, err)
	assert.NotEmpty(t, tg.Responses)

	// TransactWriteItems
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{TransactItems: []models.TransactWriteItem{{Put: &models.PutItemRequest{TableName: "t", Item: map[string]models.AttributeValue{"pk": {S: strPtr("1")}}}}}})
	assert.NoError(t, err)
}

func TestTableService_StreamErrors(t *testing.T) {
	mock := &mockStore{
		ListStreamsFunc: func(ctx context.Context, tableName string, limit int, startArn string) ([]models.StreamSummary, string, error) {
			return nil, "", errors.New("store error")
		},
		DescribeStreamFunc: func(ctx context.Context, arn string, limit int, startShard string) (*models.StreamDescription, error) {
			return nil, errors.New("store error")
		},
		GetShardIteratorFunc: func(ctx context.Context, arn string, shardId string, iterType string, seq string) (string, error) {
			return "", errors.New("store error")
		},
		GetRecordsFunc: func(ctx context.Context, iter string, limit int) ([]models.Record, string, error) {
			return nil, "", errors.New("store error")
		},
	}
	service := NewTableService(mock)

	// ListStreams Error
	_, err := service.ListStreams(context.Background(), &models.ListStreamsRequest{})
	assert.Error(t, err)

	// DescribeStream Validation
	_, err = service.DescribeStream(context.Background(), &models.DescribeStreamRequest{StreamArn: ""})
	assert.Error(t, err)

	// DescribeStream Store Error
	_, err = service.DescribeStream(context.Background(), &models.DescribeStreamRequest{StreamArn: "arn"})
	assert.Error(t, err)

	// GetShardIterator Validation
	_, err = service.GetShardIterator(context.Background(), &models.GetShardIteratorRequest{})
	assert.Error(t, err)

	// GetShardIterator Store Error
	_, err = service.GetShardIterator(context.Background(), &models.GetShardIteratorRequest{StreamArn: "arn", ShardId: "1", ShardIteratorType: "LATEST"})
	assert.Error(t, err)

	// GetRecords Validation
	_, err = service.GetRecords(context.Background(), &models.GetRecordsRequest{ShardIterator: ""})
	assert.Error(t, err)

	// GetRecords Store Error
	_, err = service.GetRecords(context.Background(), &models.GetRecordsRequest{ShardIterator: "iter"})
	assert.Error(t, err)
}

func TestTableService_UpdateTable(t *testing.T) {
	mock := &mockStore{
		UpdateTableFunc: func(ctx context.Context, tableName string, spec *models.StreamSpecification) (*models.Table, error) {
			return &models.Table{TableName: tableName, StreamSpecification: spec}, nil
		},
	}
	service := NewTableService(mock)

	// Success
	up, err := service.UpdateTable(context.Background(), "table", &models.StreamSpecification{StreamEnabled: true})
	assert.NoError(t, err)
	assert.NotNil(t, up.StreamSpecification)
	assert.True(t, up.StreamSpecification.StreamEnabled)

	// Validation - Empty Name
	_, err = service.UpdateTable(context.Background(), "", &models.StreamSpecification{})
	assert.Error(t, err)

	// Store Error
	mock.UpdateTableFunc = func(ctx context.Context, tableName string, spec *models.StreamSpecification) (*models.Table, error) {
		return nil, errors.New("store error")
	}
	_, err = service.UpdateTable(context.Background(), "table", &models.StreamSpecification{})
	assert.Error(t, err)

	// Not Found Error
	mock.UpdateTableFunc = func(ctx context.Context, tableName string, spec *models.StreamSpecification) (*models.Table, error) {
		return nil, store.ErrTableNotFound
	}
	_, err = service.UpdateTable(context.Background(), "table", nil)
	assert.Error(t, err)
}

func TestTableService_BatchErrors(t *testing.T) {
	mock := &mockStore{}
	service := NewTableService(mock)

	// BatchGetItem Validation
	_, err := service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{})
	assert.Error(t, err)

	// BatchWriteItem Validation (Empty)
	_, err = service.BatchWriteItem(context.Background(), &models.BatchWriteItemRequest{})
	assert.Error(t, err)

	// BatchWriteItem Validation (Too many - mock not needed, logic is in service)
	manyItems := make(map[string][]models.WriteRequest)
	manyItems["t"] = make([]models.WriteRequest, 26)
	_, err = service.BatchWriteItem(context.Background(), &models.BatchWriteItemRequest{RequestItems: manyItems})
	assert.Error(t, err)

	// Store Errors
	mock.BatchGetItemFunc = func(ctx context.Context, items map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
		return nil, nil, errors.New("err")
	}
	_, err = service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{RequestItems: map[string]models.KeysAndAttributes{"t": {}}})
	assert.Error(t, err)

	mock.BatchWriteItemFunc = func(ctx context.Context, items map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error) {
		return nil, errors.New("err")
	}
	_, err = service.BatchWriteItem(context.Background(), &models.BatchWriteItemRequest{RequestItems: map[string][]models.WriteRequest{"t": {{}}}})
	assert.Error(t, err)

	// Store TableNotFound
	mock.BatchGetItemFunc = func(ctx context.Context, items map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
		return nil, nil, store.ErrTableNotFound
	}
	_, err = service.BatchGetItem(context.Background(), &models.BatchGetItemRequest{RequestItems: map[string]models.KeysAndAttributes{"t": {}}})
	assert.Error(t, err)
}

func TestTableService_TransactErrors(t *testing.T) {
	mock := &mockStore{}
	service := NewTableService(mock)

	// TransactGet Validation
	_, err := service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{})
	assert.Error(t, err)

	// TransactWrite Validation (Empty)
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{})
	assert.Error(t, err)

	// TransactWrite Validation (Too many)
	items := make([]models.TransactWriteItem, 26)
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{TransactItems: items})
	assert.Error(t, err)

	// Store Errors
	mock.TransactGetItemsFunc = func(ctx context.Context, items []models.TransactGetItem) ([]models.ItemResponse, error) {
		return nil, errors.New("err")
	}
	_, err = service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{TransactItems: []models.TransactGetItem{{}}})
	assert.Error(t, err)

	mock.TransactWriteItemsFunc = func(ctx context.Context, items []models.TransactWriteItem, token string) error {
		return errors.New("err")
	}
	_, err = service.TransactWriteItems(context.Background(), &models.TransactWriteItemsRequest{TransactItems: []models.TransactWriteItem{{}}})
	assert.Error(t, err)

	// Table Not Found
	mock.TransactGetItemsFunc = func(ctx context.Context, items []models.TransactGetItem) ([]models.ItemResponse, error) {
		return nil, store.ErrTableNotFound
	}
	_, err = service.TransactGetItems(context.Background(), &models.TransactGetItemsRequest{TransactItems: []models.TransactGetItem{{}}})
	assert.Error(t, err)
}
