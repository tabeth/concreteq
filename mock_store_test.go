package main

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/store"
)

// MockStore is a mock implementation of the Store interface for testing.
type MockStore struct {
	mock.Mock
}

var _ store.Store = (*MockStore)(nil)

func (m *MockStore) CreateQueue(ctx context.Context, name string, attributes, tags map[string]string) (map[string]string, error) {
	args := m.Called(ctx, name, attributes, tags)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}

func (m *MockStore) DeleteQueue(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}
func (m *MockStore) ListQueues(ctx context.Context, maxResults int, nextToken, queueNamePrefix string) ([]string, string, error) {
	args := m.Called(ctx, maxResults, nextToken, queueNamePrefix)
	var queues []string
	if args.Get(0) != nil {
		queues = args.Get(0).([]string)
	}
	return queues, args.String(1), args.Error(2)
}
func (m *MockStore) GetQueueAttributes(ctx context.Context, name string) (map[string]string, error) {
	args := m.Called(ctx, name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}
func (m *MockStore) SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error {
	args := m.Called(ctx, name, attributes)
	return args.Error(0)
}
func (m *MockStore) GetQueueURL(ctx context.Context, name string) (string, error) {
	args := m.Called(ctx, name)
	return args.String(0), args.Error(1)
}
func (m *MockStore) PurgeQueue(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}
func (m *MockStore) SendMessage(ctx context.Context, queueName string, message *models.SendMessageRequest) (*models.SendMessageResponse, error) {
	args := m.Called(ctx, queueName, message)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.SendMessageResponse), args.Error(1)
}
func (m *MockStore) SendMessageBatch(ctx context.Context, queueName string, req *models.SendMessageBatchRequest) (*models.SendMessageBatchResponse, error) {
	args := m.Called(ctx, queueName, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.SendMessageBatchResponse), args.Error(1)
}
func (m *MockStore) ReceiveMessage(ctx context.Context, queueName string, req *models.ReceiveMessageRequest) (*models.ReceiveMessageResponse, error) {
	args := m.Called(ctx, queueName, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ReceiveMessageResponse), args.Error(1)
}
func (m *MockStore) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	args := m.Called(ctx, queueName, receiptHandle)
	return args.Error(0)
}
func (m *MockStore) DeleteMessageBatch(ctx context.Context, queueName string, entries []models.DeleteMessageBatchRequestEntry) (*models.DeleteMessageBatchResponse, error) {
	args := m.Called(ctx, queueName, entries)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.DeleteMessageBatchResponse), args.Error(1)
}
func (m *MockStore) ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int) error {
	args := m.Called(ctx, queueName, receiptHandle, visibilityTimeout)
	return args.Error(0)
}
func (m *MockStore) ChangeMessageVisibilityBatch(ctx context.Context, queueName string, entries []models.ChangeMessageVisibilityBatchRequestEntry) (*models.ChangeMessageVisibilityBatchResponse, error) {
	args := m.Called(ctx, queueName, entries)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ChangeMessageVisibilityBatchResponse), args.Error(1)
}
func (m *MockStore) AddPermission(ctx context.Context, queueName, label string, accountIds []string, actions []string) error {
	args := m.Called(ctx, queueName, label, accountIds, actions)
	return args.Error(0)
}
func (m *MockStore) RemovePermission(ctx context.Context, queueName, label string) error { return nil }
func (m *MockStore) ListQueueTags(ctx context.Context, queueName string) (map[string]string, error) {
	args := m.Called(ctx, queueName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}
func (m *MockStore) TagQueue(ctx context.Context, queueName string, tags map[string]string) error {
	args := m.Called(ctx, queueName, tags)
	return args.Error(0)
}
func (m *MockStore) UntagQueue(ctx context.Context, queueName string, tagKeys []string) error {
	args := m.Called(ctx, queueName, tagKeys)
	return args.Error(0)
}
func (m *MockStore) ListDeadLetterSourceQueues(ctx context.Context, queueURL string, maxResults int, nextToken string) ([]string, string, error) {
	args := m.Called(ctx, queueURL, maxResults, nextToken)
	var queues []string
	if args.Get(0) != nil {
		queues = args.Get(0).([]string)
	}
	return queues, args.String(1), args.Error(2)
}
func (m *MockStore) StartMessageMoveTask(ctx context.Context, sourceArn, destinationArn string) (string, error) {
	// Simple mock implementation
	args := m.Called(ctx, sourceArn, destinationArn)
	return args.String(0), args.Error(1)
}
func (m *MockStore) CancelMessageMoveTask(ctx context.Context, taskHandle string) error {
	args := m.Called(ctx, taskHandle)
	return args.Error(0)
}
func (m *MockStore) ListMessageMoveTasks(ctx context.Context, sourceArn string) ([]models.ListMessageMoveTasksResultEntry, error) {
	args := m.Called(ctx, sourceArn)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]models.ListMessageMoveTasksResultEntry), args.Error(1)
}
