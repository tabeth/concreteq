// Package main provides the entry point and integration tests.
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

// CreateQueue mocks the CreateQueue method.
func (m *MockStore) CreateQueue(ctx context.Context, name string, attributes, tags map[string]string) (map[string]string, error) {
	args := m.Called(ctx, name, attributes, tags)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}

// DeleteQueue mocks the DeleteQueue method.
func (m *MockStore) DeleteQueue(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

// ListQueues mocks the ListQueues method.
func (m *MockStore) ListQueues(ctx context.Context, maxResults int, nextToken, queueNamePrefix string) ([]string, string, error) {
	args := m.Called(ctx, maxResults, nextToken, queueNamePrefix)
	var queues []string
	if args.Get(0) != nil {
		queues = args.Get(0).([]string)
	}
	return queues, args.String(1), args.Error(2)
}

// GetQueueAttributes mocks the GetQueueAttributes method.
func (m *MockStore) GetQueueAttributes(ctx context.Context, name string) (map[string]string, error) {
	args := m.Called(ctx, name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}

// SetQueueAttributes mocks the SetQueueAttributes method.
func (m *MockStore) SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error {
	args := m.Called(ctx, name, attributes)
	return args.Error(0)
}

// GetQueueURL mocks the GetQueueURL method.
func (m *MockStore) GetQueueURL(ctx context.Context, name string) (string, error) {
	args := m.Called(ctx, name)
	return args.String(0), args.Error(1)
}

// PurgeQueue mocks the PurgeQueue method.
func (m *MockStore) PurgeQueue(ctx context.Context, name string) error {
	args := m.Called(ctx, name)
	return args.Error(0)
}

// SendMessage mocks the SendMessage method.
func (m *MockStore) SendMessage(ctx context.Context, queueName string, message *models.SendMessageRequest) (*models.SendMessageResponse, error) {
	args := m.Called(ctx, queueName, message)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.SendMessageResponse), args.Error(1)
}

// SendMessageBatch mocks the SendMessageBatch method.
func (m *MockStore) SendMessageBatch(ctx context.Context, queueName string, req *models.SendMessageBatchRequest) (*models.SendMessageBatchResponse, error) {
	args := m.Called(ctx, queueName, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.SendMessageBatchResponse), args.Error(1)
}

// ReceiveMessage mocks the ReceiveMessage method.
func (m *MockStore) ReceiveMessage(ctx context.Context, queueName string, req *models.ReceiveMessageRequest) (*models.ReceiveMessageResponse, error) {
	args := m.Called(ctx, queueName, req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ReceiveMessageResponse), args.Error(1)
}

// DeleteMessage mocks the DeleteMessage method.
func (m *MockStore) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	args := m.Called(ctx, queueName, receiptHandle)
	return args.Error(0)
}

// DeleteMessageBatch mocks the DeleteMessageBatch method.
func (m *MockStore) DeleteMessageBatch(ctx context.Context, queueName string, entries []models.DeleteMessageBatchRequestEntry) (*models.DeleteMessageBatchResponse, error) {
	args := m.Called(ctx, queueName, entries)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.DeleteMessageBatchResponse), args.Error(1)
}

// ChangeMessageVisibility mocks the ChangeMessageVisibility method.
func (m *MockStore) ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int) error {
	args := m.Called(ctx, queueName, receiptHandle, visibilityTimeout)
	return args.Error(0)
}

// ChangeMessageVisibilityBatch mocks the ChangeMessageVisibilityBatch method.
func (m *MockStore) ChangeMessageVisibilityBatch(ctx context.Context, queueName string, entries []models.ChangeMessageVisibilityBatchRequestEntry) (*models.ChangeMessageVisibilityBatchResponse, error) {
	args := m.Called(ctx, queueName, entries)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.ChangeMessageVisibilityBatchResponse), args.Error(1)
}

// AddPermission mocks the AddPermission method.
func (m *MockStore) AddPermission(ctx context.Context, queueName, label string, accountIds []string, actions []string) error {
	return nil
}

// RemovePermission mocks the RemovePermission method.
func (m *MockStore) RemovePermission(ctx context.Context, queueName, label string) error { return nil }

// ListQueueTags mocks the ListQueueTags method.
func (m *MockStore) ListQueueTags(ctx context.Context, queueName string) (map[string]string, error) {
	args := m.Called(ctx, queueName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}

// TagQueue mocks the TagQueue method.
func (m *MockStore) TagQueue(ctx context.Context, queueName string, tags map[string]string) error {
	args := m.Called(ctx, queueName, tags)
	return args.Error(0)
}

// UntagQueue mocks the UntagQueue method.
func (m *MockStore) UntagQueue(ctx context.Context, queueName string, tagKeys []string) error {
	args := m.Called(ctx, queueName, tagKeys)
	return args.Error(0)
}

// ListDeadLetterSourceQueues mocks the ListDeadLetterSourceQueues method.
func (m *MockStore) ListDeadLetterSourceQueues(ctx context.Context, queueURL string, maxResults int, nextToken string) ([]string, string, error) {
	args := m.Called(ctx, queueURL, maxResults, nextToken)
	var queues []string
	if args.Get(0) != nil {
		queues = args.Get(0).([]string)
	}
	return queues, args.String(1), args.Error(2)
}

// StartMessageMoveTask mocks the StartMessageMoveTask method.
func (m *MockStore) StartMessageMoveTask(ctx context.Context, sourceArn, destinationArn string) (string, error) {
	// Simple mock implementation
	args := m.Called(ctx, sourceArn, destinationArn)
	return args.String(0), args.Error(1)
}

// CancelMessageMoveTask mocks the CancelMessageMoveTask method.
func (m *MockStore) CancelMessageMoveTask(ctx context.Context, taskHandle string) error {
	args := m.Called(ctx, taskHandle)
	return args.Error(0)
}

// ListMessageMoveTasks mocks the ListMessageMoveTasks method.
func (m *MockStore) ListMessageMoveTasks(ctx context.Context, sourceArn string) ([]models.ListMessageMoveTasksResultEntry, error) {
	args := m.Called(ctx, sourceArn)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]models.ListMessageMoveTasksResultEntry), args.Error(1)
}
