package store

import "context"

// Store is the interface for the underlying storage system.
// It defines all the data operations required by the SQS-compatible API.
type Store interface {
	// Queue Management
	CreateQueue(ctx context.Context, name string, attributes, tags map[string]string) error
	DeleteQueue(ctx context.Context, name string) error
	ListQueues(ctx context.Context, maxResults int, nextToken, queueNamePrefix string) ([]string, string, error)
	GetQueueAttributes(ctx context.Context, name string) (map[string]string, error)
	SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error
	GetQueueURL(ctx context.Context, name string) (string, error)
	PurgeQueue(ctx context.Context, name string) error

	// Message Management
	SendMessage(ctx context.Context, queueName string, messageBody string) (string, error)
	SendMessageBatch(ctx context.Context, queueName string, messages []string) ([]string, error)
	ReceiveMessage(ctx context.Context, queueName string) (string, string, error) // returns message, receiptHandle, error
	DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error
	DeleteMessageBatch(ctx context.Context, queueName string, receiptHandles []string) error
	ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int) error
	ChangeMessageVisibilityBatch(ctx context.Context, queueName string, entries map[string]int) error

	// Permissions
	AddPermission(ctx context.Context, queueName, label string, permissions map[string][]string) error
	RemovePermission(ctx context.Context, queueName, label string) error

	// Tagging
	ListQueueTags(ctx context.Context, queueName string) (map[string]string, error)
	TagQueue(ctx context.Context, queueName string, tags map[string]string) error
	UntagQueue(ctx context.Context, queueName string, tagKeys []string) error

	// Dead-Letter Queues
	ListDeadLetterSourceQueues(ctx context.Context, queueURL string) ([]string, error)

	// Message Move Tasks
	StartMessageMoveTask(ctx context.Context, sourceArn, destinationArn string) (string, error)
	CancelMessageMoveTask(ctx context.Context, taskHandle string) error
	ListMessageMoveTasks(ctx context.Context, sourceArn string) ([]string, error)
}
