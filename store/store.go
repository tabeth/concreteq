// Package store defines the interface for the persistence layer of the application.
// It abstracts the specific database implementation (like FoundationDB) from the
// HTTP handlers, allowing for easier testing and potential future database migrations.
package store

import (
	"context"
	"errors"

	"github.com/tabeth/concreteq/models"
)

// Pre-defined error variables allow for type-safe error checking in the handler layer.
// This is more robust than checking for error strings. For example, handlers can use
// `errors.Is(err, store.ErrQueueDoesNotExist)` to implement specific logic for that case.
var (
	// ErrQueueDoesNotExist is returned when trying to operate on a queue that does not exist.
	// This maps to the SQS `QueueDoesNotExist` error.
	ErrQueueDoesNotExist = errors.New("queue does not exist")

	// ErrPurgeQueueInProgress is returned when a purge request is made for a queue
	// that has been purged in the last 60 seconds, as per SQS behavior.
	ErrPurgeQueueInProgress = errors.New("purge queue in progress")

	// ErrInvalidReceiptHandle is returned when a receipt handle is malformed, expired, or invalid.
	// This maps to the SQS `ReceiptHandleIsInvalid` error.
	ErrInvalidReceiptHandle = errors.New("receipt handle is invalid")

	// ErrMessageNotInflight is returned when trying to change the visibility of a message
	// that is not currently in-flight.
	ErrMessageNotInflight = errors.New("message not in flight")
)

// Store is the central interface for all data persistence operations.
// It defines a contract that any storage backend must fulfill to be compatible with this application.
// The methods are designed to map closely to the actions available in the Amazon SQS API.
// Using `context.Context` as the first parameter in all methods is a standard Go practice
// for handling request-scoped values, cancellation, and deadlines.
type Store interface {
	// --- Queue Management ---

	// CreateQueue creates a new queue with the given name, attributes, and tags.
	// If the queue already exists, it returns the existing attributes for idempotency checks.
	CreateQueue(ctx context.Context, name string, attributes map[string]string, tags map[string]string) (existingAttributes map[string]string, err error)
	// DeleteQueue removes a queue and all of its messages.
	DeleteQueue(ctx context.Context, name string) error
	// ListQueues returns a list of queue names, supporting pagination and prefix filtering.
	ListQueues(ctx context.Context, maxResults int, nextToken, queueNamePrefix string) (queueNames []string, newNextToken string, err error)
	// GetQueueAttributes retrieves the attributes for a specified queue.
	GetQueueAttributes(ctx context.Context, name string) (map[string]string, error)
	// SetQueueAttributes sets the attributes for a specified queue.
	SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error
	// GetQueueURL retrieves the URL for a queue by its name.
	GetQueueURL(ctx context.Context, name string) (string, error)
	// PurgeQueue deletes all messages from a queue without deleting the queue itself.
	PurgeQueue(ctx context.Context, name string) error

	// --- Message Management ---

	// SendMessage adds a message to the specified queue.
	SendMessage(ctx context.Context, queueName string, message *models.SendMessageRequest) (*models.SendMessageResponse, error)
	// SendMessageBatch adds a batch of messages to the specified queue.
	SendMessageBatch(ctx context.Context, queueName string, req *models.SendMessageBatchRequest) (*models.SendMessageBatchResponse, error)
	// ReceiveMessage retrieves one or more messages from a queue.
	ReceiveMessage(ctx context.Context, queueName string, req *models.ReceiveMessageRequest) (*models.ReceiveMessageResponse, error)
	// DeleteMessage deletes a message from a queue using its receipt handle.
	DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error
	// DeleteMessageBatch deletes a batch of messages from a queue.
	DeleteMessageBatch(ctx context.Context, queueName string, entries []models.DeleteMessageBatchRequestEntry) (*models.DeleteMessageBatchResponse, error)
	// ChangeMessageVisibility changes the visibility timeout of a specific message.
	ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int) error
	// ChangeMessageVisibilityBatch changes the visibility timeout for a batch of messages.
	ChangeMessageVisibilityBatch(ctx context.Context, queueName string, entries []models.ChangeMessageVisibilityBatchRequestEntry) (*models.ChangeMessageVisibilityBatchResponse, error)

	// --- Permissions --- (Not yet implemented)

	// AddPermission adds a permission to a queue for a specific principal.
	AddPermission(ctx context.Context, queueName, label string, permissions map[string][]string) error
	// RemovePermission removes a permission from a queue.
	RemovePermission(ctx context.Context, queueName, label string) error

	// --- Tagging --- (Not yet implemented)

	// ListQueueTags lists the tags for a specified queue.
	ListQueueTags(ctx context.Context, queueName string) (map[string]string, error)
	// TagQueue adds tags to a queue.
	TagQueue(ctx context.Context, queueName string, tags map[string]string) error
	// UntagQueue removes tags from a queue.
	UntagQueue(ctx context.Context, queueName string, tagKeys []string) error

	// --- Dead-Letter Queues --- (Not yet implemented)

	// ListDeadLetterSourceQueues lists queues that have the specified queue as a dead-letter queue.
	ListDeadLetterSourceQueues(ctx context.Context, queueURL string, maxResults int, nextToken string) ([]string, string, error)

	// --- Message Move Tasks --- (Not yet implemented)

	// StartMessageMoveTask starts a task to move messages from a source queue to a destination queue.
	StartMessageMoveTask(ctx context.Context, req *models.StartMessageMoveTaskRequest) (*models.StartMessageMoveTaskResponse, error)
	// CancelMessageMoveTask cancels a message move task.
	CancelMessageMoveTask(ctx context.Context, req *models.CancelMessageMoveTaskRequest) (*models.CancelMessageMoveTaskResponse, error)
	// ListMessageMoveTasks lists the message move tasks for a specific source queue.
	ListMessageMoveTasks(ctx context.Context, req *models.ListMessageMoveTasksRequest) (*models.ListMessageMoveTasksResponse, error)
}
