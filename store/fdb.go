package store

import (
	"context"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

var (
	// ErrQueueAlreadyExists is returned when trying to create a queue that already exists.
	ErrQueueAlreadyExists = errors.New("queue already exists")
)

// FDBStore is a FoundationDB implementation of the Store interface.
type FDBStore struct {
	db  fdb.Database
	dir directory.DirectorySubspace
}

// NewFDBStore creates a new FDBStore.
func NewFDBStore() (*FDBStore, error) {
	fdb.MustAPIVersion(740)
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	dir, err := directory.CreateOrOpen(db, []string{"concreteq"}, nil)
	if err != nil {
		return nil, err
	}

	return &FDBStore{db: db, dir: dir}, nil
}

// CreateQueue creates a new queue in FoundationDB.
// It creates a dedicated subspace for the queue to store its metadata and messages.
func (s *FDBStore) CreateQueue(ctx context.Context, name string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := directory.Exists(tr, []string{"concreteq", name})
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, ErrQueueAlreadyExists
		}

		_, err = directory.Create(tr, []string{"concreteq", name}, nil)
		if err != nil {
			return nil, err
		}

		// We can store initial metadata here if needed.
		// For example, creation timestamp.
		// queueSubspace, _ := directory.Open(tr, []string{"concreteq", name}, nil)
		// tr.Set(queueSubspace.Pack(tuple.Tuple{"metadata", "created_at"}), []byte(time.Now().Format(time.RFC3339)))

		return nil, nil
	})
	return err
}

// --- Other Stub Implementations ---

func (s *FDBStore) DeleteQueue(ctx context.Context, name string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) ListQueues(ctx context.Context) ([]string, error) {
	// TODO: Implement in FoundationDB
	return nil, nil
}

func (s *FDBStore) GetQueueAttributes(ctx context.Context, name string) (map[string]string, error) {
	// TODO: Implement in FoundationDB
	return nil, nil
}

func (s *FDBStore) SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) GetQueueURL(ctx context.Context, name string) (string, error) {
	// TODO: Implement in FoundationDB
	return "", nil
}

func (s *FDBStore) PurgeQueue(ctx context.Context, name string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) SendMessage(ctx context.Context, queueName string, messageBody string) (string, error) {
	// TODO: Implement in FoundationDB
	return "", nil
}

func (s *FDBStore) SendMessageBatch(ctx context.Context, queueName string, messages []string) ([]string, error) {
	// TODO: Implement in FoundationDB
	return nil, nil
}

func (s *FDBStore) ReceiveMessage(ctx context.Context, queueName string) (string, string, error) {
	// TODO: Implement in FoundationDB
	return "", "", nil
}

func (s *FDBStore) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) DeleteMessageBatch(ctx context.Context, queueName string, receiptHandles []string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) ChangeMessageVisibilityBatch(ctx context.Context, queueName string, entries map[string]int) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) AddPermission(ctx context.Context, queueName, label string, permissions map[string][]string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) RemovePermission(ctx context.Context, queueName, label string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) ListQueueTags(ctx context.Context, queueName string) (map[string]string, error) {
	// TODO: Implement in FoundationDB
	return nil, nil
}

func (s *FDBStore) TagQueue(ctx context.Context, queueName string, tags map[string]string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) UntagQueue(ctx context.Context, queueName string, tagKeys []string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) ListDeadLetterSourceQueues(ctx context.Context, queueURL string) ([]string, error) {
	// TODO: Implement in FoundationDB
	return nil, nil
}

func (s *FDBStore) StartMessageMoveTask(ctx context.Context, sourceArn, destinationArn string) (string, error) {
	// TODO: Implement in FoundationDB
	return "", nil
}

func (s *FDBStore) CancelMessageMoveTask(ctx context.Context, taskHandle string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) ListMessageMoveTasks(ctx context.Context, sourceArn string) ([]string, error) {
	// TODO: Implement in FoundationDB
	return nil, nil
}
