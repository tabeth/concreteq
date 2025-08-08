package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
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
func (s *FDBStore) CreateQueue(ctx context.Context, name string, attributes, tags map[string]string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := directory.Exists(tr, []string{"concreteq", name})
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, ErrQueueAlreadyExists
		}

		queueDir, err := directory.Create(tr, []string{"concreteq", name}, nil)
		if err != nil {
			return nil, err
		}

		if attributes != nil {
			attrsBytes, err := json.Marshal(attributes)
			if err != nil {
				return nil, err
			}
			tr.Set(queueDir.Pack(tuple.Tuple{"attributes"}), attrsBytes)
		}

		if tags != nil {
			tagsBytes, err := json.Marshal(tags)
			if err != nil {
				return nil, err
			}
			tr.Set(queueDir.Pack(tuple.Tuple{"tags"}), tagsBytes)
		}

		return nil, nil
	})
	return err
}

// --- Other Stub Implementations ---

func (s *FDBStore) DeleteQueue(ctx context.Context, name string) error {
	// TODO: Implement in FoundationDB
	return nil
}

func (s *FDBStore) ListQueues(ctx context.Context, maxResults int, nextToken, queueNamePrefix string) ([]string, string, error) {
	queues, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		return s.dir.List(tr, []string{})
	})
	if err != nil {
		return nil, "", err
	}

	allQueues := queues.([]string)
	var filteredQueues []string

	// Filter by prefix
	if queueNamePrefix != "" {
		for _, q := range allQueues {
			if strings.HasPrefix(q, queueNamePrefix) {
				filteredQueues = append(filteredQueues, q)
			}
		}
	} else {
		filteredQueues = allQueues
	}

	// Find starting index from nextToken
	startIndex := 0
	if nextToken != "" {
		for i, q := range filteredQueues {
			if q == nextToken {
				startIndex = i + 1
				break
			}
		}
	}

	if startIndex >= len(filteredQueues) {
		return []string{}, "", nil // No more results
	}

	// Determine the slice of queues to return
	var resultQueues []string
	var newNextToken string

	endIndex := len(filteredQueues)
	if maxResults > 0 {
		endIndex = startIndex + maxResults
	}

	if endIndex > len(filteredQueues) {
		endIndex = len(filteredQueues)
	}

	resultQueues = filteredQueues[startIndex:endIndex]

	if maxResults > 0 && endIndex < len(filteredQueues) {
		newNextToken = resultQueues[len(resultQueues)-1]
	}

	return resultQueues, newNextToken, nil
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
