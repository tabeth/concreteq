package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/tabeth/concreteq/models"
)

// FDBStore is a FoundationDB implementation of the Store interface.
type FDBStore struct {
	db  fdb.Database
	dir directory.DirectorySubspace
}

// NewFDBStore creates a new FDBStore.
func NewFDBStore() (*FDBStore, error) {
	fdb.MustAPIVersion(730)
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
func (s *FDBStore) CreateQueue(ctx context.Context, name string, attributes map[string]string, tags map[string]string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// The directory layer provides a way to organize data. We'll create a directory for each queue.
		queueDir, err := s.dir.Create(tr, []string{name}, nil)
		if err != nil {
			// TODO(tabeth): Maybe instead of checking for this string we save the directory value else where
			// and check it or otherwise see if there's a way to get the presence of the directory rather than
			// parse error strings.

			// The Go binding's directory layer returns a generic error when the directory exists.
			// We check for this specific string. This is brittle but a common pattern with this library.
			if strings.Contains(err.Error(), "already exists") {
				return nil, ErrQueueAlreadyExists
			}
			return nil, err
		}

		// Store attributes as a JSON blob under an 'attributes' key in the queue's directory.
		if len(attributes) > 0 {
			attrsBytes, err := json.Marshal(attributes)
			if err != nil {
				return nil, err // Should not happen with map[string]string
			}
			tr.Set(queueDir.Pack(tuple.Tuple{"attributes"}), attrsBytes)
		}

		// Store tags as a JSON blob under a 'tags' key.
		if len(tags) > 0 {
			tagsBytes, err := json.Marshal(tags)
			if err != nil {
				return nil, err // Should not happen with map[string]string
			}
			tr.Set(queueDir.Pack(tuple.Tuple{"tags"}), tagsBytes)
		}

		return nil, nil
	})
	return err
}

// DeleteQueue deletes a queue from FoundationDB.
func (s *FDBStore) DeleteQueue(ctx context.Context, name string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{name})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrQueueDoesNotExist
		}

		_, err = s.dir.Remove(tr, []string{name})
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
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
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Check if queue exists
		exists, err := s.dir.Exists(tr, []string{name})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrQueueDoesNotExist
		}

		queueDir, err := s.dir.Open(tr, []string{name}, nil)
		if err != nil {
			// This should not happen if `exists` is true, but handle defensively.
			return nil, err
		}

		// Check for recent purge to enforce 60-second cooldown
		now := time.Now().Unix()
		lastPurgedBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"last_purged_at"})).Get()
		if err != nil {
			return nil, err
		}

		if len(lastPurgedBytes) > 0 {
			lastPurged, err := strconv.ParseInt(string(lastPurgedBytes), 10, 64)
			if err == nil { // Only check if parsing was successful
				if now-lastPurged < 60 {
					return nil, ErrPurgeQueueInProgress
				}
			}
			// If parsing fails, we can choose to ignore and proceed with the purge.
		}

		// Purge messages. We assume messages are in a "messages" subdirectory.
		// We clear the range of keys within this subdirectory without deleting the directory itself.
		messagesDir := queueDir.Sub("messages")
		prefix := messagesDir.Pack(tuple.Tuple{})
		pr, err := fdb.PrefixRange(prefix)
		if err != nil {
			return nil, err
		}
		tr.ClearRange(pr)

		// Update last purged timestamp
		tr.Set(queueDir.Pack(tuple.Tuple{"last_purged_at"}), []byte(strconv.FormatInt(now, 10)))

		return nil, nil
	})
	return err
}

func (s *FDBStore) SendMessage(ctx context.Context, queueName string, message *models.SendMessageRequest) (*models.SendMessageResponse, error) {
	resp, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Check if queue exists
		exists, err := s.dir.Exists(tr, []string{queueName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrQueueDoesNotExist
		}

		queueDir, err := s.dir.Open(tr, []string{queueName}, nil)
		if err != nil {
			return nil, err
		}
		messagesDir := queueDir.Sub("messages")
		dedupDir := queueDir.Sub("dedup")

		isFifo := strings.HasSuffix(queueName, ".fifo")

		// FIFO Deduplication Check
		if isFifo && message.MessageDeduplicationId != nil && *message.MessageDeduplicationId != "" {
			dedupKey := dedupDir.Pack(tuple.Tuple{*message.MessageDeduplicationId})
			val, err := tr.Get(dedupKey).Get()
			if err != nil {
				return nil, err
			}
			if val != nil {
				// Deduplication ID already exists. SQS accepts the message but doesn't deliver it.
				// We can simulate this by just returning a successful response without storing the message again.
				// For simplicity, we'll just return the stored message ID.
				// A more complete implementation would return the original message's ID and sequence number.
				return &models.SendMessageResponse{
					MessageId:      string(val), // Assuming we stored the message ID here
					MD5OfMessageBody: "deduplicated", // Placeholder
				}, nil
			}
		}


		// Generate a unique message ID
		messageID := uuid.New().String()

		// Calculate MD5 of message body
		bodyHash := md5.Sum([]byte(message.MessageBody))
		md5OfBody := hex.EncodeToString(bodyHash[:])

		// Calculate MD5 of message attributes
		var md5OfAttributes string
		if len(message.MessageAttributes) > 0 {
			attrHash := md5.Sum(hashAttributes(message.MessageAttributes))
			md5OfAttributes = hex.EncodeToString(attrHash[:])
		}

		// Calculate MD5 of system message attributes
		var md5OfSystemAttributes string
		if len(message.MessageSystemAttributes) > 0 {
			sysAttrHash := md5.Sum(hashSystemAttributes(message.MessageSystemAttributes))
			md5OfSystemAttributes = hex.EncodeToString(sysAttrHash[:])
		}

		// Handle DelaySeconds
		visibleAfter := time.Now().Unix()
		if message.DelaySeconds != nil {
			visibleAfter += int64(*message.DelaySeconds)
		}

		// Store the message
		internalMessage := models.Message{
			ID:                messageID,
			Body:              message.MessageBody,
			Attributes:        message.MessageAttributes,
			SystemAttributes:  message.MessageSystemAttributes,
			MD5OfBody:         md5OfBody,
			MD5OfAttributes:   md5OfAttributes,
			MD5OfSysAttributes: md5OfSystemAttributes,
			VisibleAfter:      visibleAfter,
		}
		msgBytes, err := json.Marshal(internalMessage)
		if err != nil {
			return nil, err
		}
		tr.Set(messagesDir.Pack(tuple.Tuple{messageID}), msgBytes)

		// Store Deduplication ID for FIFO queues
		if isFifo && message.MessageDeduplicationId != nil && *message.MessageDeduplicationId != "" {
			dedupKey := dedupDir.Pack(tuple.Tuple{*message.MessageDeduplicationId})
			// Store the message ID and set an expiration (5 minutes)
			tr.Set(dedupKey, []byte(messageID))
			// FDB doesn't have built-in TTL, so this would need a background process to clean up.
			// For this implementation, we'll just set it.
		}

		// Construct the response
		response := &models.SendMessageResponse{
			MessageId:        messageID,
			MD5OfMessageBody: md5OfBody,
		}
		if md5OfAttributes != "" {
			response.MD5OfMessageAttributes = &md5OfAttributes
		}
		if md5OfSystemAttributes != "" {
			response.MD5OfMessageSystemAttributes = &md5OfSystemAttributes
		}

		// SequenceNumber for FIFO queues
		if isFifo {
			seq, err := s.getNextSequenceNumber(tr, queueDir)
			if err != nil {
				return nil, err
			}
			seqStr := strconv.FormatInt(seq, 10)
			response.SequenceNumber = &seqStr
		}

		return response, nil
	})

	if err != nil {
		return nil, err
	}
	return resp.(*models.SendMessageResponse), nil
}

// getNextSequenceNumber increments and returns a sequence number for a FIFO queue.
func (s *FDBStore) getNextSequenceNumber(tr fdb.Transaction, queueDir directory.DirectorySubspace) (int64, error) {
	key := queueDir.Pack(tuple.Tuple{"sequence_number"})
	// Atomically increment the sequence number.
	tr.Add(key, []byte{0, 0, 0, 0, 0, 0, 0, 1}) // Increment by 1 (as 64-bit little-endian)
	valBytes, err := tr.Get(key).Get()
	if err != nil {
		return 0, err
	}
	// The value is a 64-bit little-endian integer.
	return int64(binary.LittleEndian.Uint64(valBytes)), nil
}

// hashAttributes creates a deterministic byte representation of message attributes for hashing, following the SQS specification.
func hashAttributes(attributes map[string]models.MessageAttributeValue) []byte {
	var keys []string
	for k := range attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		v := attributes[k]

		// Encode Name
		binary.Write(&buf, binary.BigEndian, int32(len(k)))
		buf.WriteString(k)

		// Encode DataType
		binary.Write(&buf, binary.BigEndian, int32(len(v.DataType)))
		buf.WriteString(v.DataType)

		// Encode Value
		if strings.HasPrefix(v.DataType, "String") || strings.HasPrefix(v.DataType, "Number") {
			buf.WriteByte(1) // String transport type
			binary.Write(&buf, binary.BigEndian, int32(len(*v.StringValue)))
			buf.WriteString(*v.StringValue)
		} else if strings.HasPrefix(v.DataType, "Binary") {
			buf.WriteByte(2) // Binary transport type
			binary.Write(&buf, binary.BigEndian, int32(len(v.BinaryValue)))
			buf.Write(v.BinaryValue)
		}
	}
	return buf.Bytes()
}

// hashSystemAttributes creates a deterministic byte representation of message system attributes for hashing.
func hashSystemAttributes(attributes map[string]models.MessageSystemAttributeValue) []byte {
	var keys []string
	for k := range attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		v := attributes[k]

		// Encode Name
		binary.Write(&buf, binary.BigEndian, int32(len(k)))
		buf.WriteString(k)

		// Encode DataType
		binary.Write(&buf, binary.BigEndian, int32(len(v.DataType)))
		buf.WriteString(v.DataType)

		// Encode Value
		if strings.HasPrefix(v.DataType, "String") {
			buf.WriteByte(1) // String transport type
			binary.Write(&buf, binary.BigEndian, int32(len(*v.StringValue)))
			buf.WriteString(*v.StringValue)
		} else if strings.HasPrefix(v.DataType, "Binary") {
			buf.WriteByte(2) // Binary transport type
			binary.Write(&buf, binary.BigEndian, int32(len(v.BinaryValue)))
			buf.Write(v.BinaryValue)
		}
	}
	return buf.Bytes()
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
