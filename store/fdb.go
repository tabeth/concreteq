// Package store provides the FoundationDB implementation of the storage interface.
package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"math/rand"

	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/tabeth/concreteq/models"
)

const (
	// By sharding the queue's visibility index, we can distribute the load of
	// dequeuing operations across multiple prefixes. This significantly reduces
	// transaction conflicts under high contention, as consumers will be reading
	// from different parts of the keyspace. A value of 16 is chosen as a
	// reasonable starting point to provide a good balance of load distribution
	// without creating an excessive number of sub-prefixes.
	numShards = 16
)

// FDBStore is a concrete implementation of the Store interface using FoundationDB.
// It holds a connection to the database and a directory subspace for this application
// to keep its data separate from other applications in the same FDB cluster.
type FDBStore struct {
	db  fdb.Database
	dir directory.DirectorySubspace
}

// GetDB returns the underlying FoundationDB database object, primarily for testing purposes.
func (s *FDBStore) GetDB() fdb.Database {
	return s.db
}

// NewFDBStore creates a new FDBStore with a default root directory path "concreteq".
func NewFDBStore() (*FDBStore, error) {
	return NewFDBStoreAtPath("concreteq")
}

// NewFDBStoreAtPath creates a new FDBStore at a specific directory path within FoundationDB.
// This is particularly useful for testing, as it allows each test run to use a
// unique, isolated data space, preventing interference between parallel tests.
func NewFDBStoreAtPath(path ...string) (*FDBStore, error) {
	// FoundationDB requires specifying the API version to ensure client-server compatibility.
	fdb.MustAPIVersion(730)
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	// The directory layer is a FoundationDB feature that provides a way to manage
	// hierarchical data organization, similar to a file system. We create or open
	// a directory for our application's data.
	dir, err := directory.CreateOrOpen(db, path, nil)
	if err != nil {
		return nil, err
	}

	return &FDBStore{db: db, dir: dir}, nil
}

// CreateQueue creates a new queue in FoundationDB. If the queue already exists,
// it returns the existing attributes to allow the handler to perform idempotency checks.
// The entire operation is performed within a single transaction to ensure atomicity.
//
// Data Model:
// - Each queue is represented by a directory within the main application directory.
//   e.g., `(app_dir, "my-queue")`
// - Attributes and tags are stored as JSON blobs at well-known keys within the queue's directory.
//   e.g., `(app_dir, "my-queue", "attributes") -> "{...}"`
func (s *FDBStore) CreateQueue(ctx context.Context, name string, attributes map[string]string, tags map[string]string) (map[string]string, error) {
	resp, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// First, check if a queue with this name already exists.
		exists, err := s.dir.Exists(tr, []string{name})
		if err != nil {
			return nil, err
		}
		if exists {
			// If it exists, read its attributes and return them for the handler to compare.
			queueDir, err := s.dir.Open(tr, []string{name}, nil)
			if err != nil {
				return nil, err
			}
			attrsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
			if err != nil {
				return nil, err
			}
			if len(attrsBytes) == 0 {
				return make(map[string]string), nil
			}
			var storedAttrs map[string]string
			if err := json.Unmarshal(attrsBytes, &storedAttrs); err != nil {
				return nil, err
			}
			return storedAttrs, nil
		}

		// If it doesn't exist, create a new directory (subspace) for this specific queue.
		queueDir, err := s.dir.Create(tr, []string{name}, nil)
		if err != nil {
			return nil, err
		}

		// Store queue attributes as a single JSON object.
		if len(attributes) > 0 {
			attrsBytes, err := json.Marshal(attributes)
			if err != nil {
				return nil, err
			}
			tr.Set(queueDir.Pack(tuple.Tuple{"attributes"}), attrsBytes)
		}

		// Store tags similarly.
		if len(tags) > 0 {
			tagsBytes, err := json.Marshal(tags)
			if err != nil {
				return nil, err
			}
			tr.Set(queueDir.Pack(tuple.Tuple{"tags"}), tagsBytes)
		}

		// Update the DLQ index.
		if err := s.updateDLQIndex(tr, name, nil, attributes); err != nil {
			return nil, err
		}

		// Return nil to indicate the queue was newly created.
		return nil, nil
	})

	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.(map[string]string), nil
}

// DeleteQueue removes a queue and all its associated data from FoundationDB.
// The directory layer handles the removal of the entire subspace atomically.
func (s *FDBStore) DeleteQueue(ctx context.Context, name string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{name})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrQueueDoesNotExist
		}

		queueDir, err := s.dir.Open(tr, []string{name}, nil)
		if err != nil {
			return nil, err
		}

		// Clean up the DLQ index before deleting the queue.
		var oldAttributes map[string]string
		oldAttrsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
		if err != nil {
			return nil, err
		}
		if len(oldAttrsBytes) > 0 {
			if err := json.Unmarshal(oldAttrsBytes, &oldAttributes); err != nil {
				return nil, err
			}
		}
		if err := s.updateDLQIndex(tr, name, oldAttributes, nil); err != nil {
			return nil, err
		}

		// `dir.Remove` deletes the directory and all its contents (keys and subdirectories).
		_, err = s.dir.Remove(tr, []string{name})
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

// ListQueues retrieves a list of all queue names.
// It reads all subdirectories from the application's root directory.
// Pagination and filtering are handled in-memory after fetching all names.
// For very large numbers of queues, a more scalable approach would be needed.
func (s *FDBStore) ListQueues(ctx context.Context, maxResults int, nextToken, queueNamePrefix string) ([]string, string, error) {
	// A ReadTransact is used as we are only reading data.
	queues, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		return s.dir.List(tr, []string{})
	})
	if err != nil {
		return nil, "", err
	}

	allQueues := queues.([]string)
	var filteredQueues []string

	// Filter by prefix if provided.
	if queueNamePrefix != "" {
		for _, q := range allQueues {
			if strings.HasPrefix(q, queueNamePrefix) {
				filteredQueues = append(filteredQueues, q)
			}
		}
	} else {
		filteredQueues = allQueues
	}

	// Implement pagination based on the nextToken.
	startIndex := 0
	if nextToken != "" {
		found := false
		// The nextToken is the name of the last queue from the previous page.
		// We find it and start the next page from the following item.
		for i, q := range filteredQueues {
			if q == nextToken {
				startIndex = i + 1
				found = true
				break
			}
		}
		if !found {
			// As per SQS behavior, an invalid token returns an empty list.
			return []string{}, "", nil
		}
	}

	if startIndex >= len(filteredQueues) {
		return []string{}, "", nil // No more results.
	}

	// Slice the results for the current page.
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

	// If there are more results, set the nextToken for the next call.
	if maxResults > 0 && endIndex < len(filteredQueues) {
		newNextToken = resultQueues[len(resultQueues)-1]
	}

	return resultQueues, newNextToken, nil
}

// GetQueueAttributes retrieves the attributes for a specified queue from FoundationDB.
func (s *FDBStore) GetQueueAttributes(ctx context.Context, name string) (map[string]string, error) {
	// A ReadTransact is used because we are only reading data. This is more efficient
	// than a full read-write transaction.
	attributes, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		// Check if the queue's directory exists. If not, the queue does not exist.
		exists, err := s.dir.Exists(tr, []string{name})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrQueueDoesNotExist
		}

		// Open the directory for the specified queue.
		queueDir, err := s.dir.Open(tr, []string{name}, nil)
		if err != nil {
			return nil, err
		}

		// The attributes are stored as a JSON blob in the "attributes" key
		// within the queue's directory.
		attrsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
		if err != nil {
			return nil, err
		}

		// If the key doesn't exist or is empty, there are no attributes.
		if len(attrsBytes) == 0 {
			return make(map[string]string), nil
		}

		// Unmarshal the JSON data into a map to be returned.
		var attrs map[string]string
		if err := json.Unmarshal(attrsBytes, &attrs); err != nil {
			// If the data is corrupt, return an error.
			return nil, err
		}
		return attrs, nil
	})

	if err != nil {
		return nil, err
	}
	return attributes.(map[string]string), nil
}

// SetQueueAttributes updates the attributes for a specified queue.
// Note: This implementation overwrites all existing attributes with the provided ones.
// A more advanced implementation might merge them.
func (s *FDBStore) SetQueueAttributes(ctx context.Context, name string, attributes map[string]string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{name})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrQueueDoesNotExist
		}

		queueDir, err := s.dir.Open(tr, []string{name}, nil)
		if err != nil {
			return nil, err
		}

		// Get the old attributes to compare for DLQ index changes.
		var oldAttributes map[string]string
		oldAttrsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
		if err != nil {
			return nil, err
		}
		if len(oldAttrsBytes) > 0 {
			if err := json.Unmarshal(oldAttrsBytes, &oldAttributes); err != nil {
				return nil, err
			}
		}

		// Update the DLQ index before setting the new attributes.
		if err := s.updateDLQIndex(tr, name, oldAttributes, attributes); err != nil {
			return nil, err
		}

		attrsBytes, err := json.Marshal(attributes)
		if err != nil {
			return nil, err
		}
		tr.Set(queueDir.Pack(tuple.Tuple{"attributes"}), attrsBytes)
		return nil, nil
	})
	return err
}

// GetQueueURL is not yet implemented. The URL is constructed in the handler layer.
func (s *FDBStore) GetQueueURL(ctx context.Context, name string) (string, error) {
	// TODO: This logic is currently in the handler; might be better to have a
	//       central configuration for the service's base URL.
	return "", nil
}

// PurgeQueue deletes all messages from a queue without deleting the queue itself.
func (s *FDBStore) PurgeQueue(ctx context.Context, name string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{name})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrQueueDoesNotExist
		}

		queueDir, err := s.dir.Open(tr, []string{name}, nil)
		if err != nil {
			return nil, err
		}

		// Enforce the SQS rule of a 60-second cooldown period between purges.
		now := time.Now().Unix()
		lastPurgedBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"last_purged_at"})).Get()
		if err != nil {
			return nil, err
		}

		if len(lastPurgedBytes) > 0 {
			lastPurged, err := strconv.ParseInt(string(lastPurgedBytes), 10, 64)
			if err == nil {
				if now-lastPurged < 60 {
					return nil, ErrPurgeQueueInProgress
				}
			}
		}

		// Clear all keys within the messages subspace for this queue.
		messagesDir := queueDir.Sub("messages")
		prefix := messagesDir.Pack(tuple.Tuple{})
		pr, err := fdb.PrefixRange(prefix)
		if err != nil {
			return nil, err
		}
		tr.ClearRange(pr)

		// Record the time of this purge to enforce the cooldown.
		tr.Set(queueDir.Pack(tuple.Tuple{"last_purged_at"}), []byte(strconv.FormatInt(now, 10)))

		return nil, nil
	})
	return err
}

// SendMessage adds a message to a queue. This is a complex transaction that handles
// both Standard and FIFO queue logic.
//
// Data Model for Messages:
// - Main message data: `(queue, "messages", messageId) -> json_blob_of_message`
// - Standard queue index: `(queue, "visible_idx", visible_after_ts, messageId) -> ""`
// - FIFO queue index: `(queue, "fifo_idx", groupId, sequenceNumber) -> messageId`
// - FIFO deduplication: `(queue, "dedup", deduplicationId) -> json_blob_of_response`
func (s *FDBStore) SendMessage(ctx context.Context, queueName string, message *models.SendMessageRequest) (*models.SendMessageResponse, error) {
	resp, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Find the queue's directory.
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
		isFifo := strings.HasSuffix(queueName, ".fifo")

		if isFifo {
			// Check if ContentBasedDeduplication is enabled
			attrsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
			if err != nil {
				return nil, err
			}
			var contentBasedDedupEnabled bool
			if len(attrsBytes) > 0 {
				var attrs map[string]string
				if err := json.Unmarshal(attrsBytes, &attrs); err == nil {
					if attrs["ContentBasedDeduplication"] == "true" {
						contentBasedDedupEnabled = true
					}
				}
			}

			// Auto-generate Deduplication ID if enabled and missing
			if contentBasedDedupEnabled && (message.MessageDeduplicationId == nil || *message.MessageDeduplicationId == "") {
				hash := sha256.Sum256([]byte(message.MessageBody))
				dedupId := hex.EncodeToString(hash[:])
				message.MessageDeduplicationId = &dedupId
			}
		}

		// 2. FIFO queues: Handle content-based deduplication. If a message with the
		//    same deduplication ID was sent recently, return the previous response.
		if isFifo && message.MessageDeduplicationId != nil && *message.MessageDeduplicationId != "" {
			dedupDir := queueDir.Sub("dedup")
			dedupKey := dedupDir.Pack(tuple.Tuple{*message.MessageDeduplicationId})
			val, err := tr.Get(dedupKey).Get()
			if err != nil {
				return nil, err
			}
			if val != nil {
				var storedResp models.SendMessageResponse
				if err := json.Unmarshal(val, &storedResp); err == nil {
					return &storedResp, nil
				}
			}
		}

		// 3. Generate a unique ID and calculate MD5 hashes for the message body and attributes.
		messageID := uuid.New().String()
		bodyHash := md5.Sum([]byte(message.MessageBody))
		md5OfBody := hex.EncodeToString(bodyHash[:])
		var md5OfAttributes string
		if len(message.MessageAttributes) > 0 {
			attrHash := md5.Sum(hashAttributes(message.MessageAttributes, nil))
			md5OfAttributes = hex.EncodeToString(attrHash[:])
		}

		// 4. Construct the internal message object to be stored.
		sentTimestamp := time.Now().Unix()
		internalMessage := models.Message{
			ID:                messageID,
			Body:              message.MessageBody,
			Attributes:        message.MessageAttributes,
			MD5OfBody:         md5OfBody,
			MD5OfAttributes:   md5OfAttributes,
			SentTimestamp:     sentTimestamp,
			SenderId:          "123456789012", // Placeholder SenderId
		}

		// 5. Write to the appropriate index based on queue type.
		if isFifo {
			// For FIFO queues, we need a globally ordered sequence number.
			fifoIdxDir := queueDir.Sub("fifo_idx")
			seq, err := s.getNextSequenceNumber(tr, queueDir)
			if err != nil {
				return nil, err
			}
			internalMessage.SequenceNumber = seq
			internalMessage.MessageGroupId = *message.MessageGroupId

			// The FIFO index is ordered by (group ID, sequence number).
			fifoKey := fifoIdxDir.Pack(tuple.Tuple{*message.MessageGroupId, seq})
			tr.Set(fifoKey, []byte(messageID))

		} else {
			// For Standard queues, we use a visibility timeout index.
			visibleIdxDir := queueDir.Sub("visible_idx")
			visibleAfter := sentTimestamp
			if message.DelaySeconds != nil {
				visibleAfter += int64(*message.DelaySeconds)
			}
			internalMessage.VisibleAfter = visibleAfter

			// The index is ordered by (shard, visibility_time, messageId).
			// Sharding distributes the dequeue load across multiple prefixes,
			// preventing hot spots on the head of the queue.
			shardID := rand.Intn(numShards)
			visKey := visibleIdxDir.Pack(tuple.Tuple{shardID, visibleAfter, messageID})
			tr.Set(visKey, []byte{})
		}

		// 6. Write the full message data to the messages subspace.
		msgBytes, err := json.Marshal(internalMessage)
		if err != nil {
			return nil, err
		}
		tr.Set(messagesDir.Pack(tuple.Tuple{messageID}), msgBytes)

		// 7. Construct the API response.
		response := &models.SendMessageResponse{
			MessageId:        messageID,
			MD5OfMessageBody: md5OfBody,
		}
		if md5OfAttributes != "" {
			response.MD5OfMessageAttributes = &md5OfAttributes
		}
		if isFifo {
			seqStr := strconv.FormatInt(internalMessage.SequenceNumber, 10)
			response.SequenceNumber = &seqStr
		}

		// 8. For FIFO queues, store the response for future deduplication lookups.
		if isFifo && message.MessageDeduplicationId != nil && *message.MessageDeduplicationId != "" {
			dedupDir := queueDir.Sub("dedup")
			dedupKey := dedupDir.Pack(tuple.Tuple{*message.MessageDeduplicationId})
			respBytes, err := json.Marshal(response)
			if err == nil {
				tr.Set(dedupKey, respBytes)
				// A real-world implementation would need a background process to
				// clean up expired deduplication entries.
			}
		}

		return response, nil
	})

	if err != nil {
		return nil, err
	}
	return resp.(*models.SendMessageResponse), nil
}

// buildResponseAttributes assembles the map of system attributes for a message being returned to a client.
// It includes only the attributes requested by the client.
func (s *FDBStore) buildResponseAttributes(msg *models.Message, req *models.ReceiveMessageRequest) map[string]string {
	attrs := make(map[string]string)
	requestedAttrs := make(map[string]bool)

	// SQS allows attributes to be requested via two different (but overlapping) parameters.
	allRequested := append(req.AttributeNames, req.MessageSystemAttributeNames...)

	for _, attrName := range allRequested {
		requestedAttrs[attrName] = true
	}

	wantsAll := requestedAttrs["All"]

	if wantsAll || requestedAttrs["ApproximateReceiveCount"] {
		attrs["ApproximateReceiveCount"] = strconv.Itoa(msg.ReceivedCount)
	}
	if wantsAll || requestedAttrs["ApproximateFirstReceiveTimestamp"] {
		// SQS returns timestamps as epoch milliseconds.
		attrs["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(msg.FirstReceived*1000, 10)
	}
	if wantsAll || requestedAttrs["SentTimestamp"] {
		attrs["SentTimestamp"] = strconv.FormatInt(msg.SentTimestamp*1000, 10)
	}
	if wantsAll || requestedAttrs["SenderId"] {
		attrs["SenderId"] = msg.SenderId
	}
	if msg.MessageGroupId != "" && (wantsAll || requestedAttrs["MessageGroupId"]) {
		attrs["MessageGroupId"] = msg.MessageGroupId
	}
	if msg.SequenceNumber != 0 && (wantsAll || requestedAttrs["SequenceNumber"]) {
		attrs["SequenceNumber"] = strconv.FormatInt(msg.SequenceNumber, 10)
	}

	return attrs
}

// buildResponseMessageAttributes assembles the map of user-defined attributes for a message.
func (s *FDBStore) buildResponseMessageAttributes(msg *models.Message, req *models.ReceiveMessageRequest) map[string]models.MessageAttributeValue {
	if len(msg.Attributes) == 0 {
		return nil
	}
	// If no specific attributes are requested, SQS returns nothing.
	if len(req.MessageAttributeNames) == 0 {
		return nil
	}

	returnedAttrs := make(map[string]models.MessageAttributeValue)

	for _, reqName := range req.MessageAttributeNames {
		if reqName == "All" || reqName == ".*" {
			// If "All" or ".*" is requested, return all attributes.
			return msg.Attributes
		}

		if strings.HasSuffix(reqName, ".*") {
			// Handle prefix wildcard
			prefix := strings.TrimSuffix(reqName, ".*")
			for attrName, attrValue := range msg.Attributes {
				if strings.HasPrefix(attrName, prefix) {
					returnedAttrs[attrName] = attrValue
				}
			}
		} else {
			// Handle exact match
			if attrValue, ok := msg.Attributes[reqName]; ok {
				returnedAttrs[reqName] = attrValue
			}
		}
	}

	if len(returnedAttrs) == 0 {
		return nil
	}
	return returnedAttrs
}

// getNextSequenceNumber atomically increments and returns a sequence number for a FIFO queue.
// This provides the strict ordering required for FIFO operations.
func (s *FDBStore) getNextSequenceNumber(tr fdb.Transaction, queueDir directory.DirectorySubspace) (int64, error) {
	key := queueDir.Pack(tuple.Tuple{"sequence_number"})
	// `Add` is an atomic operation in FoundationDB. We add 1 to a 64-bit little-endian integer.
	tr.Add(key, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	valBytes, err := tr.Get(key).Get()
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(valBytes)), nil
}

// updateDLQIndex maintains the reverse index for dead-letter queue sources.
// It should be called within a transaction whenever a queue's RedrivePolicy might change.
func (s *FDBStore) updateDLQIndex(tr fdb.Transaction, sourceQueueName string, oldAttributes, newAttributes map[string]string) error {
	// Helper to parse the DLQ name from a RedrivePolicy string.
	getDLQName := func(policyStr string) string {
		if policyStr == "" {
			return ""
		}
		var policy struct {
			DeadLetterTargetArn string `json:"deadLetterTargetArn"`
		}
		if json.Unmarshal([]byte(policyStr), &policy) != nil {
			return ""
		}
		parts := strings.Split(policy.DeadLetterTargetArn, ":")
		if len(parts) > 0 {
			return parts[len(parts)-1]
		}
		return ""
	}

	oldPolicy, _ := oldAttributes["RedrivePolicy"]
	newPolicy, _ := newAttributes["RedrivePolicy"]

	oldDLQName := getDLQName(oldPolicy)
	newDLQName := getDLQName(newPolicy)

	// If the DLQ target hasn't changed, there's nothing to do.
	if oldDLQName == newDLQName {
		return nil
	}

	dlqIndexDir, err := s.dir.CreateOrOpen(tr, []string{"dlq_sources"}, nil)
	if err != nil {
		return err
	}

	// If there was an old DLQ, remove the old index entry.
	if oldDLQName != "" {
		tr.Clear(dlqIndexDir.Pack(tuple.Tuple{oldDLQName, sourceQueueName}))
	}

	// If there is a new DLQ, add the new index entry.
	if newDLQName != "" {
		tr.Set(dlqIndexDir.Pack(tuple.Tuple{newDLQName, sourceQueueName}), []byte{})
	}

	return nil
}

// hashAttributes creates a deterministic byte representation of message attributes for hashing.
// This is required by the SQS specification for calculating the MD5OfMessageAttributes.
// The attributes must be sorted by name and encoded in a specific binary format.
func hashAttributes(attributes map[string]models.MessageAttributeValue, keysToHash []string) []byte {
	var keys []string
	if keysToHash != nil {
		keys = keysToHash
	} else {
		for k := range attributes {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		v := attributes[k]

		// Encode Name (length-prefixed)
		binary.Write(&buf, binary.BigEndian, int32(len(k)))
		buf.WriteString(k)

		// Encode DataType (length-prefixed)
		binary.Write(&buf, binary.BigEndian, int32(len(v.DataType)))
		buf.WriteString(v.DataType)

		// Encode Value (transport type marker + length-prefixed value)
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

// hashSystemAttributes does the same as hashAttributes but for system-level attributes.
func hashSystemAttributes(attributes map[string]models.MessageSystemAttributeValue) []byte {
	var keys []string
	for k := range attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		v := attributes[k]

		binary.Write(&buf, binary.BigEndian, int32(len(k)))
		buf.WriteString(k)
		binary.Write(&buf, binary.BigEndian, int32(len(v.DataType)))
		buf.WriteString(v.DataType)

		if strings.HasPrefix(v.DataType, "String") {
			buf.WriteByte(1)
			binary.Write(&buf, binary.BigEndian, int32(len(*v.StringValue)))
			buf.WriteString(*v.StringValue)
		} else if strings.HasPrefix(v.DataType, "Binary") {
			buf.WriteByte(2)
			binary.Write(&buf, binary.BigEndian, int32(len(v.BinaryValue)))
			buf.Write(v.BinaryValue)
		}
	}
	return buf.Bytes()
}

// SendMessageBatch handles sending multiple messages in a single transaction.
// It iterates through the entries, performs validation, and attempts to send each one.
// It returns a response indicating which messages succeeded and which failed.
func (s *FDBStore) SendMessageBatch(ctx context.Context, queueName string, req *models.SendMessageBatchRequest) (*models.SendMessageBatchResponse, error) {
	resp, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
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
		isFifo := strings.HasSuffix(queueName, ".fifo")

		var contentBasedDedupEnabled bool
		if isFifo {
			attrsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
			if err != nil {
				return nil, err
			}
			if len(attrsBytes) > 0 {
				var attrs map[string]string
				if err := json.Unmarshal(attrsBytes, &attrs); err == nil {
					if attrs["ContentBasedDeduplication"] == "true" {
						contentBasedDedupEnabled = true
					}
				}
			}
		}

		successful := []models.SendMessageBatchResultEntry{}
		failed := []models.BatchResultErrorEntry{}

		for i := range req.Entries {
			entry := &req.Entries[i]

			// Perform per-entry validation within the transaction. If a message fails
			// validation, it's added to the `failed` list and we continue to the next.
			if entry.DelaySeconds != nil {
				if *entry.DelaySeconds < 0 || *entry.DelaySeconds > 900 {
					failed = append(failed, models.BatchResultErrorEntry{Id: entry.Id, Code: "InvalidParameterValue", Message: "DelaySeconds must be between 0 and 900.", SenderFault: true})
					continue
				}
				if isFifo {
					failed = append(failed, models.BatchResultErrorEntry{Id: entry.Id, Code: "InvalidParameterValue", Message: "DelaySeconds is not supported for FIFO queues.", SenderFault: true})
					continue
				}
			}
			if isFifo && entry.MessageGroupId == nil {
				failed = append(failed, models.BatchResultErrorEntry{Id: entry.Id, Code: "MissingParameter", Message: "MessageGroupId is required for FIFO queues.", SenderFault: true})
				continue
			}

			// Auto-generate Deduplication ID if enabled and missing
			if isFifo && contentBasedDedupEnabled && (entry.MessageDeduplicationId == nil || *entry.MessageDeduplicationId == "") {
				hash := sha256.Sum256([]byte(entry.MessageBody))
				dedupId := hex.EncodeToString(hash[:])
				entry.MessageDeduplicationId = &dedupId
			}

			// The logic for each message is very similar to the single SendMessage handler.
			if isFifo && entry.MessageDeduplicationId != nil && *entry.MessageDeduplicationId != "" {
				dedupDir := queueDir.Sub("dedup")
				dedupKey := dedupDir.Pack(tuple.Tuple{*entry.MessageDeduplicationId})
				val, err := tr.Get(dedupKey).Get()
				if err != nil {
					failed = append(failed, models.BatchResultErrorEntry{Id: entry.Id, Code: "InternalError", Message: err.Error(), SenderFault: false})
					continue
				}
				if val != nil {
					var storedResp models.SendMessageBatchResultEntry
					if err := json.Unmarshal(val, &storedResp); err == nil {
						successful = append(successful, storedResp)
						continue
					}
				}
			}

			// Create and store the message.
			messageID := uuid.New().String()
			bodyHash := md5.Sum([]byte(entry.MessageBody))
			md5OfBody := hex.EncodeToString(bodyHash[:])
			var md5OfAttributes string
			if len(entry.MessageAttributes) > 0 {
				attrHash := md5.Sum(hashAttributes(entry.MessageAttributes, nil))
				md5OfAttributes = hex.EncodeToString(attrHash[:])
			}
			sentTimestamp := time.Now().Unix()
			internalMessage := models.Message{
				ID:              messageID,
				Body:            entry.MessageBody,
				Attributes:      entry.MessageAttributes,
				MD5OfBody:       md5OfBody,
				MD5OfAttributes: md5OfAttributes,
				SentTimestamp:   sentTimestamp,
				SenderId:        "123456789012", // Placeholder
			}

			if isFifo {
				fifoIdxDir := queueDir.Sub("fifo_idx")
				seq, err := s.getNextSequenceNumber(tr, queueDir)
				if err != nil {
					failed = append(failed, models.BatchResultErrorEntry{Id: entry.Id, Code: "InternalError", Message: "Failed to generate sequence number.", SenderFault: false})
					continue
				}
				internalMessage.SequenceNumber = seq
				internalMessage.MessageGroupId = *entry.MessageGroupId
				fifoKey := fifoIdxDir.Pack(tuple.Tuple{*entry.MessageGroupId, seq})
				tr.Set(fifoKey, []byte(messageID))
			} else {
				visibleIdxDir := queueDir.Sub("visible_idx")
				visibleAfter := sentTimestamp
				if entry.DelaySeconds != nil {
					visibleAfter += int64(*entry.DelaySeconds)
				}
				internalMessage.VisibleAfter = visibleAfter
				// Use the same sharding logic as the single send.
				shardID := rand.Intn(numShards)
				visKey := visibleIdxDir.Pack(tuple.Tuple{shardID, visibleAfter, messageID})
				tr.Set(visKey, []byte{})
			}

			msgBytes, err := json.Marshal(internalMessage)
			if err != nil {
				failed = append(failed, models.BatchResultErrorEntry{Id: entry.Id, Code: "InternalError", Message: "Failed to marshal message.", SenderFault: false})
				continue
			}
			tr.Set(messagesDir.Pack(tuple.Tuple{messageID}), msgBytes)

			// Construct the successful result entry.
			resultEntry := models.SendMessageBatchResultEntry{
				Id:               entry.Id,
				MessageId:        messageID,
				MD5OfMessageBody: md5OfBody,
			}
			if md5OfAttributes != "" {
				resultEntry.MD5OfMessageAttributes = &md5OfAttributes
			}
			if isFifo {
				seqStr := strconv.FormatInt(internalMessage.SequenceNumber, 10)
				resultEntry.SequenceNumber = &seqStr
			}
			successful = append(successful, resultEntry)

			// Store result for FIFO deduplication.
			if isFifo && entry.MessageDeduplicationId != nil && *entry.MessageDeduplicationId != "" {
				dedupDir := queueDir.Sub("dedup")
				dedupKey := dedupDir.Pack(tuple.Tuple{*entry.MessageDeduplicationId})
				respBytes, err := json.Marshal(resultEntry)
				if err == nil {
					tr.Set(dedupKey, respBytes)
				}
			}
		}

		return &models.SendMessageBatchResponse{Successful: successful, Failed: failed}, nil
	})

	if err != nil {
		// If the entire transaction fails (e.g., due to a conflict), the handler
		// layer is responsible for converting this into a batch failure response.
		return nil, err
	}

	return resp.(*models.SendMessageBatchResponse), nil
}

// receiveStandardMessages contains the logic for retrieving messages from a Standard SQS queue.
// It iterates through randomized shards of the visibility index to find available messages.
// This approach distributes the read load and significantly reduces transaction conflicts
// under high contention compared to scanning the head of the queue every time.
func (s *FDBStore) receiveStandardMessages(tr fdb.Transaction, queueDir directory.DirectorySubspace, req *models.ReceiveMessageRequest, maxMessages int, visibilityTimeout int, queueAttributes map[string]string) ([]models.ResponseMessage, error) {
	messagesDir := queueDir.Sub("messages")
	visibleIdxDir := queueDir.Sub("visible_idx")
	inflightDir := queueDir.Sub("inflight")

	// Parse RedrivePolicy once
	var dlqName string
	var maxReceiveCount int
	var hasRedrivePolicy bool

	if policyStr, ok := queueAttributes["RedrivePolicy"]; ok {
		var policy struct {
			DeadLetterTargetArn string `json:"deadLetterTargetArn"`
			MaxReceiveCount     string `json:"maxReceiveCount"`
		}
		if json.Unmarshal([]byte(policyStr), &policy) == nil {
			if count, err := strconv.Atoi(policy.MaxReceiveCount); err == nil {
				maxReceiveCount = count
				parts := strings.Split(policy.DeadLetterTargetArn, ":")
				if len(parts) > 0 {
					dlqName = parts[len(parts)-1]
					hasRedrivePolicy = true
				}
			}
		}
	}

	var receivedMessages []models.ResponseMessage
	now := time.Now().Unix()

	// Start scanning from a random shard to ensure consumers are distributed
	// across the keyspace and not all hitting shard 0 simultaneously.
	startShard := rand.Intn(numShards)

	// Iterate through all shards, wrapping around, until we've collected
	// enough messages or checked all shards.
	for i := 0; i < numShards; i++ {
		shardID := (startShard + i) % numShards

		// Query the visibility index for messages in the current shard that are visible.
		// The key is `(shardID, visible_after_ts, messageId)`.
		beginKey := visibleIdxDir.Pack(tuple.Tuple{shardID, 0})
		endKey := visibleIdxDir.Pack(tuple.Tuple{shardID, now + 1})
		keyRange := tr.GetRange(fdb.KeyRange{Begin: beginKey, End: endKey}, fdb.RangeOptions{Limit: maxMessages - len(receivedMessages)})

		iter := keyRange.Iterator()
		for iter.Advance() {
			kv := iter.MustGet()

			t, err := visibleIdxDir.Unpack(kv.Key)
			if err != nil {
				continue // Should not happen with well-formed keys.
			}
			// t[0] is shardID, t[1] is visibleAfter, t[2] is messageID
			messageID := t[2].(string)

			msgBytes, err := tr.Get(messagesDir.Pack(tuple.Tuple{messageID})).Get()
			if err != nil || msgBytes == nil {
				// Message might have been deleted or claimed by another consumer
				// that committed first. This is an expected outcome of the race.
				continue
			}
			var msg models.Message
			if err := json.Unmarshal(msgBytes, &msg); err != nil {
				continue
			}

			// The message is now "in-flight".
			// 1. Delete the old visibility index entry.
			tr.Clear(kv.Key)

			// 2. Update the message's internal state.
			msg.ReceivedCount++
			if msg.FirstReceived == 0 {
				msg.FirstReceived = now
			}

			// Check RedrivePolicy for DLQ
			if hasRedrivePolicy && msg.ReceivedCount > maxReceiveCount {
				// Move to DLQ
				dlqExists, err := s.dir.Exists(tr, []string{dlqName})
				if err == nil && dlqExists {
					dlqDir, err := s.dir.Open(tr, []string{dlqName}, nil)
					if err == nil {
						// Delete from source queue
						tr.Clear(messagesDir.Pack(tuple.Tuple{messageID}))

						// Add to DLQ
						dlqMessagesDir := dlqDir.Sub("messages")
						dlqVisibleIdxDir := dlqDir.Sub("visible_idx")

						// Reset visibility for DLQ (visible immediately)
						msg.VisibleAfter = now

						dlqMsgBytes, _ := json.Marshal(msg)
						tr.Set(dlqMessagesDir.Pack(tuple.Tuple{messageID}), dlqMsgBytes)

						shardID := rand.Intn(numShards)
						visKey := dlqVisibleIdxDir.Pack(tuple.Tuple{shardID, now, messageID})
						tr.Set(visKey, []byte{})

						continue
					}
				}
			}

			newVisibilityTimeout := now + int64(visibilityTimeout)
			msg.VisibleAfter = newVisibilityTimeout

			// 3. Write the updated message back to the main message store.
			newMsgBytes, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			tr.Set(messagesDir.Pack(tuple.Tuple{messageID}), newMsgBytes)

			// 4. Create a *new* index entry for when the message should become visible
			//    again if it's not deleted. This entry is placed in a *new random shard*
			//    to prevent a thundering herd problem on a single shard if many
			//    messages time out simultaneously.
			newShardID := rand.Intn(numShards)
			newVisKey := visibleIdxDir.Pack(tuple.Tuple{newShardID, newVisibilityTimeout, messageID})
			tr.Set(newVisKey, []byte{})

			// 5. Generate a unique receipt handle for this specific receive operation.
			//    Store a record of it so we can find the message later for deletion.
			receiptHandle := uuid.New().String()
			// The receipt handle needs to know which shard the *new* visibility key
			// is in so it can be deleted if ChangeMessageVisibility is called.
			// For simplicity in DeleteMessage, we just store the old key.
			receiptData := map[string]interface{}{
				"id":      msg.ID,
				"vis_key": kv.Key, // Store the *original* key.
			}
			receiptBytes, _ := json.Marshal(receiptData)
			tr.Set(inflightDir.Pack(tuple.Tuple{receiptHandle}), receiptBytes)

			// 6. Construct the message object to be returned to the client.
			responseMsg := models.ResponseMessage{
				MessageId:     msg.ID,
				ReceiptHandle: receiptHandle,
				Body:          msg.Body,
				MD5OfBody:     msg.MD5OfBody,
			}
			responseMsg.Attributes = s.buildResponseAttributes(&msg, req)
			responseMsg.MessageAttributes = s.buildResponseMessageAttributes(&msg, req)
			if len(responseMsg.MessageAttributes) > 0 {
				var attrKeys []string
				for k := range responseMsg.MessageAttributes {
					attrKeys = append(attrKeys, k)
				}
				md5Bytes := md5.Sum(hashAttributes(responseMsg.MessageAttributes, attrKeys))
				md5Str := hex.EncodeToString(md5Bytes[:])
				responseMsg.MD5OfMessageAttributes = &md5Str
			}
			receivedMessages = append(receivedMessages, responseMsg)

			// If we've hit our max, we're done.
			if len(receivedMessages) >= maxMessages {
				break
			}
		}

		// If we've hit our max, we're done.
		if len(receivedMessages) >= maxMessages {
			break
		}
	}

	return receivedMessages, nil
}

// receiveFifoMessages contains the more complex logic for retrieving messages from a FIFO queue.
func (s *FDBStore) receiveFifoMessages(tr fdb.Transaction, queueDir directory.DirectorySubspace, req *models.ReceiveMessageRequest, maxMessages int, visibilityTimeout int, queueAttributes map[string]string) ([]models.ResponseMessage, error) {
	messagesDir := queueDir.Sub("messages")
	fifoIdxDir := queueDir.Sub("fifo_idx")
	inflightGroupsDir := queueDir.Sub("inflight_groups")
	receiveAttemptsDir := queueDir.Sub("receive_attempts")

	// Parse RedrivePolicy once
	var dlqName string
	var maxReceiveCount int
	var hasRedrivePolicy bool

	if policyStr, ok := queueAttributes["RedrivePolicy"]; ok {
		var policy struct {
			DeadLetterTargetArn string `json:"deadLetterTargetArn"`
			MaxReceiveCount     string `json:"maxReceiveCount"`
		}
		if json.Unmarshal([]byte(policyStr), &policy) == nil {
			if count, err := strconv.Atoi(policy.MaxReceiveCount); err == nil {
				maxReceiveCount = count
				parts := strings.Split(policy.DeadLetterTargetArn, ":")
				if len(parts) > 0 {
					dlqName = parts[len(parts)-1]
					hasRedrivePolicy = true
				}
			}
		}
	}

	now := time.Now().Unix()

	// 1. FIFO Receive Deduplication: If the same attempt ID is used within 5 minutes,
	//    return the same result as the original request.
	if req.ReceiveRequestAttemptId != "" {
		attemptKey := receiveAttemptsDir.Pack(tuple.Tuple{req.ReceiveRequestAttemptId})
		val, err := tr.Get(attemptKey).Get()
		if err != nil {
			return nil, err
		}
		if val != nil {
			var storedAttempt struct {
				Messages  []models.ResponseMessage `json:"messages"`
				Timestamp int64                    `json:"timestamp"`
			}
			if err := json.Unmarshal(val, &storedAttempt); err == nil {
				if now-storedAttempt.Timestamp < 300 { // 5-minute window
					return storedAttempt.Messages, nil
				}
			}
		}
	}

	var receivedMessages []models.ResponseMessage
	var targetGroupID string

	// 2. Find an available message group. SQS FIFO guarantees that messages from one
	//    group are processed in order and not concurrently. We achieve this by "locking"
	//    a message group when messages are received from it.
	findAvailableGroup := func(startKey, endKey fdb.KeyConvertible) (string, error) {
		iter := tr.GetRange(fdb.KeyRange{Begin: startKey, End: endKey}, fdb.RangeOptions{}).Iterator()
		processedGroups := make(map[string]bool)
		for iter.Advance() {
			kv := iter.MustGet()
			t, err := fifoIdxDir.Unpack(kv.Key)
			if err != nil {
				continue
			}
			groupID := t[0].(string)
			if processedGroups[groupID] {
				continue
			} // Already checked this group.
			processedGroups[groupID] = true

			// Check if the group is locked (has in-flight messages).
			lockKey := inflightGroupsDir.Pack(tuple.Tuple{groupID})
			lockVal, err := tr.Get(lockKey).Get()
			if err != nil {
				return "", err
			}

			if lockVal != nil {
				lockExpiry, err := strconv.ParseInt(string(lockVal), 10, 64)
				if err == nil && now < lockExpiry {
					continue // Group is locked.
				}
			}
			return groupID, nil // Found an available group.
		}
		return "", nil // No available group found.
	}

	// To ensure fairness, we scan starting from the group after the one we served last.
	lastGroupKey := queueDir.Pack(tuple.Tuple{"last_group_id"})
	lastGroupBytes, err := tr.Get(lastGroupKey).Get()
	if err != nil {
		return nil, err
	}

	prefixRange, _ := fdb.PrefixRange(fifoIdxDir.Pack(tuple.Tuple{}))
	scanStartKey := prefixRange.Begin
	if len(lastGroupBytes) > 0 {
		// Start scan after the last served group ID. Appending a null byte ensures
		// we start at the very next possible key in lexicographical order.
		scanStartKey = fifoIdxDir.Pack(tuple.Tuple{string(lastGroupBytes) + "\x00"})
	}

	// First scan: from last served group to the end of the index.
	targetGroupID, err = findAvailableGroup(scanStartKey, prefixRange.End)
	if err != nil {
		return nil, err
	}

	// If not found, wrap around and scan from the beginning to the last served group.
	if targetGroupID == "" {
		targetGroupID, err = findAvailableGroup(prefixRange.Begin, scanStartKey)
		if err != nil {
			return nil, err
		}
	}

	// 3. If no available group was found, return an empty list of messages.
	if targetGroupID == "" {
		return []models.ResponseMessage{}, nil
	}

	// 4. Retrieve up to `maxMessages` from the chosen group. Because the index is
	//    ordered by sequence number, this will get the oldest available messages.
	prefixTuple := tuple.Tuple{targetGroupID}
	pr, err := fdb.PrefixRange(fifoIdxDir.Pack(prefixTuple))
	if err != nil {
		return nil, err
	}
	r := tr.GetRange(pr, fdb.RangeOptions{Limit: maxMessages})

	iter := r.Iterator()
	for iter.Advance() {
		kv := iter.MustGet()
		messageID := string(kv.Value)

		msgBytes, err := tr.Get(messagesDir.Pack(tuple.Tuple{messageID})).Get()
		if err != nil || msgBytes == nil {
			continue
		}
		var msg models.Message
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			continue
		}

		// Update Receive Count
		msg.ReceivedCount++
		if msg.FirstReceived == 0 {
			msg.FirstReceived = now
		}

		// Check RedrivePolicy for DLQ
		if hasRedrivePolicy && msg.ReceivedCount > maxReceiveCount {
			// Move to DLQ
			dlqExists, err := s.dir.Exists(tr, []string{dlqName})
			if err == nil && dlqExists {
				dlqDir, err := s.dir.Open(tr, []string{dlqName}, nil)
				if err == nil {
					// Delete from source queue
					tr.Clear(messagesDir.Pack(tuple.Tuple{messageID}))
					tr.Clear(kv.Key) // Delete from fifo_idx

					// Add to DLQ
					dlqMessagesDir := dlqDir.Sub("messages")
					dlqFifoIdxDir := dlqDir.Sub("fifo_idx")

					// Get new sequence number for DLQ
					seq, err := s.getNextSequenceNumber(tr, dlqDir)
					if err == nil {
						msg.SequenceNumber = seq
						// Keep MessageGroupId.

						dlqMsgBytes, _ := json.Marshal(msg)
						tr.Set(dlqMessagesDir.Pack(tuple.Tuple{messageID}), dlqMsgBytes)

						fifoKey := dlqFifoIdxDir.Pack(tuple.Tuple{msg.MessageGroupId, seq})
						tr.Set(fifoKey, []byte(messageID))

						continue
					}
				}
			}
		}

		// Save updated message (persist receive count)
		newMsgBytes, _ := json.Marshal(msg)
		tr.Set(messagesDir.Pack(tuple.Tuple{messageID}), newMsgBytes)

		// Create a receipt handle. For FIFO, we must store the index key so we can
		// delete it later, which is different from Standard queues.
		receiptHandle := uuid.New().String()
		receiptData := map[string]interface{}{
			"id":       msg.ID,
			"fifo_key": kv.Key,
		}
		receiptBytes, _ := json.Marshal(receiptData)
		tr.Set(queueDir.Sub("inflight").Pack(tuple.Tuple{receiptHandle}), receiptBytes)

		// Construct the response message.
		responseMsg := models.ResponseMessage{
			MessageId:     msg.ID,
			ReceiptHandle: receiptHandle,
			Body:          msg.Body,
			MD5OfBody:     msg.MD5OfBody,
		}
		responseMsg.Attributes = s.buildResponseAttributes(&msg, req)
		responseMsg.MessageAttributes = s.buildResponseMessageAttributes(&msg, req)
		if len(responseMsg.MessageAttributes) > 0 {
			var attrKeys []string
			for k := range responseMsg.MessageAttributes {
				attrKeys = append(attrKeys, k)
			}
			md5Bytes := md5.Sum(hashAttributes(responseMsg.MessageAttributes, attrKeys))
			md5Str := hex.EncodeToString(md5Bytes[:])
			responseMsg.MD5OfMessageAttributes = &md5Str
		}
		receivedMessages = append(receivedMessages, responseMsg)
	}

	// 5. If we successfully retrieved messages, lock the group and store the
	//    result for receive-deduplication.
	if len(receivedMessages) > 0 {
		// Lock the group for the duration of the visibility timeout.
		lockExpiry := now + int64(visibilityTimeout)
		tr.Set(inflightGroupsDir.Pack(tuple.Tuple{targetGroupID}), []byte(strconv.FormatInt(lockExpiry, 10)))

		// Update the last group served for fairness.
		tr.Set(lastGroupKey, []byte(targetGroupID))

		// Store the result for deduplication if an attempt ID was provided.
		if req.ReceiveRequestAttemptId != "" {
			attemptKey := receiveAttemptsDir.Pack(tuple.Tuple{req.ReceiveRequestAttemptId})
			attemptData := struct {
				Messages  []models.ResponseMessage `json:"messages"`
				Timestamp int64                    `json:"timestamp"`
			}{Messages: receivedMessages, Timestamp: now}
			attemptBytes, _ := json.Marshal(attemptData)
			tr.Set(attemptKey, attemptBytes)
		}
	}

	return receivedMessages, nil
}

// ReceiveMessage orchestrates the message retrieval process, including long polling.
func (s *FDBStore) ReceiveMessage(ctx context.Context, queueName string, req *models.ReceiveMessageRequest) (*models.ReceiveMessageResponse, error) {
	maxMessages := 1
	if req.MaxNumberOfMessages > 0 {
		maxMessages = req.MaxNumberOfMessages
	}
	waitTime := 0
	if req.WaitTimeSeconds > 0 {
		waitTime = req.WaitTimeSeconds
	}

	isFifo := strings.HasSuffix(queueName, ".fifo")
	startTime := time.Now()

	// This loop implements long polling. It will continuously try to fetch messages
	// until messages are found or the `WaitTimeSeconds` timeout is reached.
	for {
		rawMessages, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
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

			// Determine the visibility timeout. Use the value from the request if provided,
			// otherwise fall back to the queue's default, then the SQS default (30s).
			var queueAttributes map[string]string
			attrsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
			if err != nil {
				return nil, err
			}
			if len(attrsBytes) > 0 {
				json.Unmarshal(attrsBytes, &queueAttributes)
			}

			visibilityTimeout := 30 // SQS default
			if vtStr, ok := queueAttributes["VisibilityTimeout"]; ok {
				if vt, err := strconv.Atoi(vtStr); err == nil {
					visibilityTimeout = vt
				}
			}
			if req.VisibilityTimeout > 0 {
				visibilityTimeout = req.VisibilityTimeout
			}

			// Call the appropriate receive logic based on queue type.
			if isFifo {
				return s.receiveFifoMessages(tr, queueDir, req, maxMessages, visibilityTimeout, queueAttributes)
			} else {
				return s.receiveStandardMessages(tr, queueDir, req, maxMessages, visibilityTimeout, queueAttributes)
			}
		})

		if err != nil {
			return nil, err
		}

		var messages []models.ResponseMessage
		if rawMessages != nil {
			messages = rawMessages.([]models.ResponseMessage)
		}

		// If messages were found, return them immediately.
		if len(messages) > 0 {
			return &models.ReceiveMessageResponse{Messages: messages}, nil
		}

		// If no messages were found and the wait time has elapsed, return an empty response.
		if time.Since(startTime).Seconds() >= float64(waitTime) {
			return &models.ReceiveMessageResponse{Messages: []models.ResponseMessage{}}, nil
		}

		// Wait briefly before retrying to avoid busy-spinning.
		time.Sleep(100 * time.Millisecond)
	}
}

// DeleteMessage deletes a single message from a queue using its receipt handle.
func (s *FDBStore) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
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
		inflightDir := queueDir.Sub("inflight")

		// 1. Look up the receipt handle to find the message it corresponds to.
		inflightKey := inflightDir.Pack(tuple.Tuple{receiptHandle})
		receiptBytes, err := tr.Get(inflightKey).Get()
		if err != nil {
			return nil, err
		}
		// If the handle doesn't exist, the message might have already been deleted
		// or its visibility timeout expired. SQS treats this as a success.
		if receiptBytes == nil {
			return nil, nil
		}

		var receiptData map[string]interface{}
		if err := json.Unmarshal(receiptBytes, &receiptData); err != nil {
			return nil, ErrInvalidReceiptHandle
		}

		messageID := receiptData["id"].(string)

		// 2. For FIFO queues, we need to unlock the message's group so other messages
		//    from that group can be received.
		msgBytes, err := tr.Get(messagesDir.Pack(tuple.Tuple{messageID})).Get()
		if err != nil {
			return nil, err
		}
		if msgBytes != nil {
			isFifo := strings.HasSuffix(queueName, ".fifo")
			if isFifo {
				var msg models.Message
				if err := json.Unmarshal(msgBytes, &msg); err == nil && msg.MessageGroupId != "" {
					inflightGroupsDir := queueDir.Sub("inflight_groups")
					tr.Clear(inflightGroupsDir.Pack(tuple.Tuple{msg.MessageGroupId}))
				}
			}
		}

		// 3. Delete the main message data.
		tr.Clear(messagesDir.Pack(tuple.Tuple{messageID}))

		// 4. Delete the message from its index. The receipt data contains the
		//    original index key so we know which index to clean up.
		if fifoKeyStr, ok := receiptData["fifo_key"].(string); ok {
			// It's a FIFO message, delete from `fifo_idx`.
			fifoKey, err := base64.StdEncoding.DecodeString(fifoKeyStr)
			if err == nil {
				tr.Clear(fdb.Key(fifoKey))
			}
		} else if visKeyBytes, ok := receiptData["vis_key"].([]byte); ok {
			// It's a Standard message, delete from `visible_idx`.
			tr.Clear(fdb.Key(visKeyBytes))
		}

		// 5. Delete the in-flight receipt handle itself.
		tr.Clear(inflightKey)

		return nil, nil
	})
	return err
}

// DeleteMessageBatch deletes multiple messages in a single transaction.
func (s *FDBStore) DeleteMessageBatch(ctx context.Context, queueName string, entries []models.DeleteMessageBatchRequestEntry) (*models.DeleteMessageBatchResponse, error) {
	resp, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{queueName})
		if err != nil {
			return nil, err
		}
		if !exists {
			// As per SQS, if the queue doesn't exist, the whole batch fails.
			// The handler layer will convert this error into a proper batch response.
			return nil, ErrQueueDoesNotExist
		}

		queueDir, err := s.dir.Open(tr, []string{queueName}, nil)
		if err != nil {
			return nil, err
		}

		messagesDir := queueDir.Sub("messages")
		inflightDir := queueDir.Sub("inflight")
		isFifo := strings.HasSuffix(queueName, ".fifo")

		successful := []models.DeleteMessageBatchResultEntry{}
		failed := []models.BatchResultErrorEntry{}

		// Process each entry in the batch.
		for _, entry := range entries {
			receiptHandle := entry.ReceiptHandle
			inflightKey := inflightDir.Pack(tuple.Tuple{receiptHandle})
			receiptBytes, err := tr.Get(inflightKey).Get()
			if err != nil {
				// A database error fails the whole transaction.
				return nil, err
			}

			// If the receipt handle is not found, it's considered a success.
			if receiptBytes == nil {
				successful = append(successful, models.DeleteMessageBatchResultEntry{Id: entry.Id})
				continue
			}

			var receiptData map[string]interface{}
			if err := json.Unmarshal(receiptBytes, &receiptData); err != nil {
				failed = append(failed, models.BatchResultErrorEntry{Id: entry.Id, Code: "InvalidReceiptHandle", Message: "The receipt handle is not valid.", SenderFault: true})
				continue
			}

			messageID, ok := receiptData["id"].(string)
			if !ok {
				failed = append(failed, models.BatchResultErrorEntry{Id: entry.Id, Code: "InvalidReceiptHandle", Message: "The receipt handle is corrupt.", SenderFault: true})
				continue
			}

			// The deletion logic is identical to the single DeleteMessage handler.
			msgBytes, err := tr.Get(messagesDir.Pack(tuple.Tuple{messageID})).Get()
			if err != nil {
				return nil, err
			}
			if msgBytes != nil && isFifo {
				var msg models.Message
				if err := json.Unmarshal(msgBytes, &msg); err == nil && msg.MessageGroupId != "" {
					inflightGroupsDir := queueDir.Sub("inflight_groups")
					tr.Clear(inflightGroupsDir.Pack(tuple.Tuple{msg.MessageGroupId}))
				}
			}
			tr.Clear(messagesDir.Pack(tuple.Tuple{messageID}))
			tr.Clear(inflightKey)
			if fifoKeyStr, ok := receiptData["fifo_key"].(string); ok {
				fifoKey, err := base64.StdEncoding.DecodeString(fifoKeyStr)
				if err == nil {
					tr.Clear(fdb.Key(fifoKey))
				}
			}

			successful = append(successful, models.DeleteMessageBatchResultEntry{Id: entry.Id})
		}

		return &models.DeleteMessageBatchResponse{Successful: successful, Failed: failed}, nil
	})

	if err != nil {
		return nil, err
	}

	return resp.(*models.DeleteMessageBatchResponse), nil
}

// --- Unimplemented Methods ---

// ChangeMessageVisibility changes the visibility timeout of a specified message in a queue to a new value.
// This is typically used to extend the amount of time a consumer has to process a message.
func (s *FDBStore) ChangeMessageVisibility(ctx context.Context, queueName string, receiptHandle string, visibilityTimeout int) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return nil, s.changeMessageVisibility(tr, queueName, receiptHandle, visibilityTimeout)
	})
	return err
}

func (s *FDBStore) changeMessageVisibility(tr fdb.Transaction, queueName string, receiptHandle string, visibilityTimeout int) error {
	queueDir, err := s.dir.Open(tr, []string{queueName}, nil)
	if err != nil {
		// This can happen if the queue is deleted mid-transaction.
		return ErrQueueDoesNotExist
	}

	messagesDir := queueDir.Sub("messages")
	inflightDir := queueDir.Sub("inflight")
	visibleIdxDir := queueDir.Sub("visible_idx")

	inflightKey := inflightDir.Pack(tuple.Tuple{receiptHandle})
	receiptBytes, err := tr.Get(inflightKey).Get()
	if err != nil {
		return err
	}
	if receiptBytes == nil {
		return ErrInvalidReceiptHandle
	}

	var receiptData map[string]interface{}
	if err := json.Unmarshal(receiptBytes, &receiptData); err != nil {
		return ErrInvalidReceiptHandle
	}

	messageID, ok := receiptData["id"].(string)
	if !ok {
		return ErrInvalidReceiptHandle
	}

	msgBytes, err := tr.Get(messagesDir.Pack(tuple.Tuple{messageID})).Get()
	if err != nil {
		return err
	}
	if msgBytes == nil {
		tr.Clear(inflightKey)
		return ErrMessageNotInflight
	}
	var msg models.Message
	if err := json.Unmarshal(msgBytes, &msg); err != nil {
		return err
	}

	// Clear the old visibility timeout key.
	if visKeyBytes, ok := receiptData["vis_key"].([]byte); ok {
		tr.Clear(fdb.Key(visKeyBytes))
	}

	now := time.Now().Unix()
	newVisibilityTimeout := now + int64(visibilityTimeout)
	msg.VisibleAfter = newVisibilityTimeout

	newMsgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	tr.Set(messagesDir.Pack(tuple.Tuple{messageID}), newMsgBytes)

	newShardID := rand.Intn(numShards)
	newVisKey := visibleIdxDir.Pack(tuple.Tuple{newShardID, newVisibilityTimeout, messageID})
	tr.Set(newVisKey, []byte{})

	receiptData["vis_key"] = newVisKey
	newReceiptBytes, err := json.Marshal(receiptData)
	if err != nil {
		return err
	}
	tr.Set(inflightKey, newReceiptBytes)

	return nil
}

// ChangeMessageVisibilityBatch changes the visibility timeout of multiple messages in a single call.
// This reduces the number of API requests required to manage message visibility.
func (s *FDBStore) ChangeMessageVisibilityBatch(ctx context.Context, queueName string, entries []models.ChangeMessageVisibilityBatchRequestEntry) (*models.ChangeMessageVisibilityBatchResponse, error) {
	resp := &models.ChangeMessageVisibilityBatchResponse{
		Successful: []models.ChangeMessageVisibilityBatchResultEntry{},
		Failed:     []models.BatchResultErrorEntry{},
	}

	// First, check if the queue exists. If not, the entire batch fails.
	// This check is implicitly handled by getQueueSubspace, but an explicit
	// check here makes the logic clearer and fails faster.
	_, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{queueName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrQueueDoesNotExist
		}
		return nil, nil
	})

	if err != nil {
		// If the queue doesn't exist, we can't proceed. The handler layer
		// will need to turn this into a batch failure response.
		// For other errors, we also return immediately.
		return nil, err
	}

	for _, entry := range entries {
		err := s.ChangeMessageVisibility(ctx, queueName, entry.ReceiptHandle, entry.VisibilityTimeout)
		if err != nil {
			// Map internal errors to SQS error types for the response.
			code := "InternalError"
			senderFault := false
			if errors.Is(err, ErrInvalidReceiptHandle) {
				code = "ReceiptHandleIsInvalid"
				senderFault = true
			} else if errors.Is(err, ErrMessageNotInflight) {
				code = "MessageNotInflight"
				senderFault = true
			}

			resp.Failed = append(resp.Failed, models.BatchResultErrorEntry{
				Id:          entry.Id,
				Code:        code,
				Message:     err.Error(),
				SenderFault: senderFault,
			})
		} else {
			resp.Successful = append(resp.Successful, models.ChangeMessageVisibilityBatchResultEntry{
				Id: entry.Id,
			})
		}
	}

	return resp, nil
}

// AddPermission adds a permission to a queue for a specific principal.
// This allows sharing access to the queue with other AWS accounts.
func (s *FDBStore) AddPermission(ctx context.Context, queueName, label string, accountIds []string, actions []string) error {
	// Use generic map for Statement to preserve all fields
	type Policy struct {
		Version   string                   `json:"Version"`
		Id        string                   `json:"Id"`
		Statement []map[string]interface{} `json:"Statement"`
	}

	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
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

		// Retrieve existing attributes to get the current Policy
		attributesKey := queueDir.Pack(tuple.Tuple{"attributes"})
		attrsBytes, err := tr.Get(attributesKey).Get()
		if err != nil {
			return nil, err
		}

		var attributes map[string]string
		if len(attrsBytes) > 0 {
			if err := json.Unmarshal(attrsBytes, &attributes); err != nil {
				return nil, err
			}
		} else {
			attributes = make(map[string]string)
		}

		// Parse existing policy or create new
		var policy Policy
		if policyStr, ok := attributes["Policy"]; ok && policyStr != "" {
			if err := json.Unmarshal([]byte(policyStr), &policy); err != nil {
				policy = Policy{
					Version:   "2012-10-17",
					Id:        "QueuePolicy",
					Statement: []map[string]interface{}{},
				}
			}
		} else {
			policy = Policy{
				Version:   "2012-10-17",
				Id:        "QueuePolicy",
				Statement: []map[string]interface{}{},
			}
		}

		resourceArn := "arn:aws:sqs:us-east-1:123456789012:" + queueName

		newStatement := map[string]interface{}{
			"Sid":       label,
			"Effect":    "Allow",
			"Principal": map[string]any{"AWS": accountIds},
			"Action":    actions,
			"Resource":  resourceArn,
		}

		// Add or Replace statement with same Sid (Label)
		found := false
		for i, stmt := range policy.Statement {
			if sid, ok := stmt["Sid"].(string); ok && sid == label {
				policy.Statement[i] = newStatement
				found = true
				break
			}
		}
		if !found {
			policy.Statement = append(policy.Statement, newStatement)
		}

		// Serialize policy back to JSON
		newPolicyBytes, err := json.Marshal(policy)
		if err != nil {
			return nil, err
		}
		attributes["Policy"] = string(newPolicyBytes)

		// Save attributes
		finalAttrsBytes, err := json.Marshal(attributes)
		if err != nil {
			return nil, err
		}
		tr.Set(attributesKey, finalAttrsBytes)

		return nil, nil
	})

	return err
}

// RemovePermission removes a permission from a queue.
// It looks for a policy statement with the specified label and deletes it.
func (s *FDBStore) RemovePermission(ctx context.Context, queueName, label string) error {
	// Use generic map for Statement to preserve all fields
	type Policy struct {
		Version   string                   `json:"Version"`
		Id        string                   `json:"Id"`
		Statement []map[string]interface{} `json:"Statement"`
	}

	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
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

		// Retrieve existing attributes to get the current Policy
		attributesKey := queueDir.Pack(tuple.Tuple{"attributes"})
		attrsBytes, err := tr.Get(attributesKey).Get()
		if err != nil {
			return nil, err
		}

		var attributes map[string]string
		if len(attrsBytes) > 0 {
			if err := json.Unmarshal(attrsBytes, &attributes); err != nil {
				return nil, err
			}
		} else {
			return nil, ErrLabelDoesNotExist
		}

		// Parse existing policy
		var policy Policy
		if policyStr, ok := attributes["Policy"]; ok && policyStr != "" {
			if err := json.Unmarshal([]byte(policyStr), &policy); err != nil {
				return nil, err
			}
		} else {
			return nil, ErrLabelDoesNotExist
		}

		// Find and remove the statement with the matching Sid (Label)
		found := false
		newStatements := []map[string]interface{}{}
		for _, stmt := range policy.Statement {
			if sid, ok := stmt["Sid"].(string); ok && sid == label {
				found = true
			} else {
				newStatements = append(newStatements, stmt)
			}
		}

		if !found {
			return nil, ErrLabelDoesNotExist
		}

		// Update policy
		policy.Statement = newStatements

		// If no statements left, we delete the Policy attribute (as per standard SQS behavior)
		if len(policy.Statement) == 0 {
			delete(attributes, "Policy")
		} else {
			newPolicyBytes, err := json.Marshal(policy)
			if err != nil {
				return nil, err
			}
			attributes["Policy"] = string(newPolicyBytes)
		}

		// Save attributes
		finalAttrsBytes, err := json.Marshal(attributes)
		if err != nil {
			return nil, err
		}
		tr.Set(attributesKey, finalAttrsBytes)

		return nil, nil
	})

	return err
}

// ListQueueTags lists the tags for a specified queue.
// Tags are key-value pairs used for organizing and tracking resources.
func (s *FDBStore) ListQueueTags(ctx context.Context, queueName string) (map[string]string, error) {
	tags, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
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

		tagsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"tags"})).Get()
		if err != nil {
			return nil, err
		}
		if len(tagsBytes) == 0 {
			return make(map[string]string), nil
		}

		var storedTags map[string]string
		if err := json.Unmarshal(tagsBytes, &storedTags); err != nil {
			return nil, err
		}
		return storedTags, nil
	})
	if err != nil {
		return nil, err
	}
	return tags.(map[string]string), nil
}

// TagQueue adds tags to a queue.
// If a tag with the same key already exists, its value is overwritten.
func (s *FDBStore) TagQueue(ctx context.Context, queueName string, tags map[string]string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
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

		// Get existing tags, or create a new map
		var existingTags map[string]string
		tagsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"tags"})).Get()
		if err != nil {
			return nil, err
		}
		if len(tagsBytes) > 0 {
			if err := json.Unmarshal(tagsBytes, &existingTags); err != nil {
				return nil, err // Corrupt data
			}
		} else {
			existingTags = make(map[string]string)
		}

		// Add new tags
		for k, v := range tags {
			existingTags[k] = v
		}

		// Write back
		newTagsBytes, err := json.Marshal(existingTags)
		if err != nil {
			return nil, err
		}
		tr.Set(queueDir.Pack(tuple.Tuple{"tags"}), newTagsBytes)
		return nil, nil
	})
	return err
}

// UntagQueue removes tags from a queue.
// It takes a list of tag keys to be removed.
func (s *FDBStore) UntagQueue(ctx context.Context, queueName string, tagKeys []string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
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

		var existingTags map[string]string
		tagsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"tags"})).Get()
		if err != nil {
			return nil, err
		}
		if len(tagsBytes) > 0 {
			if err := json.Unmarshal(tagsBytes, &existingTags); err != nil {
				return nil, err
			}
		} else {
			// Nothing to untag
			return nil, nil
		}

		for _, key := range tagKeys {
			delete(existingTags, key)
		}

		newTagsBytes, err := json.Marshal(existingTags)
		if err != nil {
			return nil, err
		}
		tr.Set(queueDir.Pack(tuple.Tuple{"tags"}), newTagsBytes)
		return nil, nil
	})
	return err
}

// ListDeadLetterSourceQueues lists queues that have the specified queue configured as their dead-letter queue.
// This is useful for identifying which queues are sending failed messages to a particular DLQ.
func (s *FDBStore) ListDeadLetterSourceQueues(ctx context.Context, queueURL string, maxResults int, nextToken string) ([]string, string, error) {
	parts := strings.Split(queueURL, "/")
	if len(parts) == 0 {
		return nil, "", ErrQueueDoesNotExist
	}
	dlqName := parts[len(parts)-1]
	if dlqName == "" {
		return nil, "", ErrQueueDoesNotExist
	}

	// Verify the DLQ actually exists.
	_, err := s.GetQueueAttributes(ctx, dlqName)
	if err != nil {
		return nil, "", err // Returns ErrQueueDoesNotExist if applicable
	}

	resp, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{"dlq_sources"})
		if err != nil {
			return nil, err
		}
		if !exists {
			return []string{}, nil
		}

		dlqIndexDir, err := s.dir.Open(tr, []string{"dlq_sources"}, nil)
		if err != nil {
			return nil, err
		}

		// Define the range for the scan.
		// The key is `(dlq_name, source_queue_name)`.
		beginTuple := tuple.Tuple{dlqName}
		if nextToken != "" {
			beginTuple = tuple.Tuple{dlqName, nextToken + "\x00"} // Start after the nextToken
		}
		pr, err := fdb.PrefixRange(dlqIndexDir.Pack(tuple.Tuple{dlqName}))
		if err != nil {
			return nil, err
		}
		keyRange := fdb.KeyRange{Begin: dlqIndexDir.Pack(beginTuple), End: pr.End}

		// Perform the range read.
		rangeOpts := fdb.RangeOptions{}
		if maxResults > 0 {
			rangeOpts.Limit = maxResults
		}
		kvs, err := tr.GetRange(keyRange, rangeOpts).GetSliceWithError()
		if err != nil {
			return nil, err
		}

		var sourceQueues []string
		for _, kv := range kvs {
			t, err := dlqIndexDir.Unpack(kv.Key)
			if err != nil || len(t) < 2 {
				continue
			}
			sourceQueueName, ok := t[1].(string)
			if !ok {
				continue
			}
			sourceQueues = append(sourceQueues, sourceQueueName)
		}
		return sourceQueues, nil
	})

	if err != nil {
		return nil, "", err
	}

	sourceQueues := resp.([]string)
	var newNextToken string
	if maxResults > 0 && len(sourceQueues) == maxResults {
		newNextToken = sourceQueues[len(sourceQueues)-1]
	}

	return sourceQueues, newNextToken, nil
}

// StartMessageMoveTask initiates a task to move messages from a source queue to a destination queue.
// It persists the task state and spawns a background goroutine to perform the message movement.
func (s *FDBStore) StartMessageMoveTask(ctx context.Context, sourceArn, destinationArn string) (string, error) {
	taskHandle := uuid.New().String()
	now := time.Now().UnixNano() / int64(time.Millisecond)

	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// We use a global subspace for move tasks since they are identified by a unique handle
		// and can be listed by source ARN.
		tasksDir, err := s.dir.CreateOrOpen(tr, []string{"move_tasks"}, nil)
		if err != nil {
			return nil, err
		}

		// Store task details
		task := models.ListMessageMoveTasksResultEntry{
			TaskHandle:       taskHandle,
			Status:           "RUNNING",
			SourceArn:        sourceArn,
			DestinationArn:   destinationArn,
			StartedTimestamp: now,
		}

		taskBytes, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}

		// Key: (handle) -> task_json
		tr.Set(tasksDir.Pack(tuple.Tuple{taskHandle}), taskBytes)

		// Create an index for listing by SourceArn
		// Key: ("by_source", sourceArn, startedTimestamp, handle) -> ""
		indexDir := tasksDir.Sub("by_source")
		tr.Set(indexDir.Pack(tuple.Tuple{sourceArn, now, taskHandle}), []byte{})

		return nil, nil
	})

	if err != nil {
		return "", err
	}

	// spawn background worker
	go s.runMessageMoveTask(taskHandle, sourceArn, destinationArn)

	return taskHandle, nil
}

func getQueueNameFromArn(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

func (s *FDBStore) updateTaskStatus(taskHandle, status, failureReason string, moved int64) {
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tasksDir, err := s.dir.CreateOrOpen(tr, []string{"move_tasks"}, nil)
		if err != nil {
			return nil, err
		}
		taskKey := tasksDir.Pack(tuple.Tuple{taskHandle})
		taskBytes, err := tr.Get(taskKey).Get()
		if err != nil || taskBytes == nil {
			return nil, nil
		}

		var task models.ListMessageMoveTasksResultEntry
		if err := json.Unmarshal(taskBytes, &task); err != nil {
			return nil, err
		}

		task.Status = status
		if failureReason != "" {
			task.FailureReason = failureReason
		}
		task.ApproximateNumberOfMessagesMoved = moved

		newBytes, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		tr.Set(taskKey, newBytes)
		return nil, nil
	})
}

func (s *FDBStore) runMessageMoveTask(taskHandle, sourceArn, destinationArn string) {
	ctx := context.Background()
	sourceQueueName := getQueueNameFromArn(sourceArn)
	destQueueName := getQueueNameFromArn(destinationArn)
	if destQueueName == "" {
		destQueueName = sourceQueueName
	}

	totalMoved := int64(0)

	for {
		// 1. Check for cancellation
		var currentStatus string
		_, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
			tasksDir, err := s.dir.Open(tr, []string{"move_tasks"}, nil)
			if err != nil {
				return nil, err
			}
			taskBytes, err := tr.Get(tasksDir.Pack(tuple.Tuple{taskHandle})).Get()
			if err != nil || taskBytes == nil {
				return nil, errors.New("task not found")
			}
			var task models.ListMessageMoveTasksResultEntry
			if err := json.Unmarshal(taskBytes, &task); err != nil {
				return nil, err
			}
			currentStatus = task.Status
			return nil, nil
		})

		if err != nil || currentStatus == "CANCELLED" {
			return
		}

		// 2. Receive messages
		receiveReq := &models.ReceiveMessageRequest{
			QueueUrl:              sourceQueueName,
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       1,
			VisibilityTimeout:     30,
			AttributeNames:        []string{"All"},
			MessageAttributeNames: []string{"All"},
		}

		resp, err := s.ReceiveMessage(ctx, sourceQueueName, receiveReq)
		if err != nil {
			s.updateTaskStatus(taskHandle, "FAILED", err.Error(), totalMoved)
			return
		}

		if len(resp.Messages) == 0 {
			s.updateTaskStatus(taskHandle, "COMPLETED", "", totalMoved)
			return
		}

		// 3. Send Batch
		sendEntries := make([]models.SendMessageBatchRequestEntry, 0, len(resp.Messages))
		deleteEntries := make([]models.DeleteMessageBatchRequestEntry, 0, len(resp.Messages))

		for _, msg := range resp.Messages {
			msgAttrs := make(map[string]models.MessageAttributeValue)
			for k, v := range msg.MessageAttributes {
				msgAttrs[k] = v
			}

			entry := models.SendMessageBatchRequestEntry{
				Id:                msg.MessageId,
				MessageBody:       msg.Body,
				MessageAttributes: msgAttrs,
			}
			// Check for FIFO attributes in Attributes map
			if val, ok := msg.Attributes["MessageGroupId"]; ok {
				groupId := val
				entry.MessageGroupId = &groupId
			}
			if val, ok := msg.Attributes["MessageDeduplicationId"]; ok {
				dedupId := val
				entry.MessageDeduplicationId = &dedupId
			}
			sendEntries = append(sendEntries, entry)

			deleteEntries = append(deleteEntries, models.DeleteMessageBatchRequestEntry{
				Id:            msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
			})
		}

		sendReq := &models.SendMessageBatchRequest{
			QueueUrl: destQueueName,
			Entries:  sendEntries,
		}

		sendResp, err := s.SendMessageBatch(ctx, destQueueName, sendReq)
		if err != nil {
			s.updateTaskStatus(taskHandle, "FAILED", "Failed to send messages: "+err.Error(), totalMoved)
			return
		}

		// Only delete messages that were successfully sent.
		successIds := make(map[string]bool)
		for _, success := range sendResp.Successful {
			successIds[success.Id] = true
		}

		var successfulDeleteEntries []models.DeleteMessageBatchRequestEntry
		for _, entry := range deleteEntries {
			if successIds[entry.Id] {
				successfulDeleteEntries = append(successfulDeleteEntries, entry)
			}
		}

		if len(successfulDeleteEntries) > 0 {
			_, err = s.DeleteMessageBatch(ctx, sourceQueueName, successfulDeleteEntries)
			if err != nil {
				s.updateTaskStatus(taskHandle, "FAILED", "Failed to delete messages: "+err.Error(), totalMoved)
				return
			}
			totalMoved += int64(len(successfulDeleteEntries))
		}

		if len(sendResp.Failed) > 0 {
			if len(successfulDeleteEntries) == 0 {
				s.updateTaskStatus(taskHandle, "FAILED", "All messages in batch failed to send", totalMoved)
				return
			}
			// Some failed, but some succeeded. Continue, but maybe log failure?
			// The status update below effectively updates the count.
		}

		// Update progress
		s.updateTaskStatus(taskHandle, "RUNNING", "", totalMoved)
	}
}

// CancelMessageMoveTask cancels a running message move task.
func (s *FDBStore) CancelMessageMoveTask(ctx context.Context, taskHandle string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tasksDir, err := s.dir.CreateOrOpen(tr, []string{"move_tasks"}, nil)
		if err != nil {
			return nil, err
		}

		taskKey := tasksDir.Pack(tuple.Tuple{taskHandle})
		taskBytes, err := tr.Get(taskKey).Get()
		if err != nil {
			return nil, err
		}
		if taskBytes == nil {
			return nil, errors.New("ResourceNotFoundException") // Task does not exist
		}

		var task models.ListMessageMoveTasksResultEntry
		if err := json.Unmarshal(taskBytes, &task); err != nil {
			return nil, err
		}

		// Update status to CANCELLED
		if task.Status != "COMPLETED" && task.Status != "FAILED" && task.Status != "CANCELLED" {
			task.Status = "CANCELLED"
			task.FailureReason = "Cancelled by user" // Optional
			newTaskBytes, err := json.Marshal(task)
			if err != nil {
				return nil, err
			}
			tr.Set(taskKey, newTaskBytes)
		}

		return nil, nil
	})

	return err
}

// ListMessageMoveTasks lists the message move tasks for a specific source queue.
func (s *FDBStore) ListMessageMoveTasks(ctx context.Context, sourceArn string) ([]models.ListMessageMoveTasksResultEntry, error) {
	tasks, err := s.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		// Read-only transaction cannot CreateOrOpen. We must assume it exists or use Open.
		// However, "move_tasks" might not exist yet if no tasks were started.
		// Use Open and handle error if it doesn't exist.
		exists, err := s.dir.Exists(tr, []string{"move_tasks"})
		if err != nil {
			return nil, err
		}
		if !exists {
			return []models.ListMessageMoveTasksResultEntry{}, nil
		}

		tasksDir, err := s.dir.Open(tr, []string{"move_tasks"}, nil)
		if err != nil {
			return nil, err
		}

		// Key: ("by_source", sourceArn, startedTimestamp, handle) -> ""
		indexDir := tasksDir.Sub("by_source")
		sourceIndexTuple := tuple.Tuple{sourceArn}

		// Range scan for the given source ARN
		pr, err := fdb.PrefixRange(indexDir.Pack(sourceIndexTuple))
		if err != nil {
			return nil, err
		}

		iter := tr.GetRange(pr, fdb.RangeOptions{}).Iterator()
		var results []models.ListMessageMoveTasksResultEntry

		for iter.Advance() {
			kv := iter.MustGet()
			t, err := indexDir.Unpack(kv.Key)
			if err != nil {
				continue
			}
			// Tuple structure: {sourceArn, startedTimestamp, handle}
			// t[0] = sourceArn, t[1] = startedTimestamp, t[2] = handle
			if len(t) < 3 {
				continue
			}
			taskHandle, ok := t[2].(string)
			if !ok {
				continue
			}

			// Fetch the full task details
			taskKey := tasksDir.Pack(tuple.Tuple{taskHandle})
			taskBytes, err := tr.Get(taskKey).Get()
			if err != nil || taskBytes == nil {
				continue
			}

			var task models.ListMessageMoveTasksResultEntry
			if err := json.Unmarshal(taskBytes, &task); err != nil {
				continue
			}
			results = append(results, task)
		}

		return results, nil
	})

	if err != nil {
		return nil, err
	}
	if tasks == nil {
		return []models.ListMessageMoveTasksResultEntry{}, nil
	}
	return tasks.([]models.ListMessageMoveTasksResultEntry), nil
}
