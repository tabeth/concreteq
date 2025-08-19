package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"errors"
	"github.com/google/uuid"
	"github.com/tabeth/concreteq/models"
)

// FDBStore is a FoundationDB implementation of the Store interface.
type FDBStore struct {
	db  fdb.Database
	dir directory.DirectorySubspace
}

// GetDB returns the underlying FoundationDB database object.
func (s *FDBStore) GetDB() fdb.Database {
	return s.db
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
		// Check if the directory already exists in a more robust way.
		exists, err := s.dir.Exists(tr, []string{name})
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, ErrQueueAlreadyExists
		}

		// The directory layer provides a way to organize data. We'll create a directory for each queue.
		queueDir, err := s.dir.Create(tr, []string{name}, nil)
		if err != nil {
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
		found := false
		for i, q := range filteredQueues {
			if q == nextToken {
				startIndex = i + 1
				found = true
				break
			}
		}
		if !found {
			// Invalid nextToken, return empty list as per SQS behavior
			return []string{}, "", nil
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
		// 1. Check if queue exists
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

		// 2. FIFO-specific: Check for send deduplication
		if isFifo && message.MessageDeduplicationId != nil && *message.MessageDeduplicationId != "" {
			dedupDir := queueDir.Sub("dedup")
			dedupKey := dedupDir.Pack(tuple.Tuple{*message.MessageDeduplicationId})
			val, err := tr.Get(dedupKey).Get()
			if err != nil {
				return nil, err
			}
			if val != nil {
				// Message with this dedup ID already exists. Return stored response.
				var storedResp models.SendMessageResponse
				if err := json.Unmarshal(val, &storedResp); err == nil {
					return &storedResp, nil
				}
				// If unmarshal fails, proceed to send a new one, overwriting the corrupt dedup entry.
			}
		}

		// 3. Common: Generate IDs and Hashes
		messageID := uuid.New().String()
		bodyHash := md5.Sum([]byte(message.MessageBody))
		md5OfBody := hex.EncodeToString(bodyHash[:])
		var md5OfAttributes string
		if len(message.MessageAttributes) > 0 {
			attrHash := md5.Sum(hashAttributes(message.MessageAttributes, nil))
			md5OfAttributes = hex.EncodeToString(attrHash[:])
		}

		// 4. Common: Construct the internal message model
		sentTimestamp := time.Now().Unix()
		internalMessage := models.Message{
			ID:                messageID,
			Body:              message.MessageBody,
			Attributes:        message.MessageAttributes,
			MD5OfBody:         md5OfBody,
			MD5OfAttributes:   md5OfAttributes,
			SentTimestamp:     sentTimestamp,
			SenderId:          "123456789012", // Placeholder for SenderId
		}

		// 5. Write message and index based on queue type
		if isFifo {
			// FIFO Path
			fifoIdxDir := queueDir.Sub("fifo_idx")
			seq, err := s.getNextSequenceNumber(tr, queueDir)
			if err != nil {
				return nil, err
			}
			internalMessage.SequenceNumber = seq
			internalMessage.MessageGroupId = *message.MessageGroupId

			// Write to the FIFO index: (groupId, sequenceNumber) -> messageId
			fifoKey := fifoIdxDir.Pack(tuple.Tuple{*message.MessageGroupId, seq})
			tr.Set(fifoKey, []byte(messageID))

		} else {
			// Standard Queue Path
			visibleIdxDir := queueDir.Sub("visible_idx")
			visibleAfter := sentTimestamp
			if message.DelaySeconds != nil {
				visibleAfter += int64(*message.DelaySeconds)
			}
			internalMessage.VisibleAfter = visibleAfter

			// Write to the visibility index: (visible_after_ts, messageId) -> empty
			visKey := visibleIdxDir.Pack(tuple.Tuple{visibleAfter, messageID})
			tr.Set(visKey, []byte{})
		}

		// 6. Common: Write the main message blob
		msgBytes, err := json.Marshal(internalMessage)
		if err != nil {
			return nil, err
		}
		tr.Set(messagesDir.Pack(tuple.Tuple{messageID}), msgBytes)

		// 7. Common: Construct the response
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

		// 8. FIFO-specific: Store response for send-deduplication
		if isFifo && message.MessageDeduplicationId != nil && *message.MessageDeduplicationId != "" {
			dedupDir := queueDir.Sub("dedup")
			dedupKey := dedupDir.Pack(tuple.Tuple{*message.MessageDeduplicationId})
			respBytes, err := json.Marshal(response)
			if err == nil {
				tr.Set(dedupKey, respBytes)
				// TODO: Need a background process to clean up expired dedup entries.
			}
		}

		return response, nil
	})

	if err != nil {
		return nil, err
	}
	return resp.(*models.SendMessageResponse), nil
}

// buildResponseAttributes constructs the system attributes map for a message response.
func (s *FDBStore) buildResponseAttributes(msg *models.Message, req *models.ReceiveMessageRequest) map[string]string {
	attrs := make(map[string]string)
	requestedAttrs := make(map[string]bool)

	// Combine AttributeNames (deprecated) and MessageSystemAttributeNames
	allRequested := append(req.AttributeNames, req.MessageSystemAttributeNames...)

	for _, attrName := range allRequested {
		requestedAttrs[attrName] = true
	}

	wantsAll := requestedAttrs["All"]

	if wantsAll || requestedAttrs["ApproximateReceiveCount"] {
		attrs["ApproximateReceiveCount"] = strconv.Itoa(msg.ReceivedCount)
	}
	if wantsAll || requestedAttrs["ApproximateFirstReceiveTimestamp"] {
		// Convert epoch seconds to epoch milliseconds
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

// buildResponseMessageAttributes constructs the user-defined message attributes map for a message response.
func (s *FDBStore) buildResponseMessageAttributes(msg *models.Message, req *models.ReceiveMessageRequest) map[string]models.MessageAttributeValue {
	// If no attributes are in the message, or none requested, return nil
	if len(msg.Attributes) == 0 || len(req.MessageAttributeNames) == 0 {
		return nil
	}

	returnedAttrs := make(map[string]models.MessageAttributeValue)
	wantsAll := false
	requested := make(map[string]bool)

	for _, name := range req.MessageAttributeNames {
		if name == "All" || name == ".*" {
			wantsAll = true
			break
		}
		requested[name] = true
	}

	if wantsAll {
		return msg.Attributes
	}

	for name, value := range msg.Attributes {
		if requested[name] {
			returnedAttrs[name] = value
		}
	}

	if len(returnedAttrs) == 0 {
		return nil
	}
	return returnedAttrs
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

func (s *FDBStore) receiveStandardMessages(tr fdb.Transaction, queueDir directory.DirectorySubspace, req *models.ReceiveMessageRequest, maxMessages int, visibilityTimeout int) ([]models.ResponseMessage, error) {
	messagesDir := queueDir.Sub("messages")
	visibleIdxDir := queueDir.Sub("visible_idx")
	inflightDir := queueDir.Sub("inflight")

	var receivedMessages []models.ResponseMessage
	now := time.Now().Unix()

	// Query the visibility index for messages that are currently visible.
	// The key is (visible_after_ts, messageId). We want all keys where visible_after_ts <= now.
	beginKey := visibleIdxDir.Pack(tuple.Tuple{0})
	endKey := visibleIdxDir.Pack(tuple.Tuple{now + 1})
	r := tr.GetRange(fdb.KeyRange{Begin: beginKey, End: endKey}, fdb.RangeOptions{Limit: maxMessages})

	iter := r.Iterator()
	for iter.Advance() {
		kv := iter.MustGet()

		// Extract message ID from the index key
		t, err := visibleIdxDir.Unpack(kv.Key)
		if err != nil { continue } // Should not happen
		messageID := t[1].(string)

		// Get the full message from the main store
		msgBytes, err := tr.Get(messagesDir.Pack(tuple.Tuple{messageID})).Get()
		if err != nil || msgBytes == nil { continue } // Message might have been deleted
		var msg models.Message
		if err := json.Unmarshal(msgBytes, &msg); err != nil { continue }

		// This message is visible and available. Let's process it.
		// 1. Delete old index entry
		tr.Clear(kv.Key)

		// 2. Update message's visibility and receive count
		newVisibilityTimeout := now + int64(visibilityTimeout)
		msg.VisibleAfter = newVisibilityTimeout
		msg.ReceivedCount++
		if msg.FirstReceived == 0 {
			msg.FirstReceived = now
		}

		// 3. Write the updated message back to the main store
		newMsgBytes, err := json.Marshal(msg)
		if err != nil { continue }
		tr.Set(messagesDir.Pack(tuple.Tuple{messageID}), newMsgBytes)

		// 4. Create new index entry for the updated visibility
		newVisKey := visibleIdxDir.Pack(tuple.Tuple{newVisibilityTimeout, messageID})
		tr.Set(newVisKey, []byte{})

		// 5. Create receipt handle and construct response message
		receiptHandle := uuid.New().String()
		receiptData := map[string]interface{}{
			"id":      msg.ID,
			"vis_key": kv.Key, // The key in the visible_idx
		}
		receiptBytes, _ := json.Marshal(receiptData)
		tr.Set(inflightDir.Pack(tuple.Tuple{receiptHandle}), receiptBytes)

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
			for k := range responseMsg.MessageAttributes { attrKeys = append(attrKeys, k) }
			md5Bytes := md5.Sum(hashAttributes(responseMsg.MessageAttributes, attrKeys))
			md5Str := hex.EncodeToString(md5Bytes[:])
			responseMsg.MD5OfMessageAttributes = &md5Str
		}
		receivedMessages = append(receivedMessages, responseMsg)
	}

	return receivedMessages, nil
}

func (s *FDBStore) receiveFifoMessages(tr fdb.Transaction, queueDir directory.DirectorySubspace, req *models.ReceiveMessageRequest, maxMessages int, visibilityTimeout int) ([]models.ResponseMessage, error) {
	messagesDir := queueDir.Sub("messages")
	fifoIdxDir := queueDir.Sub("fifo_idx")
	inflightDir := queueDir.Sub("inflight_groups")
	receiveAttemptsDir := queueDir.Sub("receive_attempts")
	now := time.Now().Unix()

	// 1. Handle ReceiveRequestAttemptId deduplication
	if req.ReceiveRequestAttemptId != "" {
		attemptKey := receiveAttemptsDir.Pack(tuple.Tuple{req.ReceiveRequestAttemptId})
		val, err := tr.Get(attemptKey).Get()
		if err != nil { return nil, err }
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
	var processedGroups = make(map[string]bool)

	// 2. Find an available message group
	pr, _ := fdb.PrefixRange(fifoIdxDir.Pack(tuple.Tuple{}))
	iter := tr.GetRange(pr, fdb.RangeOptions{}).Iterator()

	for iter.Advance() {
		kv := iter.MustGet()
		t, err := fifoIdxDir.Unpack(kv.Key)
		if err != nil { continue }

		groupID := t[0].(string)
		if processedGroups[groupID] { continue } // Already checked this group

		// Check if the group is locked
		lockKey := inflightDir.Pack(tuple.Tuple{groupID})
		lockVal, err := tr.Get(lockKey).Get()
		if err != nil { return nil, err }

		if lockVal != nil {
			lockExpiry, err := strconv.ParseInt(string(lockVal), 10, 64)
			if err == nil && now < lockExpiry {
				processedGroups[groupID] = true
				continue // Group is locked, try next message
			}
		}

		// This group is available, so we'll process it.
		targetGroupID = groupID
		break
	}

	// 3. If no available group was found, return empty
	if targetGroupID == "" {
		return []models.ResponseMessage{}, nil
	}

	// 4. Retrieve messages from the chosen group
	prefixTuple := tuple.Tuple{targetGroupID}
	pr, err := fdb.PrefixRange(fifoIdxDir.Pack(prefixTuple))
	if err != nil { return nil, err }
	r := tr.GetRange(pr, fdb.RangeOptions{Limit: maxMessages})

	iter = r.Iterator()
	for iter.Advance() {
		kv := iter.MustGet()
		messageID := string(kv.Value)

		msgBytes, err := tr.Get(messagesDir.Pack(tuple.Tuple{messageID})).Get()
		if err != nil || msgBytes == nil { continue }
		var msg models.Message
		if err := json.Unmarshal(msgBytes, &msg); err != nil { continue }

		receiptHandle := uuid.New().String()
		receiptData := map[string]interface{}{
			"id":       msg.ID,
			"fifo_key": kv.Key, // The key in the fifo_idx
		}
		receiptBytes, _ := json.Marshal(receiptData)
		// Use a different inflight dir for fifo receipts to avoid clashes
		tr.Set(queueDir.Sub("inflight").Pack(tuple.Tuple{receiptHandle}), receiptBytes)

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
			for k := range responseMsg.MessageAttributes { attrKeys = append(attrKeys, k) }
			md5Bytes := md5.Sum(hashAttributes(responseMsg.MessageAttributes, attrKeys))
			md5Str := hex.EncodeToString(md5Bytes[:])
			responseMsg.MD5OfMessageAttributes = &md5Str
		}
		receivedMessages = append(receivedMessages, responseMsg)
	}

	// 5. If we got messages, lock the group and store the deduplication record
	if len(receivedMessages) > 0 {
		// Lock the group
		lockExpiry := now + int64(visibilityTimeout)
		tr.Set(inflightDir.Pack(tuple.Tuple{targetGroupID}), []byte(strconv.FormatInt(lockExpiry, 10)))

		// Store the result for deduplication
		if req.ReceiveRequestAttemptId != "" {
			attemptKey := receiveAttemptsDir.Pack(tuple.Tuple{req.ReceiveRequestAttemptId})
			attemptData := struct {
				Messages  []models.ResponseMessage `json:"messages"`
				Timestamp int64                    `json:"timestamp"`
			}{ Messages: receivedMessages, Timestamp: now }
			attemptBytes, _ := json.Marshal(attemptData)
			tr.Set(attemptKey, attemptBytes)
		}
	}

	return receivedMessages, nil
}


func (s *FDBStore) ReceiveMessage(ctx context.Context, queueName string, req *models.ReceiveMessageRequest) (*models.ReceiveMessageResponse, error) {
	maxMessages := 1
	if req.MaxNumberOfMessages > 0 { maxMessages = req.MaxNumberOfMessages }
	waitTime := 0
	if req.WaitTimeSeconds > 0 { waitTime = req.WaitTimeSeconds }

	isFifo := strings.HasSuffix(queueName, ".fifo")
	startTime := time.Now()

	for {
		rawMessages, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			exists, err := s.dir.Exists(tr, []string{queueName})
			if err != nil { return nil, err }
			if !exists { return nil, ErrQueueDoesNotExist }

			queueDir, err := s.dir.Open(tr, []string{queueName}, nil)
			if err != nil { return nil, err }

			// Get queue's default visibility timeout
			var queueAttributes map[string]string
			attrsBytes, err := tr.Get(queueDir.Pack(tuple.Tuple{"attributes"})).Get()
			if err != nil { return nil, err }
			if len(attrsBytes) > 0 { json.Unmarshal(attrsBytes, &queueAttributes) }

			visibilityTimeout := 30 // SQS default
			if vtStr, ok := queueAttributes["VisibilityTimeout"]; ok {
				if vt, err := strconv.Atoi(vtStr); err == nil { visibilityTimeout = vt }
			}
			if req.VisibilityTimeout > 0 { visibilityTimeout = req.VisibilityTimeout }

			if isFifo {
				return s.receiveFifoMessages(tr, queueDir, req, maxMessages, visibilityTimeout)
			} else {
				return s.receiveStandardMessages(tr, queueDir, req, maxMessages, visibilityTimeout)
			}
		})

		if err != nil { return nil, err }

		var messages []models.ResponseMessage
		if rawMessages != nil {
			messages = rawMessages.([]models.ResponseMessage)
		}

		if len(messages) > 0 {
			return &models.ReceiveMessageResponse{Messages: messages}, nil
		}

		if time.Since(startTime).Seconds() >= float64(waitTime) {
			return &models.ReceiveMessageResponse{Messages: []models.ResponseMessage{}}, nil // Return empty, not nil
		}

		time.Sleep(100 * time.Millisecond) // Wait before retrying for long poll
	}
}

func (s *FDBStore) DeleteMessage(ctx context.Context, queueName string, receiptHandle string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		exists, err := s.dir.Exists(tr, []string{queueName})
		if err != nil { return nil, err }
		if !exists { return nil, ErrQueueDoesNotExist }

		queueDir, err := s.dir.Open(tr, []string{queueName}, nil)
		if err != nil { return nil, err }

		messagesDir := queueDir.Sub("messages")
		inflightDir := queueDir.Sub("inflight")

		// 1. Find the receipt handle in the inflight records
		inflightKey := inflightDir.Pack(tuple.Tuple{receiptHandle})
		receiptBytes, err := tr.Get(inflightKey).Get()
		if err != nil {
			return nil, err
		}
		// If the receipt handle doesn't exist, the message is already considered
		// deleted or was never in-flight. SQS considers this a successful deletion.
		if receiptBytes == nil {
			return nil, nil
		}

		var receiptData map[string]interface{}
		if err := json.Unmarshal(receiptBytes, &receiptData); err != nil {
			// If the handle is corrupt, it's an invalid handle.
			return nil, ErrInvalidReceiptHandle
		}

		messageID := receiptData["id"].(string)

		// 2. Delete the primary message
		tr.Clear(messagesDir.Pack(tuple.Tuple{messageID}))

		// 3. Delete the message from the correct index
		if fifoKeyStr, ok := receiptData["fifo_key"].(string); ok {
			// It's a FIFO message, delete from fifo_idx
			fifoKey, err := base64.StdEncoding.DecodeString(fifoKeyStr)
			if err != nil { return nil, errors.New("invalid receipt handle: corrupt fifo_key") }
			tr.Clear(fdb.Key(fifoKey))
		} else if visKeyStr, ok := receiptData["vis_key"].(string); ok {
			// It's a Standard message, delete from visible_idx
			visKey, err := base64.StdEncoding.DecodeString(visKeyStr)
			if err != nil { return nil, errors.New("invalid receipt handle: corrupt vis_key") }
			tr.Clear(fdb.Key(visKey))
		}

		// 4. Delete the inflight receipt handle
		tr.Clear(inflightKey)

		return nil, nil
	})
	return err
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
