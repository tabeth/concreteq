package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/filter"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	libfdb "github.com/tabeth/kiroku-core/libs/fdb"
)

// Transactor abstracts fdb.Database for testing
type Transactor interface {
	Transact(func(fdb.Transaction) (interface{}, error)) (interface{}, error)
	ReadTransact(func(fdb.ReadTransaction) (interface{}, error)) (interface{}, error)
}

// RealTransactor wraps fdb.Database
type RealTransactor struct {
	db fdb.Database
}

func (t *RealTransactor) Transact(f func(fdb.Transaction) (interface{}, error)) (interface{}, error) {
	return t.db.Transact(f)
}

func (t *RealTransactor) ReadTransact(f func(fdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
	return t.db.ReadTransact(f)
}

type Store struct {
	db       Transactor
	topicDir directory.DirectorySubspace
	subDir   directory.DirectorySubspace
	dedupDir directory.DirectorySubspace // Subspace for deduplication
}

// openDBFunc is a variable to allow mocking in tests
var openDBFunc = libfdb.OpenDB

func NewStore(apiVersion int) (*Store, error) {
	db, err := openDBFunc(apiVersion)
	if err != nil {
		return nil, err
	}

	// Create or open directories for topics and subscriptions
	topicDir, err := directory.CreateOrOpen(db, []string{"concretens", "topics"}, nil)
	if err != nil {
		return nil, err
	}

	subDir, err := directory.CreateOrOpen(db, []string{"concretens", "subscriptions"}, nil)
	if err != nil {
		return nil, err
	}
	dedupDir, err := directory.CreateOrOpen(db, []string{"concretens", "dedup"}, nil)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:       &RealTransactor{db: db},
		topicDir: topicDir,
		subDir:   subDir,
		dedupDir: dedupDir,
	}, nil
}

func (s *Store) CreateTopic(ctx context.Context, name string, attributes map[string]string) (*models.Topic, error) {
	// 1. Validation for FIFO
	isFifo := false
	if strings.HasSuffix(name, ".fifo") {
		isFifo = true
		if attributes == nil {
			attributes = make(map[string]string)
		}
		attributes["FifoTopic"] = "true"
	}
	if attributes != nil && attributes["FifoTopic"] == "true" && !strings.HasSuffix(name, ".fifo") {
		return nil, fmt.Errorf("FIFO topics must end with .fifo")
	}

	// 2. ID Generation
	topicArn := fmt.Sprintf("arn:concretens:topic:%s", name)

	topic := &models.Topic{
		TopicArn:    topicArn,
		Name:        name,
		Attributes:  attributes,
		CreatedTime: time.Now(),
		FifoTopic:   isFifo,
	}

	// ... (Transaction)
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Check exists
		key := s.topicDir.Pack(tuple.Tuple{topicArn})
		existing, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if len(existing) > 0 {
			// Idempotent return if same?
			return nil, nil // Or return existing?
		}

		data, err := json.Marshal(topic)
		if err != nil {
			return nil, err
		}
		tr.Set(key, data)
		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	return topic, nil
}

func (s *Store) GetTopicAttributes(ctx context.Context, topicArn string) (map[string]string, error) {
	topic, err := s.GetTopic(ctx, topicArn)
	if err != nil {
		return nil, err
	}
	// Return copy of attributes
	attrs := make(map[string]string)
	for k, v := range topic.Attributes {
		attrs[k] = v
	}
	return attrs, nil
}

func (s *Store) SetTopicAttributes(ctx context.Context, topicArn string, attributes map[string]string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.topicDir.Pack(tuple.Tuple{topicArn})
		data, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("topic not found")
		}

		var topic models.Topic
		if err := json.Unmarshal(data, &topic); err != nil {
			return nil, err
		}

		// Update attributes
		if topic.Attributes == nil {
			topic.Attributes = make(map[string]string)
		}
		for k, v := range attributes {
			topic.Attributes[k] = v
		}

		updatedData, err := json.Marshal(topic)
		if err != nil {
			return nil, err
		}
		tr.Set(key, updatedData)
		return nil, nil
	})
	return err
}

func (s *Store) GetTopic(ctx context.Context, topicArn string) (*models.Topic, error) {
	result, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.topicDir.Pack(tuple.Tuple{topicArn})
		return tr.Get(key).Get()
	})
	if err != nil {
		return nil, err
	}

	data := result.([]byte)
	if data == nil {
		return nil, fmt.Errorf("topic not found: %s", topicArn)
	}

	var topic models.Topic
	if err := json.Unmarshal(data, &topic); err != nil {
		return nil, err
	}

	return &topic, nil
}

func (s *Store) Subscribe(ctx context.Context, sub *models.Subscription) (*models.Subscription, error) {
	// 1. Validate Topic Exists
	topic, err := s.GetTopic(ctx, sub.TopicArn)
	if err != nil {
		return nil, err
	}

	// 2. FIFO Restriction
	if topic.FifoTopic && sub.Protocol != "sqs" {
		return nil, fmt.Errorf("FIFO topics only support sqs protocol")
	}

	// 3. ID Generation
	subID := uuid.New().String()
	sub.SubscriptionArn = fmt.Sprintf("%s:%s", sub.TopicArn, subID)

	// Default Status for non-http
	if sub.Protocol == "http" || sub.Protocol == "https" {
		sub.Status = "PendingConfirmation"
		sub.ConfirmationToken = uuid.New().String()
	} else {
		sub.Status = "Active"
	}

	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.subDir.Pack(tuple.Tuple{sub.SubscriptionArn})
		data, err := json.Marshal(sub)
		if err != nil {
			return nil, err
		}
		tr.Set(key, data)

		// Index by Topic: topicDir + topicArn + "subs" + subArn
		idxKey := s.topicDir.Pack(tuple.Tuple{"subs", sub.TopicArn, sub.SubscriptionArn})
		tr.Set(idxKey, []byte{})

		// If pending confirmation, enqueue a confirmation task
		if sub.Status == "PendingConfirmation" {
			task := models.DeliveryTask{
				TaskID:          uuid.New().String(),
				SubscriptionArn: sub.SubscriptionArn,
				MessageID:       "CONFIRMATION_REQUEST", // Special ID or similar mechanism
				VisibleAfter:    time.Now().Add(-1 * time.Millisecond),
				RetryCount:      0,
			}
			taskData, _ := json.Marshal(task)
			taskKey := s.subDir.Pack(tuple.Tuple{"queue", task.VisibleAfter.UnixNano(), task.TaskID})
			tr.Set(taskKey, taskData)
		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}
	return sub, nil
}

// ConfirmSubscription activates a pending subscription if the token matches.
func (s *Store) ConfirmSubscription(ctx context.Context, topicArn, token string) (*models.Subscription, error) {
	// We need to find the subscription by Token.
	// Since we don't index by Token, we might scan subscriptions for the topic.
	// This is inefficient for huge topics but acceptable for now.
	// A better way would be `s.topicDir.Pack(tuple.Tuple{"tokens", token}) -> subArn` index.
	// For now, let's iterate subs of the topic.

	sub, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		subs, err := s.ListSubscriptionsByTopic(ctx, topicArn)
		if err != nil {
			return nil, err
		}
		for _, sub := range subs {
			if sub.ConfirmationToken == token && sub.Status == "PendingConfirmation" {
				return sub, nil
			}
		}
		return nil, fmt.Errorf("invalid token or subscription not pending")
	})

	if err != nil {
		return nil, err
	}

	foundSub := sub.(*models.Subscription)

	// Update status to Active
	foundSub.Status = "Active"
	// foundSub.ConfirmationToken = "" // Optional: clear token

	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		data, err := json.Marshal(foundSub)
		if err != nil {
			return nil, err
		}
		subKey := s.subDir.Pack(tuple.Tuple{foundSub.SubscriptionArn})
		tr.Set(subKey, data)
		return nil, nil
	})

	return foundSub, err
}

func (s *Store) ListSubscriptionsByTopic(ctx context.Context, topicArn string) ([]*models.Subscription, error) {
	subs, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		// Range scan on index: topicDir.Pack("subs", topicArn)
		prefix := s.topicDir.Pack(tuple.Tuple{"subs", topicArn})
		r, err := fdb.PrefixRange(prefix)
		if err != nil {
			return nil, err
		}

		iter := rtr.GetRange(r, fdb.RangeOptions{}).Iterator()
		var subscriptions []*models.Subscription

		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			// Key structure: ... + "subs" + topicArn + subscriptionArn
			t, err := s.topicDir.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}
			// t should be ("subs", topicArn, subscriptionArn)
			if len(t) < 3 {
				continue
			}
			subscriptionArn := t[2].(string)

			// Fetch the actual subscription
			subKey := s.subDir.Pack(tuple.Tuple{subscriptionArn})
			subData := rtr.Get(subKey).MustGet()

			if len(subData) > 0 {
				var sub models.Subscription
				if err := json.Unmarshal(subData, &sub); err == nil {
					subscriptions = append(subscriptions, &sub)
				}
			}
		}
		return subscriptions, nil
	})

	if err != nil {
		return nil, err
	}
	return subs.([]*models.Subscription), nil
}

func (s *Store) PublishMessage(ctx context.Context, msg *models.Message) error {
	// 1. Validate Topic
	topic, err := s.GetTopic(ctx, msg.TopicArn)
	if err != nil {
		return err
	}

	// 2. FIFO Logic
	if topic.FifoTopic {
		if msg.MessageGroupId == "" {
			return fmt.Errorf("MessageGroupId is required for FIFO topics")
		}
		// Deduplication
		if msg.MessageDeduplicationId == "" {
			// Simple hash of body
			h := sha256.Sum256([]byte(msg.Message))
			msg.MessageDeduplicationId = fmt.Sprintf("%x", h)
		}

		// Check Dedup Store
		dedupKey := s.dedupDir.Pack(tuple.Tuple{msg.TopicArn, msg.MessageDeduplicationId})

		isDuplicate, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			existing, err := tr.Get(dedupKey).Get()
			if err != nil {
				return false, err
			}
			if len(existing) > 0 {
				return true, nil // Duplicate
			}
			tr.Set(dedupKey, []byte(time.Now().Format(time.RFC3339)))
			return false, nil
		})
		if err != nil {
			return err
		}
		if isDuplicate.(bool) {
			return nil
		}
	}

	if msg.MessageID == "" {
		msg.MessageID = uuid.New().String()
	}
	if msg.PublishedTime.IsZero() {
		msg.PublishedTime = time.Now()
	}

	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Save Message
		msgKey := s.topicDir.Pack(tuple.Tuple{"messages", msg.TopicArn, msg.MessageID})
		data, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		tr.Set(msgKey, data)

		// Fanout to Subscriptions
		// ... (Same as before)
		subs, err := s.ListSubscriptionsByTopic(ctx, msg.TopicArn)
		if err != nil {
			return nil, err
		}

		for _, sub := range subs {
			// Check Status
			if sub.Status != "Active" {
				continue
			}

			// Check Filter Policy
			if sub.FilterPolicy != "" {
				matches, err := filter.Matches(sub.FilterPolicy, msg.MessageAttributes)
				if err != nil {
					// Log error? Skip?
					continue
				}
				if !matches {
					continue
				}
			}

			task := models.DeliveryTask{
				TaskID:          uuid.New().String(),
				SubscriptionArn: sub.SubscriptionArn,
				MessageID:       msg.MessageID,
				VisibleAfter:    time.Now(),
				RetryCount:      0,
			}
			taskData, _ := json.Marshal(task)
			// Queue Key: queue + Timestamp + TaskID
			queueKey := s.subDir.Pack(tuple.Tuple{"queue", task.VisibleAfter.UnixNano(), task.TaskID})
			tr.Set(queueKey, taskData)
		}

		return nil, nil
	})
	return err
}

// PollDeliveryTasks fetches tasks that are ready for delivery and locks them (extends visibility).
func (s *Store) PollDeliveryTasks(ctx context.Context, limit int) ([]*models.DeliveryTask, error) {
	tasks, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		now := time.Now().UnixNano()

		startKey := s.subDir.Pack(tuple.Tuple{"queue", 0})
		endKey := s.subDir.Pack(tuple.Tuple{"queue", now + 1})

		r := fdb.KeyRange{Begin: startKey, End: endKey}
		iter := tr.GetRange(r, fdb.RangeOptions{Limit: limit}).Iterator()

		var claimedTasks []*models.DeliveryTask
		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			// 1. Decode task
			var task models.DeliveryTask
			if err := json.Unmarshal(kv.Value, &task); err != nil {
				tr.Clear(kv.Key)
				continue
			}

			// 2. Delete old key
			tr.Clear(kv.Key)

			// 3. Update visibility and re-insert
			task.VisibleAfter = time.Now().Add(30 * time.Second)
			task.RetryCount++

			newData, err := json.Marshal(task)
			if err != nil {
				return nil, err
			}

			newKey := s.subDir.Pack(tuple.Tuple{"queue", task.VisibleAfter.UnixNano(), task.TaskID})
			tr.Set(newKey, newData)

			claimedTasks = append(claimedTasks, &task)
		}
		return claimedTasks, nil
	})

	if err != nil {
		return nil, err
	}
	return tasks.([]*models.DeliveryTask), nil
}

// DeleteDeliveryTask removes a task from the queue (ack).
func (s *Store) DeleteDeliveryTask(ctx context.Context, task *models.DeliveryTask) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Key: "queue", VisibleAfter, TaskID
		key := s.subDir.Pack(tuple.Tuple{"queue", task.VisibleAfter.UnixNano(), task.TaskID})
		tr.Clear(key)
		return nil, nil
	})
	return err
}

// RescheduleDeliveryTask updates a task's visibility and retry count.
func (s *Store) RescheduleDeliveryTask(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
	task.RetryCount++
	task.VisibleAfter = nextVisible

	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Remove old task key
		// Note from Poll logic: The key is (visibleAfter, taskID).
		// We can't easily find the OLD visibleAfter unless we passed it or looked it up.
		// However, we are holding the task object which SHOULD have the old VisibleAfter if we haven't mutated it yet.
		// BUT: Dispatcher might have mutated it or not?
		// Wait, FDB queue pattern usually pops the item or locks it.
		// In PollDeliveryTasks, we didn't remove it, we just read it?
		// Checking PollDeliveryTasks: It does NOT delete or move. It just reads.
		// This means multiple workers could pick it up if we don't lock it.
		// Usually for simple queues we assume one-time delivery or lock.
		// Let's assume for this "simple" dispatcher, we need to delete the OLD key and add NEW key.
		// The caller must provide the *current* task state which matches DB.

		// We will assume the passed 'task' struct has the exact VisibleAfter that is in the DB key.

		oldKey := s.subDir.Pack(tuple.Tuple{"queue", task.VisibleAfter.UnixNano(), task.TaskID})
		tr.Clear(oldKey)

		// 2. Add new key
		newTask := *task // Shallow copy
		newTask.VisibleAfter = nextVisible
		newData, _ := json.Marshal(newTask)

		newKey := s.subDir.Pack(tuple.Tuple{"queue", newTask.VisibleAfter.UnixNano(), newTask.TaskID})
		tr.Set(newKey, newData)

		return nil, nil
	})

	return err
}

// MoveToDLQ moves a task from the main queue to a Dead Letter Queue.
func (s *Store) MoveToDLQ(ctx context.Context, task *models.DeliveryTask) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Delete from main queue
		key := s.subDir.Pack(tuple.Tuple{"queue", task.VisibleAfter.UnixNano(), task.TaskID})
		tr.Clear(key)

		// 2. Add to DLQ: subDir + "dlq" + taskID
		// We don't really need order in DLQ, usually just storage.
		dlqKey := s.subDir.Pack(tuple.Tuple{"dlq", task.TaskID})

		data, _ := json.Marshal(task)
		tr.Set(dlqKey, data)

		return nil, nil
	})
	return err
}

// ClearQueue removes all tasks from the queue. Intended for testing/admin.
func (s *Store) ClearQueue(ctx context.Context) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		queuePrefix := s.subDir.Pack(tuple.Tuple{"queue"})
		rng, err := fdb.PrefixRange(queuePrefix)
		if err != nil {
			return nil, err
		}
		tr.ClearRange(rng)
		return nil, nil
	})
	return err
}

func (s *Store) GetSubscription(ctx context.Context, subscriptionArn string) (*models.Subscription, error) {
	sub, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		subKey := s.subDir.Pack(tuple.Tuple{subscriptionArn})
		data, err := rtr.Get(subKey).Get()
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("subscription not found")
		}
		var s models.Subscription
		if err := json.Unmarshal(data, &s); err != nil {
			return nil, err
		}
		return &s, nil
	})
	if err != nil {
		return nil, err
	}
	return sub.(*models.Subscription), nil
}

func (s *Store) GetSubscriptionAttributes(ctx context.Context, subArn string) (map[string]string, error) {
	sub, err := s.GetSubscription(ctx, subArn)
	if err != nil {
		return nil, err
	}
	// Copy attributes
	attrs := make(map[string]string)
	for k, v := range sub.Attributes {
		attrs[k] = v
	}
	// Add System Attributes
	attrs["ConfirmationWasAuthenticated"] = "true" // Mock
	attrs["DeliveryPolicy"] = "{}"                 // Mock
	attrs["EffectiveDeliveryPolicy"] = "{}"        // Mock

	// Add RawDelivery flag as attribute for parity?
	if sub.RawDelivery {
		attrs["RawMessageDelivery"] = "true"
	} else {
		attrs["RawMessageDelivery"] = "false"
	}

	return attrs, nil
}

func (s *Store) SetSubscriptionAttributes(ctx context.Context, subArn string, attributes map[string]string) error {
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		subKey := s.subDir.Pack(tuple.Tuple{subArn})
		data, err := tr.Get(subKey).Get()
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("subscription not found")
		}

		var sub models.Subscription
		if err := json.Unmarshal(data, &sub); err != nil {
			return nil, err
		}

		if sub.Attributes == nil {
			sub.Attributes = make(map[string]string)
		}

		for k, v := range attributes {
			// Handle special attributes
			if k == "RawMessageDelivery" {
				sub.RawDelivery = (v == "true")
			}
			sub.Attributes[k] = v
		}

		updatedData, err := json.Marshal(sub)
		if err != nil {
			return nil, err
		}
		tr.Set(subKey, updatedData)
		return nil, nil
	})
	return err
}

func (s *Store) GetMessage(ctx context.Context, topicArn, messageID string) (*models.Message, error) {
	msg, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		key := s.topicDir.Pack(tuple.Tuple{"messages", topicArn, messageID})
		data, err := rtr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, fmt.Errorf("message not found")
		}
		var m models.Message
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		return &m, nil
	})
	if err != nil {
		return nil, err
	}
	return msg.(*models.Message), nil

}

// DeleteTopic deletes a topic and all its subscriptions.
func (s *Store) DeleteTopic(ctx context.Context, topicArn string) error {
	// 1. Fetch all subscriptions for this topic to delete them
	subs, err := s.ListSubscriptionsByTopic(ctx, topicArn)
	if err != nil {
		return err
	}

	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 2. Delete all subscriptions and their indices
		for _, sub := range subs {
			// Delete subscription data
			subKey := s.subDir.Pack(tuple.Tuple{sub.SubscriptionArn})
			tr.Clear(subKey)

			// Delete index topic->sub
			idxKey := s.topicDir.Pack(tuple.Tuple{"subs", sub.TopicArn, sub.SubscriptionArn})
			tr.Clear(idxKey)
		}

		// 3. Delete Topic
		key := s.topicDir.Pack(tuple.Tuple{topicArn})
		tr.Clear(key)

		// 4. Delete Messages? (Optional, SNS eventually deletes them)
		// For now, let's clear the messages range for this topic.
		// Key: "messages", topicArn, msgId
		msgPrefix := s.topicDir.Pack(tuple.Tuple{"messages", topicArn})
		rng, err := fdb.PrefixRange(msgPrefix)
		if err == nil {
			tr.ClearRange(rng)
		}

		return nil, nil
	})
	return err
}

// DeleteSubscription deletes a subscription.
func (s *Store) DeleteSubscription(ctx context.Context, subscriptionArn string) error {
	// We need the topicArn to delete the index.
	sub, err := s.GetSubscription(ctx, subscriptionArn)
	if err != nil {
		// If not found, consider it done (idempotent)
		return nil
	}

	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Delete subscription data
		subKey := s.subDir.Pack(tuple.Tuple{subscriptionArn})
		tr.Clear(subKey)

		// 2. Delete index
		idxKey := s.topicDir.Pack(tuple.Tuple{"subs", sub.TopicArn, subscriptionArn})
		tr.Clear(idxKey)
		return nil, nil
	})
	return err
}

// ListTopics lists topics. For now, it returns all (no pagination).
func (s *Store) ListTopics(ctx context.Context) ([]*models.Topic, error) {
	topics, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		r, err := fdb.PrefixRange(s.topicDir.FDBKey())
		if err != nil {
			return nil, err
		}

		iter := rtr.GetRange(r, fdb.RangeOptions{}).Iterator()
		var result []*models.Topic

		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			t, err := s.topicDir.Unpack(kv.Key)
			if err != nil {
				continue
			}

			// Topic keys are just (topicArn). Len 1.
			if len(t) != 1 {
				continue
			}
			_, ok := t[0].(string)
			if !ok {
				continue
			}

			// Decode value
			var topic models.Topic
			if err := json.Unmarshal(kv.Value, &topic); err == nil {
				result = append(result, &topic)
			}
		}
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	return topics.([]*models.Topic), nil
}

// ListSubscriptions lists all subscriptions.
func (s *Store) ListSubscriptions(ctx context.Context) ([]*models.Subscription, error) {
	subs, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		r, err := fdb.PrefixRange(s.subDir.FDBKey())
		if err != nil {
			return nil, err
		}
		iter := rtr.GetRange(r, fdb.RangeOptions{}).Iterator()
		var result []*models.Subscription

		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			t, err := s.subDir.Unpack(kv.Key)
			if err != nil {
				continue
			}

			if len(t) != 1 {
				continue
			}

			// Decode
			var sub models.Subscription
			if err := json.Unmarshal(kv.Value, &sub); err == nil {
				result = append(result, &sub)
			}
		}
		return result, nil
	})
	if err != nil {
		return nil, err
	}
	return subs.([]*models.Subscription), nil
}
