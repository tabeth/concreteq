package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

type MockTransactor struct {
	TransactFunc     func(func(fdb.Transaction) (interface{}, error)) (interface{}, error)
	ReadTransactFunc func(func(fdb.ReadTransaction) (interface{}, error)) (interface{}, error)
}

func (m *MockTransactor) Transact(f func(fdb.Transaction) (interface{}, error)) (interface{}, error) {
	return m.TransactFunc(f)
}

func (m *MockTransactor) ReadTransact(f func(fdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
	return m.ReadTransactFunc(f)
}

func TestStore_TransactErrors(t *testing.T) {
	mockErr := errors.New("simulated error")
	mock := &MockTransactor{
		TransactFunc: func(f func(fdb.Transaction) (interface{}, error)) (interface{}, error) {
			return nil, mockErr
		},
		ReadTransactFunc: func(f func(fdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
			return nil, mockErr
		},
	}

	s := &Store{
		db: mock,
	}

	ctx := context.Background()

	// CreateTopic
	_, err := s.CreateTopic(ctx, "fail", nil)
	if err != mockErr {
		t.Errorf("CreateTopic: expected mock error, got %v", err)
	}

	// GetTopic
	_, err = s.GetTopic(ctx, "arn:fail")
	if err != mockErr {
		t.Errorf("GetTopic: expected mock error, got %v", err)
	}

	// Subscribe
	sub := &models.Subscription{TopicArn: "arn:t", Protocol: "p", Endpoint: "e"}
	_, err = s.Subscribe(ctx, sub)
	if err != mockErr {
		t.Errorf("Subscribe: expected mock error, got %v", err)
	}

	// PublishMessage
	msg := &models.Message{TopicArn: "arn:t", Message: "m"}
	err = s.PublishMessage(ctx, msg)
	if err != mockErr {
		t.Errorf("PublishMessage: expected mock error, got %v", err)
	}

	// PollDeliveryTasks
	_, err = s.PollDeliveryTasks(ctx, 1)
	if err != mockErr {
		t.Errorf("PollDeliveryTasks: expected mock error, got %v", err)
	}

	// DeleteDeliveryTask
	task := &models.DeliveryTask{TaskID: "id", VisibleAfter: time.Now()}
	err = s.DeleteDeliveryTask(ctx, task)
	if err != mockErr {
		t.Errorf("DeleteDeliveryTask: expected mock error, got %v", err)
	}

	// GetSubscription
	_, err = s.GetSubscription(ctx, "arn:s")
	if err != mockErr {
		t.Errorf("GetSubscription: expected mock error, got %v", err)
	}

	// GetMessage
	_, err = s.GetMessage(ctx, "arn:t", "id")
	if err != mockErr {
		t.Errorf("GetMessage: expected mock error, got %v", err)
	}

	// ListSubscriptionsByTopic
	_, err = s.ListSubscriptionsByTopic(ctx, "arn:t")
	if err != mockErr {
		t.Errorf("ListSubscriptionsByTopic: expected mock error, got %v", err)
	}
}

func TestStore_DataCorruption(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. Corrupt Topic
	topicArn := "arn:concretens:topic:corrupt"
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.topicDir.Pack(tuple.Tuple{topicArn})
		tr.Set(key, []byte("{invalid-json"))
		return nil, nil
	})
	if _, err := s.GetTopic(ctx, topicArn); err == nil {
		t.Error("Expected error for corrupt topic")
	}

	// 2. Corrupt Subscription
	subArn := "arn:concretens:sub:corrupt"
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.subDir.Pack(tuple.Tuple{subArn})
		tr.Set(key, []byte("{invalid-json"))
		return nil, nil
	})
	if _, err := s.GetSubscription(ctx, subArn); err == nil {
		t.Error("Expected error for corrupt subscription")
	}

	// 3. Corrupt Message
	msgID := "corrupt-msg"
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.topicDir.Pack(tuple.Tuple{"messages", topicArn, msgID})
		tr.Set(key, []byte("{invalid-json"))
		return nil, nil
	})
	if _, err := s.GetMessage(ctx, topicArn, msgID); err == nil {
		t.Error("Expected error for corrupt message")
	}

	// 4. Corrupt Delivery Task
	taskID := "corrupt-task"
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.subDir.Pack(tuple.Tuple{"queue", 0, taskID})
		tr.Set(key, []byte("{invalid-json"))
		return nil, nil
	})

	tasks, err := s.PollDeliveryTasks(ctx, 10)
	if err != nil {
		t.Errorf("PollDeliveryTasks failed: %v", err)
	}
	for _, task := range tasks {
		if task.TaskID == taskID {
			t.Error("Should not return corrupt task")
		}
	}

	// 5. ListSubscriptionsByTopic Corrupt Index Target
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		idxKey := s.topicDir.Pack(tuple.Tuple{"subs", topicArn, subArn})
		tr.Set(idxKey, []byte{})
		return nil, nil
	})

	subs, err := s.ListSubscriptionsByTopic(ctx, topicArn)
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic failed: %v", err)
	}
	if len(subs) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(subs))
	}

	// 6. Index Pointing to Missing Subscription (Orphan)
	// Create orphan index entry
	orphanSubArn := "arn:sub:orphan"
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		idxKey := s.topicDir.Pack(tuple.Tuple{"subs", topicArn, orphanSubArn})
		tr.Set(idxKey, []byte{})
		return nil, nil
	})
	subs, _ = s.ListSubscriptionsByTopic(ctx, topicArn)
	// Iterate to check if orphan is returned
	for _, sub := range subs {
		if sub.SubscriptionArn == orphanSubArn {
			t.Error("Should not return orphan subscription")
		}
	}

	// 7. Malformed Index Key
	// Insert key that matches prefix but unpacks strangely?
	// s.topicDir.Pack("subs", topicArn) is the prefix.
	// We iterate keys starting with that.
	// If we insert "subs", topicArn -> value.
	// Unpack -> ("subs", topicArn). Len 2.
	s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := s.topicDir.Pack(tuple.Tuple{"subs", topicArn})
		tr.Set(key, []byte{})
		return nil, nil
	})
	subs, _ = s.ListSubscriptionsByTopic(ctx, topicArn)
	if len(subs) != 0 {
		t.Error("Expected 0 subscriptions from malformed key")
	}
}

func TestStore_NewStore_Error(t *testing.T) {
	// Mock OpenDB to fail
	orig := openDBFunc
	defer func() { openDBFunc = orig }()

	openDBFunc = func(apiVersion int) (fdb.Database, error) {
		return fdb.Database{}, errors.New("open db error")
	}

	_, err := NewStore(710)
	if err == nil {
		t.Error("Expected error from NewStore")
	}
}
