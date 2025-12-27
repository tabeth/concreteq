package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestStore_CreateTopic_GetTopic(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	ctx := context.Background()
	topicName := "test-topic-1"
	attributes := map[string]string{"foo": "bar"}

	createdTopic, err := s.CreateTopic(ctx, topicName, attributes)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	if createdTopic.Name != topicName {
		t.Errorf("Expected topic name %s, got %s", topicName, createdTopic.Name)
	}
	if createdTopic.Attributes["foo"] != "bar" {
		t.Errorf("Expected attribute foo=bar, got %v", createdTopic.Attributes)
	}

	// Verify GetTopic
	fetchedTopic, err := s.GetTopic(ctx, createdTopic.TopicArn)
	if err != nil {
		t.Fatalf("GetTopic failed: %v", err)
	}

	if fetchedTopic.TopicArn != createdTopic.TopicArn {
		t.Errorf("Expected ARN %s, got %s", createdTopic.TopicArn, fetchedTopic.TopicArn)
	}
	if fetchedTopic.Name != createdTopic.Name {
		t.Errorf("Expected name %s, got %s", createdTopic.Name, fetchedTopic.Name)
	}

	// Verify GetTopic NotFound
	_, err = s.GetTopic(ctx, "arn:concretens:topic:non-existent")
	if err == nil {
		t.Error("Expected error for non-existent topic, got nil")
	}
}

func TestStore_Subscribe(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	ctx := context.Background()
	topicName := "test-topic-sub-" + uuid.New().String()
	topic, err := s.CreateTopic(ctx, topicName, nil)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	sub := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "http",
		Endpoint: "http://example.com",
		Owner:    "test-owner",
	}

	createdSub, err := s.Subscribe(ctx, sub)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if createdSub.SubscriptionArn == "" {
		t.Error("Expected SubscriptionArn to be generated")
	}
	if createdSub.TopicArn != topic.TopicArn {
		t.Errorf("Expected TopicArn %s, got %s", topic.TopicArn, createdSub.TopicArn)
	}

	// Verify ListSubscriptionsByTopic
	subs, err := s.ListSubscriptionsByTopic(ctx, topic.TopicArn)
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic failed: %v", err)
	}
	if len(subs) != 1 {
		t.Errorf("Expected 1 subscription, got %d", len(subs))
	} else {
		if subs[0].SubscriptionArn != createdSub.SubscriptionArn {
			t.Errorf("Expected subscription ARN %s, got %s", createdSub.SubscriptionArn, subs[0].SubscriptionArn)
		}
	}
}

func TestStore_PublishMessage(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	ctx := context.Background()
	topicName := "test-topic-pub"
	topic, err := s.CreateTopic(ctx, topicName, nil)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	msg := &models.Message{
		TopicArn: topic.TopicArn,
		Message:  "hello world",
	}

	err = s.PublishMessage(ctx, msg)
	if err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	if msg.MessageID == "" {
		t.Error("Expected MessageID to be generated")
	}
}

func TestStore_ListSubscriptions(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()
	topic, _ := s.CreateTopic(ctx, "test-list-subs-"+uuid.New().String(), nil)

	// Create 3 subs
	for i := 0; i < 3; i++ {
		sub := &models.Subscription{TopicArn: topic.TopicArn, Protocol: "http", Endpoint: fmt.Sprintf("http://%d", i)}
		s.Subscribe(ctx, sub)
	}

	subs, err := s.ListSubscriptionsByTopic(ctx, topic.TopicArn)
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic failed: %v", err)
	}
	if len(subs) != 3 {
		t.Errorf("Expected 3 subs, got %d", len(subs))
	}
}

func TestStore_QueueOperations(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	// Clean queue before test to avoid interference
	s.ClearQueue(context.Background())

	ctx := context.Background()
	topic, _ := s.CreateTopic(ctx, "test-queue-"+uuid.New().String(), nil)
	sub := &models.Subscription{TopicArn: topic.TopicArn, Protocol: "http", Endpoint: "http://q"}
	s.Subscribe(ctx, sub)

	// Publish message -> triggers fanout -> enqueues task
	msg := &models.Message{TopicArn: topic.TopicArn, Message: "m"}
	s.PublishMessage(ctx, msg)

	// Poll
	tasks, err := s.PollDeliveryTasks(ctx, 10)
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task, got %d", len(tasks))
	}

	task := tasks[0]
	if task.RetryCount != 1 { // Poll increments retry count
		t.Errorf("Expected retry count 1, got %d", task.RetryCount)
	}

	// Delete (Ack)
	if err := s.DeleteDeliveryTask(ctx, task); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Poll again -> should be empty
	tasks2, err := s.PollDeliveryTasks(ctx, 10)
	if err != nil {
		t.Fatalf("Poll 2 failed: %v", err)
	}
	if len(tasks2) != 0 {
		t.Errorf("Expected 0 tasks, got %d", len(tasks2))
	}
}

func TestStore_ManagementOperations(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	s.ClearQueue(context.Background())
	ctx := context.Background()

	// 1. Create Topics
	t1, _ := s.CreateTopic(ctx, "topic-mgmt-1", nil)
	t2, _ := s.CreateTopic(ctx, "topic-mgmt-2", nil)

	// 2. List Topics
	topics, err := s.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}
	found1, found2 := false, false
	for _, top := range topics {
		if top.TopicArn == t1.TopicArn {
			found1 = true
		}
		if top.TopicArn == t2.TopicArn {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Error("ListTopics did not return created topics")
	}

	// 3. Create Subscriptions
	sub1, _ := s.Subscribe(ctx, &models.Subscription{TopicArn: t1.TopicArn, Protocol: "http", Endpoint: "e1"})
	sub2, _ := s.Subscribe(ctx, &models.Subscription{TopicArn: t1.TopicArn, Protocol: "http", Endpoint: "e2"})

	// 4. List Subscriptions
	subs, err := s.ListSubscriptions(ctx)
	if err != nil {
		t.Fatalf("ListSubscriptions failed: %v", err)
	}
	foundSub1 := false
	for _, sub := range subs {
		if sub.SubscriptionArn == sub1.SubscriptionArn {
			foundSub1 = true
		}
	}
	if !foundSub1 {
		t.Error("ListSubscriptions did not return created subscription")
	}

	// 5. Delete Subscription
	if err := s.DeleteSubscription(ctx, sub2.SubscriptionArn); err != nil {
		t.Fatalf("DeleteSubscription failed: %v", err)
	}
	// Verify gone
	if _, err := s.GetSubscription(ctx, sub2.SubscriptionArn); err == nil {
		t.Error("Subscription should be gone")
	}

	// 6. Delete Topic
	if err := s.DeleteTopic(ctx, t1.TopicArn); err != nil {
		t.Fatalf("DeleteTopic failed: %v", err)
	}
	// Verify topic gone
	if _, err := s.GetTopic(ctx, t1.TopicArn); err == nil {
		t.Error("Topic should be gone")
	}
	// Verify sub1 (attached to t1) is also gone (cascade delete)
	if _, err := s.GetSubscription(ctx, sub1.SubscriptionArn); err == nil {
		t.Error("Subscription 1 should be gone after topic delete")
	}
}

func TestStore_FilterPolicy(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	s.ClearQueue(context.Background())
	ctx := context.Background()

	// 1. Create Topic
	topic, _ := s.CreateTopic(ctx, "filter-topic-"+uuid.New().String(), nil)

	// 2. Create Sub with Policy (Blue)
	subBlue := &models.Subscription{
		TopicArn:     topic.TopicArn,
		Protocol:     "sqs", // Use SQS to avoid PendingConfirmation
		Endpoint:     "e-blue",
		FilterPolicy: `{"color": ["blue"]}`,
	}
	s.Subscribe(ctx, subBlue)

	// 3. Create Sub with No Policy (All)
	subAll := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "sqs", // Use SQS to avoid PendingConfirmation
		Endpoint: "e-all",
	}
	s.Subscribe(ctx, subAll)

	// 4. Publish Message (Red) -> Should match subAll only
	msgRed := &models.Message{
		TopicArn:          topic.TopicArn,
		Message:           "red-msg",
		MessageAttributes: map[string]string{"color": "red"},
	}
	s.PublishMessage(ctx, msgRed)

	// 5. Publish Message (Blue) -> Should match subBlue AND subAll
	msgBlue := &models.Message{
		TopicArn:          topic.TopicArn,
		Message:           "blue-msg",
		MessageAttributes: map[string]string{"color": "blue"},
	}
	s.PublishMessage(ctx, msgBlue)

	// Poll
	tasks, err := s.PollDeliveryTasks(ctx, 100)
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	// We expect:
	// - 1 task for Red Msg (for subAll)
	// - 2 tasks for Blue Msg (for subBlue and subAll)
	// Total 3 tasks.
	if len(tasks) != 3 {
		t.Fatalf("Expected 3 tasks, got %d", len(tasks))
	}

	blueCount := 0
	allCount := 0

	for _, task := range tasks {
		sub, _ := s.GetSubscription(ctx, task.SubscriptionArn)
		if sub.Endpoint == "e-blue" {
			blueCount++
			// Verify it's the blue message
			msg, _ := s.GetMessage(ctx, topic.TopicArn, task.MessageID)
			if msg.Message != "blue-msg" {
				t.Errorf("Blue sub got wrong message: %s", msg.Message)
			}
		} else if sub.Endpoint == "e-all" {
			allCount++
		}
	}

	if blueCount != 1 {
		t.Errorf("Expected 1 task for blue sub, got %d. List: %v", blueCount, tasks)
	}
	if allCount != 2 {
		t.Errorf("Expected 2 all tasks, got %d", allCount)
	}
}

func TestStore_ConfirmSubscription(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	s.ClearQueue(context.Background())
	ctx := context.Background()

	// 1. Create Topic
	topic, _ := s.CreateTopic(ctx, "confirm-topic-"+uuid.New().String(), nil)

	// 2. Subscribe (HTTP -> PendingConfirmation)
	sub := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "http",
		Endpoint: "http://example.com",
	}
	createdSub, err := s.Subscribe(ctx, sub)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if createdSub.Status != "PendingConfirmation" {
		t.Errorf("Expected PendingConfirmation, got %s", createdSub.Status)
	}
	if createdSub.ConfirmationToken == "" {
		t.Fatal("Expected ConfirmationToken to be set")
	}

	// 2a. Publish Message - Should NOT create task because status is PendingConfirmation
	s.PublishMessage(ctx, &models.Message{TopicArn: topic.TopicArn, Message: "lost-msg"})
	tasksPre, _ := s.PollDeliveryTasks(ctx, 10)
	// Expect 1 task (Confirmation Request), but NOT the "lost-msg"
	foundMsg := false
	for _, t := range tasksPre {
		if t.MessageID != "CONFIRMATION_REQUEST" {
			foundMsg = true
			break
		}
	}
	if foundMsg {
		t.Error("Should not deliver message to PendingConfirmation sub")
	}

	// 3. Confirm with Invalid Token
	_, err = s.ConfirmSubscription(ctx, createdSub.TopicArn, "invalid-token")
	if err == nil {
		t.Error("Expected error for invalid token, got nil")
	}

	// 4. Confirm with Valid Token
	confirmedSub, err := s.ConfirmSubscription(ctx, createdSub.TopicArn, createdSub.ConfirmationToken)
	if err != nil {
		t.Fatalf("ConfirmSubscription failed: %v", err)
	}
	if confirmedSub.Status != "Active" {
		t.Errorf("Expected Active, got %s", confirmedSub.Status)
	}

	// 5. Verify it is updated in store
	fetchedSub, err := s.GetSubscription(ctx, createdSub.SubscriptionArn)
	if err != nil {
		t.Fatalf("GetSubscription failed: %v", err)
	}
	if fetchedSub.Status != "Active" {
		t.Errorf("Store has status %s, expected Active", fetchedSub.Status)
	}
}

func TestStore_RetryDLQ(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	s.ClearQueue(context.Background())
	ctx := context.Background()

	// 1. Create Topic & Sub
	topic, _ := s.CreateTopic(ctx, "retry-topic-"+uuid.New().String(), nil)
	sub := &models.Subscription{TopicArn: topic.TopicArn, Protocol: "sqs", Endpoint: "queue"}
	s.Subscribe(ctx, sub)

	// 2. Publish Message
	msg := &models.Message{TopicArn: topic.TopicArn, Message: "retry-me"}
	s.PublishMessage(ctx, msg)

	// Poll to get the task
	tasks, err := s.PollDeliveryTasks(ctx, 1)
	if err != nil || len(tasks) == 0 {
		t.Fatalf("Poll failed or no tasks: %v", err)
	}
	targetTask := tasks[0]

	// 3. Test RescheduleDeliveryTask
	nextVisible := time.Now().Add(1 * time.Hour) // Future
	err = s.RescheduleDeliveryTask(ctx, targetTask, nextVisible)
	if err != nil {
		t.Fatalf("RescheduleDeliveryTask failed: %v", err)
	}

	// Verify it's gone from immediate poll
	tasks, _ = s.PollDeliveryTasks(ctx, 10)
	if len(tasks) != 0 {
		t.Errorf("Expected 0 tasks after reschedule, got %d", len(tasks))
	}

	// Inspect the task attributes directly if needed, but testing visibility is good enough.

	// 4. Test MoveToDLQ
	// Use the rescheduled task (assume we tracked it)
	// We need to fetch it? No, we have the object, but we need to ensure MoveToDLQ works on the *stored* task.
	// MoveToDLQ implementation constructs the key from task.VisibleAfter and task.TaskID to delete.
	// We updated `targetTask` locally? NO. `RescheduleDeliveryTask` does NOT update the pointer passed to it in the caller?
	// Let's check RescheduleDeliveryTask implementation.
	// func (s *Store) RescheduleDeliveryTask(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error {
	//    task.RetryCount++
	//    task.VisibleAfter = nextVisible
	//    ...
	// }
	// It modifies the pointer! So `targetTask` IS updated conformant to the new state in DB (mostly).
	// So we can use `targetTask` to move it to DLQ.

	err = s.MoveToDLQ(ctx, targetTask)
	if err != nil {
		t.Fatalf("MoveToDLQ failed: %v", err)
	}

	// After move to DLQ, it should NOT be in the queue at `nextVisible`.
	// Since we can't easily check the DLQ, success return is our proxy for now.
}

func TestStore_PublishMessage_Defaults(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()
	topic, _ := s.CreateTopic(ctx, "defaults-topic-"+uuid.New().String(), nil)

	msg := &models.Message{TopicArn: topic.TopicArn, Message: "test"}
	err = s.PublishMessage(ctx, msg)
	if err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}

	// Verify properties were set on the struct (since it's a pointer)
	if msg.MessageID == "" {
		t.Error("MessageID was not set")
	}
	if msg.PublishedTime.IsZero() {
		t.Error("PublishedTime was not set")
	}

	// Verify persistence
	fetched, err := s.GetMessage(ctx, topic.TopicArn, msg.MessageID)
	if err != nil {
		t.Fatalf("GetMessage failed: %v", err)
	}
	if fetched.MessageID != msg.MessageID {
		t.Errorf("Expected MessageID %s, got %s", msg.MessageID, fetched.MessageID)
	}
	if fetched.PublishedTime.IsZero() {
		t.Error("Fetched PublishedTime is zero")
	}
}

func TestStore_BadFilterPolicy(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()
	topic, _ := s.CreateTopic(ctx, "bad-filter-topic-"+uuid.New().String(), nil)

	// Create sub with invalid JSON filter
	sub := &models.Subscription{
		TopicArn:     topic.TopicArn,
		Protocol:     "sqs",
		Endpoint:     "e-bad",
		FilterPolicy: "{invalid-json",
	}
	// Subscribe might succeed as it just stores it?
	// Or failure? If it fails, that's fine too as long as we know.
	// But we want to test PublishMessage handling it.
	_, err = s.Subscribe(ctx, sub)
	if err != nil {
		// If Subscribe validtes it, then we covered Subscribe validation path?
		// But we want PublishMessage path.
		// Let's see if Subscribe allows it.
	}

	// Publish message
	msg := &models.Message{TopicArn: topic.TopicArn, Message: "test"}
	err = s.PublishMessage(ctx, msg)
	if err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}
	// Should just skip the sub, no error returned to publisher
}

func TestStore_NotFound_Paths(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. GetMessage Not Found
	msg, err := s.GetMessage(ctx, "arn:concretens:topic:missing", "missing-id")
	if err == nil {
		t.Error("Expected error for missing message, got nil")
	}
	if msg != nil {
		t.Error("Expected nil msg for missing ID")
	}

	// 2. GetSubscription Not Found
	sub, err := s.GetSubscription(ctx, "arn:concretens:topic:missing:sub-missing")
	if err == nil {
		t.Error("Expected error for missing subscription, got nil")
	}
	if sub != nil {
		t.Error("Expected nil sub for missing ARN")
	}
}

func TestStore_CorruptedData(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. Inject Corrupted Subscription (Invalid JSON)
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Valid Key structure: subDir + subArn
		subArn := "arn:concretens:topic:bad:sub-bad"
		subKey := s.subDir.Pack(tuple.Tuple{subArn})
		tr.Set(subKey, []byte("{invalid-json"))
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to inject corrupted sub: %v", err)
	}

	// ListSubscriptions should skip it
	subs, err := s.ListSubscriptions(ctx)
	if err != nil {
		t.Fatalf("ListSubscriptions failed: %v", err)
	}
	for _, sub := range subs {
		if sub.SubscriptionArn == "arn:concretens:topic:bad:sub-bad" {
			t.Error("ListSubscriptions returned corrupted subscription")
		}
	}

	// 2. Inject Corrupted Topic (Invalid Key Depth? or Invalid Value)
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// TopicDir + topicArn
		topicArn := "arn:concretens:topic:bad"
		key := s.topicDir.Pack(tuple.Tuple{topicArn}) // len 1 is valid
		tr.Set(key, []byte("not-json"))
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to inject corrupted topic: %v", err)
	}

	topics, err := s.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}
	for _, topic := range topics {
		if topic.TopicArn == "arn:concretens:topic:bad" {
			t.Error("ListTopics returned corrupted topic")
		}
	}
}

func TestStore_InconsistentState(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// Inject Index Entry for missing subscription
	topicArn := "arn:concretens:topic:inconsistent"
	subArn := "arn:concretens:topic:inconsistent:sub-missing"

	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		idxKey := s.topicDir.Pack(tuple.Tuple{"subs", topicArn, subArn})
		tr.Set(idxKey, []byte{})
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to inject index: %v", err)
	}

	// ListSubscriptionsByTopic should ignore the missing sub
	subs, err := s.ListSubscriptionsByTopic(ctx, topicArn)
	if err != nil {
		t.Fatalf("ListSubscriptionsByTopic failed: %v", err)
	}
	if len(subs) != 0 {
		t.Errorf("Expected 0 subs, got %d", len(subs))
	}
}

func TestStore_PublishMessage_CorruptedSub(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. Manually setup Topic, Sub (Corrupted), and Index
	// 1. Create Topic (Now required for Publish check)
	// CreateTopic generates ARN from Name.
	tName := "corrupt-sub-" + uuid.New().String()
	topic, _ := s.CreateTopic(ctx, tName, nil)
	topicArn := topic.TopicArn
	subArn := topicArn + ":sub-corrupt"

	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Sub Data (Invalid JSON)
		subKey := s.subDir.Pack(tuple.Tuple{subArn})
		tr.Set(subKey, []byte("{not-json"))

		// Index
		idxKey := s.topicDir.Pack(tuple.Tuple{"subs", topicArn, subArn})
		tr.Set(idxKey, []byte{})
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to setup corrupt sub: %v", err)
	}

	// 2. Publish Message
	// Should skip the corrupted sub and succeed
	msg := &models.Message{TopicArn: topicArn, Message: "test"}
	err = s.PublishMessage(ctx, msg)
	if err != nil {
		t.Fatalf("PublishMessage failed: %v", err)
	}
	// Success logic implies it handled the error gracefully (by skipping)
}

func TestStore_BadKey(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// Inject Bad Key into SubDir (Invalid Tuple)
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Use raw prefix + invalid byte for Tuple
		// subspace.Bytes() returns the raw prefix
		prefix := s.subDir.Bytes()
		badKey := append(prefix, []byte{0xff, 0xff}...) // 0xff is often invalid in standard tuple start?
		// Actually tuple encoding is complex. But we want Unpack to fail.
		// Unpack expects the key to contain the prefix (which it does) + tuple items.
		// If we put random junk, it might fail to decode tuple.
		tr.Set(fdb.Key(badKey), []byte("val"))
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to setup bad key: %v", err)
	}

	_, err = s.ListSubscriptions(ctx)
	if err != nil {
		t.Fatalf("ListSubscriptions failed: %v", err)
	}
	// Should skip the bad key.
	// We can't strictly check len(subs) == 0 because other tests might have left data.
	// But we verified it didn't panic.
}

func TestStore_TopicAttributes(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. Create Topic
	// 1. Create Topic
	topic, _ := s.CreateTopic(ctx, "attr-topic-"+uuid.New().String(), map[string]string{"init": "val"})

	// 2. Get Attributes
	attrs, err := s.GetTopicAttributes(ctx, topic.TopicArn)
	if err != nil {
		t.Fatalf("GetTopicAttributes failed: %v", err)
	}
	if attrs["init"] != "val" {
		t.Errorf("Expected init=val, got %v", attrs)
	}

	// 3. Set Attributes
	newAttrs := map[string]string{
		"init":  "new-val",
		"added": "added-val",
	}
	err = s.SetTopicAttributes(ctx, topic.TopicArn, newAttrs)
	if err != nil {
		t.Fatalf("SetTopicAttributes failed: %v", err)
	}

	// 4. Verify Update
	updatedAttrs, err := s.GetTopicAttributes(ctx, topic.TopicArn)
	if err != nil {
		t.Fatalf("GetTopicAttributes failed: %v", err)
	}
	if updatedAttrs["init"] != "new-val" {
		t.Errorf("Expected init=new-val, got %s", updatedAttrs["init"])
	}
	if updatedAttrs["added"] != "added-val" {
		t.Errorf("Expected added=added-val, got %s", updatedAttrs["added"])
	}
}

func TestStore_SubscriptionAttributes(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. Setup Topic & Sub
	topic, _ := s.CreateTopic(ctx, "sub-attr-topic", nil)
	sub := &models.Subscription{
		TopicArn: topic.TopicArn,
		Protocol: "sqs",
		Endpoint: "e1",
	}
	createdSub, _ := s.Subscribe(ctx, sub)

	// 2. Set Attributes (RawMessageDelivery)
	err = s.SetSubscriptionAttributes(ctx, createdSub.SubscriptionArn, map[string]string{
		"RawMessageDelivery": "true",
		"Custom":             "custom-val",
	})
	if err != nil {
		t.Fatalf("SetSubscriptionAttributes failed: %v", err)
	}

	// 3. Get Attributes & Verify
	attrs, err := s.GetSubscriptionAttributes(ctx, createdSub.SubscriptionArn)
	if err != nil {
		t.Fatalf("GetSubscriptionAttributes failed: %v", err)
	}
	if attrs["RawMessageDelivery"] != "true" {
		t.Errorf("Expected RawMessageDelivery=true, got %s", attrs["RawMessageDelivery"])
	}
	if attrs["Custom"] != "custom-val" {
		t.Errorf("Expected Custom=custom-val, got %s", attrs["Custom"])
	}

	// 4. Verify struct update (RawDelivery field)
	fetchedSub, _ := s.GetSubscription(ctx, createdSub.SubscriptionArn)
	if !fetchedSub.RawDelivery {
		t.Error("Expected RawDelivery field to be true in struct")
	}
}

func TestStore_Attributes_ErrorPaths(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. Get/Set Attributes on Non-Existent Topic
	_, err = s.GetTopicAttributes(ctx, "arn:concretens:topic:missing")
	if err == nil {
		t.Error("Expected error for missing topic attributes, got nil")
	}
	err = s.SetTopicAttributes(ctx, "arn:concretens:topic:missing", map[string]string{"foo": "bar"})
	if err == nil {
		t.Error("Expected error for missing topic set attributes, got nil")
	}

	// 2. Get/Set Attributes on Non-Existent Subscription
	_, err = s.GetSubscriptionAttributes(ctx, "arn:concretens:topic:t1:sub:missing")
	if err == nil {
		t.Error("Expected error for missing sub attributes, got nil")
	}
	err = s.SetSubscriptionAttributes(ctx, "arn:concretens:topic:t1:sub:missing", map[string]string{"foo": "bar"})
	if err == nil {
		t.Error("Expected error for missing sub set attributes, got nil")
	}
}

func TestStore_Attributes_CorruptedData(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)
	s, err := NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	ctx := context.Background()

	// 1. Corrupted Topic for SetAttributes
	topicArn := "arn:concretens:topic:corrupt"
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(s.topicDir.Pack(tuple.Tuple{topicArn}), []byte("{invalid-json"))
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to inject corrupt topic: %v", err)
	}

	err = s.SetTopicAttributes(ctx, topicArn, map[string]string{"a": "b"})
	if err == nil {
		t.Error("Expected error for SetTopicAttributes on corrupt data, got nil")
	}

	// 2. Corrupted Topic for ListTopics
	// ListTopics swallows errors for corrupted entries (skips them), so we expect NO error.
	topics, err := s.ListTopics(ctx)
	if err != nil {
		t.Errorf("Expected nil error for ListTopics with corrupt data, got %v", err)
	}
	// Verify corrupted topic is not in the list (if we could check arn, but we only have name/arn in struct)
	// Just ensuring it doesn't panic or error is enough for coverage.
	// Check that we don't see a topic with empty arn or something if it partially decoded (it won't).
	for _, tpc := range topics {
		if tpc.TopicArn == topicArn {
			t.Error("Expected corrupted topic to be skipped")
		}
	}

	// 3. Corrupted Sub for SetAttributes
	subArn := "arn:concretens:topic:t:sub:corrupt"
	_, err = s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(s.subDir.Pack(tuple.Tuple{subArn}), []byte("{invalid-json"))
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Failed to inject corrupt sub: %v", err)
	}

	err = s.SetSubscriptionAttributes(ctx, subArn, map[string]string{"a": "b"})
	if err == nil {
		t.Error("Expected error for SetSubscriptionAttributes on corrupt data, got nil")
	}
}
