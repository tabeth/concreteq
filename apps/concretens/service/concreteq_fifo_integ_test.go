package service

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	// Concretens
	nsmodels "github.com/kiroku-inc/kiroku-core/apps/concretens/models"
	nsstore "github.com/kiroku-inc/kiroku-core/apps/concretens/store"

	// ConcreteQ
	qmodels "github.com/tabeth/concreteq/models"
	qserver "github.com/tabeth/concreteq/server"
	qstore "github.com/tabeth/concreteq/store"

	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestIntegration_ConcreteQ_FIFO(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	// 1. Setup ConcreteQ
	qs, err := qstore.NewFDBStore()
	if err != nil {
		t.Fatalf("Failed to create ConcreteQ store: %v", err)
	}

	qcApp := &qserver.App{Store: qs}
	r := chi.NewRouter()
	qcApp.RegisterSQSHandlers(r)
	qServer := httptest.NewServer(r)
	defer qServer.Close()

	// 2. Setup Concretens
	nss, err := nsstore.NewStore(710)
	if err != nil {
		t.Fatalf("Failed to create Concretens store: %v", err)
	}
	nss.ClearQueue(context.Background())

	dispatcher := NewDispatcher(nss, 1, "")
	dispatcher.Start(context.Background())
	defer dispatcher.Stop()

	ctx := context.Background()

	// 3. Create FIFO Queue in ConcreteQ
	queueName := "fifo-queue-" + uuid.New().String() + ".fifo"
	_, err = qs.CreateQueue(ctx, queueName, map[string]string{
		"FifoQueue": "true",
	}, nil)
	if err != nil {
		t.Fatalf("ConcreteQ CreateQueue failed: %v", err)
	}
	queueUrl := fmt.Sprintf("%s/queues/%s", qServer.URL, queueName)
	t.Logf("Created ConcreteQ FIFO Queue: %s", queueUrl)

	// 4. Create FIFO Topic in Concretens
	topicName := "fifo-topic-" + uuid.New().String() + ".fifo"
	topic, _ := nss.CreateTopic(ctx, topicName, map[string]string{
		"ContentBasedDeduplication": "true",
	})
	if !topic.FifoTopic {
		t.Fatal("Topic should be FIFO")
	}

	// 5. Subscribe
	sub := &nsmodels.Subscription{
		TopicArn:    topic.TopicArn,
		Protocol:    "sqs",
		Endpoint:    queueUrl,
		RawDelivery: true, // Typically FIFO messages are RAW to preserve headers/properties exactly
	}
	if _, err := nss.Subscribe(ctx, sub); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// 6. Publish Message with GroupID
	msgBody := "fifo-msg-" + uuid.New().String()
	groupID := "group-1"
	msg := &nsmodels.Message{
		TopicArn:       topic.TopicArn,
		Message:        msgBody,
		MessageGroupId: groupID,
	}
	if err := nss.PublishMessage(ctx, msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// 7. Verify in ConcreteQ
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for FIFO message in ConcreteQ")
		case <-ticker.C:
			req := qmodels.ReceiveMessageRequest{
				MaxNumberOfMessages: qmodels.Ptr(10),
				VisibilityTimeout:   qmodels.Ptr(10),
				WaitTimeSeconds:     qmodels.Ptr(0),
				AttributeNames:      []string{"MessageGroupId", "ApproximateReceiveCount"},
			}
			resp, err := qs.ReceiveMessage(ctx, queueName, &req)
			if err != nil {
				// Retry on error (maybe temporary)
				t.Logf("ReceiveMessage error: %v", err)
				continue
			}

			if len(resp.Messages) > 0 {
				for _, m := range resp.Messages {
					if m.Body == msgBody {
						t.Logf("Found message: %s", m.Body)

						// Verify MessageGroupId
						// In ConcreteQ (and SQS), MessageGroupId is returned in Attributes map if requested
						// Or sometimes in response top level?
						// In models.go ResponseMessage has Attributes map[string]string.

						if val, ok := m.Attributes["MessageGroupId"]; ok {
							if val == groupID {
								t.Log("MessageGroupId Verified!")
								return // Success
							} else {
								t.Errorf("Expected GroupID %s, got %s", groupID, val)
								return
							}
						} else {
							t.Logf("MessageGroupId missing in attributes. Attributes: %v", m.Attributes)
							// If concreteq doesn't implement echoing this back yet, we might fail or warn.
							// Assuming concreteq supports it (it has the field in Message struct).
							// If it fails, it might mean concreteq implementation detail, but asking for full parity integration.
							// I'll fail for now to be strict.
							t.FailNow()
						}
						return
					}
				}
			}
		}
	}
}
