package store

import (
	"fmt"
	"testing"

	"github.com/tabeth/concreteq/models"
)

func cleanup(b *testing.B, store *FDBStore, queueName string) {
	err := store.DeleteQueue(b.Context(), queueName)
	if err != nil {
		if err.Error() != "queue does not exist" {
			b.Logf("Failed to cleanup queue %s: %v", queueName, err)
		}
	}
}

// BenchmarkReceiveMessageWithContention measures the performance of receiving messages
// from a standard queue under high contention. It simulates many concurrent clients
// all trying to dequeue messages from the same queue.
//
// The sharded index implementation is expected to perform well here, as the random
// polling of shards will distribute the transactional load across the keyspace,
// leading to fewer conflicts and higher throughput.
func BenchmarkReceiveMessageWithContention(b *testing.B) {
	// Setup: Create a unique store for this benchmark run
	dbPath := fmt.Sprintf("benchmark_contention_%d", b.N)
	store, err := NewFDBStoreAtPath(dbPath)
	if err != nil {
		b.Fatalf("Failed to create FDB store: %v", err)
	}
	queueName := "benchmark-queue-std"

	// Clean up before and after the test
	cleanup(b, store, queueName)
	defer cleanup(b, store, queueName)

	// Create a standard queue
	_, err = store.CreateQueue(b.Context(), queueName, nil, nil)
	if err != nil {
		b.Fatalf("Failed to create queue: %v", err)
	}

	// Pre-populate the queue with a large number of messages to ensure
	// the benchmark doesn't run out of work.
	const numMessages = 5000
	for i := 0; i < numMessages; i++ {
		_, err := store.SendMessage(b.Context(), queueName, &models.SendMessageRequest{
			MessageBody: fmt.Sprintf("message-%d", i),
		})
		if err != nil {
			b.Fatalf("Failed to send message: %v", err)
		}
	}

	b.ResetTimer()       // Start timing now
	b.SetParallelism(50) // 50 concurrent goroutines

	// RunParallel will create multiple goroutines and distribute b.N iterations among them.
	ctx := b.Context()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Each goroutine will loop, receiving and deleting one message at a time.
			receiveReq := &models.ReceiveMessageRequest{
				MaxNumberOfMessages: models.Ptr(1),
				WaitTimeSeconds: models.Ptr(1),
			}
			resp, err := store.ReceiveMessage(ctx, queueName, receiveReq)
			if err != nil {
				b.Logf("Received error during receive: %v", err)
				continue
			}

			if len(resp.Messages) > 0 {
				// We delete the message to complete the SQS cycle, but the primary
				// focus of this benchmark is the performance of ReceiveMessage.
				_ = store.DeleteMessage(ctx, queueName, resp.Messages[0].ReceiptHandle)
			}
		}
	})
}
