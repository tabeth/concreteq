package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/kiroku-inc/kiroku-core/apps/concretens/models"
)

type Store interface {
	PollDeliveryTasks(ctx context.Context, limit int) ([]*models.DeliveryTask, error)
	DeleteDeliveryTask(ctx context.Context, task *models.DeliveryTask) error
	GetSubscription(ctx context.Context, subscriptionArn string) (*models.Subscription, error)
	GetMessage(ctx context.Context, topicArn, messageID string) (*models.Message, error)
	RescheduleDeliveryTask(ctx context.Context, task *models.DeliveryTask, nextVisible time.Time) error
	MoveToDLQ(ctx context.Context, task *models.DeliveryTask) error
}

type Dispatcher struct {
	store      Store
	quit       chan struct{}
	httpClient *http.Client
}

func NewDispatcher(store Store, workers int) *Dispatcher {
	return &Dispatcher{
		store: store,
		quit:  make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	go d.loop()
}

func (d *Dispatcher) Stop() {
	close(d.quit)
}

func (d *Dispatcher) loop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.quit:
			return
		case <-ticker.C:
			d.processTasks()
		}
	}
}

func (d *Dispatcher) processTasks() {
	ctx := context.Background()
	tasks, err := d.store.PollDeliveryTasks(ctx, 10)
	if err != nil {
		log.Printf("Error polling tasks: %v", err)
		return
	}

	for _, task := range tasks {
		go d.deliverTask(ctx, task)
	}
}

func (d *Dispatcher) deliverTask(ctx context.Context, task *models.DeliveryTask) {
	// 1. Fetch Subscription
	sub, err := d.store.GetSubscription(ctx, task.SubscriptionArn)
	if err != nil {
		log.Printf("Error fetching subscription %s: %v", task.SubscriptionArn, err)
		return
	}

	// 2. Fetch Message
	var msg *models.Message
	isConfirmation := task.MessageID == "CONFIRMATION_REQUEST"

	if !isConfirmation {
		var err error
		msg, err = d.store.GetMessage(ctx, sub.TopicArn, task.MessageID)
		if err != nil {
			log.Printf("Error fetching message %s: %v", task.MessageID, err)
			return
		}
	} else {
		// Construct synthetic confirmation message
		msg = &models.Message{
			TopicArn:  sub.TopicArn,
			Message:   "You have subscribed to this topic. Visit the SubscribeURL to confirm.",
			MessageID: task.MessageID,
		}
	}

	// 3. Deliver based on protocol
	switch sub.Protocol {
	case "http", "https":
		err = d.deliverHTTP(ctx, sub.Endpoint, msg, sub, isConfirmation)
	case "sqs":
		err = d.deliverSQS(ctx, sub.Endpoint, msg, sub)
	default:
		log.Printf("Unsupported protocol: %s", sub.Protocol)
		// Delete task for unsupported protocols to avoid loop
	}

	if err != nil {
		log.Printf("Delivery failed to %s: %v", sub.Endpoint, err)

		const MaxRetries = 5
		const BaseBackoff = 1 * time.Second

		if task.RetryCount >= MaxRetries {
			log.Printf("Max retries reached for task %s. Moving to DLQ.", task.TaskID)
			if err := d.store.MoveToDLQ(ctx, task); err != nil {
				log.Printf("Error moving task %s to DLQ: %v", task.TaskID, err)
			}
			return
		}

		// Exponential Backoff: base * 2^retryCount
		// RetryCount is 0 initially.
		// 0: 1s
		// 1: 2s
		// 2: 4s
		backoff := BaseBackoff * (1 << task.RetryCount)
		nextVisible := time.Now().Add(backoff)

		log.Printf("Retrying task %s (count %d) after %v", task.TaskID, task.RetryCount+1, backoff)
		if err := d.store.RescheduleDeliveryTask(ctx, task, nextVisible); err != nil {
			log.Printf("Error rescheduling task %s: %v", task.TaskID, err)
		}
		return
	}

	// 4. Delete Task on success
	if err := d.store.DeleteDeliveryTask(ctx, task); err != nil {
		log.Printf("Error deleting task %s: %v", task.TaskID, err)
	}
}

func (d *Dispatcher) deliverHTTP(ctx context.Context, endpoint string, msg *models.Message, sub *models.Subscription, isConfirmation bool) error {
	var payload []byte
	var err error

	if isConfirmation {
		// SubscriptionConfirmation Format (Always JSON)
		confirmPayload := struct {
			Type             string
			TopicArn         string
			Token            string
			Message          string
			SubscribeURL     string
			SignatureVersion string
		}{
			Type:             "SubscriptionConfirmation",
			TopicArn:         sub.TopicArn,
			Token:            sub.ConfirmationToken,
			Message:          msg.Message,
			SubscribeURL:     fmt.Sprintf("http://localhost:8080/confirmSubscription?topicArn=%s&token=%s", sub.TopicArn, sub.ConfirmationToken), // TODO: Use real host
			SignatureVersion: "1",
		}
		payload, err = json.Marshal(confirmPayload)
	} else if sub.RawDelivery {
		// Raw Delivery: Just the message body
		payload = []byte(msg.Message)
	} else {
		// Notification Format (JSON Envelope)
		notificationPayload := struct {
			Type      string
			MessageId string
			TopicArn  string
			Message   string
			Timestamp time.Time
		}{
			Type:      "Notification",
			MessageId: msg.MessageID,
			TopicArn:  msg.TopicArn,
			Message:   msg.Message,
			Timestamp: msg.PublishedTime,
		}
		payload, err = json.Marshal(notificationPayload)
	}

	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	// Set Headers
	req.Header.Set("x-amz-sns-message-type", "Notification")
	if isConfirmation {
		req.Header.Set("x-amz-sns-message-type", "SubscriptionConfirmation")
	} else if sub.RawDelivery {
		// For Raw Delivery, what headers? usually text/plain or implied
		req.Header.Set("Content-Type", "text/plain")
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("x-amz-sns-message-id", msg.MessageID)
	req.Header.Set("x-amz-sns-topic-arn", msg.TopicArn)
	req.Header.Set("x-amz-sns-subscription-arn", sub.SubscriptionArn)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http status %s", resp.Status)
	}
	return nil
}

func (d *Dispatcher) deliverSQS(ctx context.Context, endpoint string, msg *models.Message, sub *models.Subscription) error {
	// Protocol: AWS SQS JSON (via X-Amz-Target)
	type MessageAttributeValue struct {
		DataType    string  `json:"DataType"`
		StringValue *string `json:"StringValue,omitempty"`
	}

	body := msg.Message
	if !sub.RawDelivery {
		// If NOT raw delivery, wrap in SNS JSON
		notificationPayload := struct {
			Type      string
			MessageId string
			TopicArn  string
			Message   string
			Timestamp time.Time
		}{
			Type:      "Notification",
			MessageId: msg.MessageID,
			TopicArn:  msg.TopicArn,
			Message:   msg.Message,
			Timestamp: msg.PublishedTime,
		}
		b, _ := json.Marshal(notificationPayload)
		body = string(b)
	}

	payload := struct {
		QueueUrl          string                           `json:"QueueUrl"`
		MessageBody       string                           `json:"MessageBody"`
		MessageAttributes map[string]MessageAttributeValue `json:"MessageAttributes,omitempty"`
	}{
		QueueUrl:    endpoint,
		MessageBody: body,
	}

	if len(msg.MessageAttributes) > 0 {
		payload.MessageAttributes = make(map[string]MessageAttributeValue)
		for k, v := range msg.MessageAttributes {
			val := v
			payload.MessageAttributes[k] = MessageAttributeValue{
				DataType:    "String",
				StringValue: &val,
			}
		}
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("sqs error %s: %s", resp.Status, string(respBody))
}
