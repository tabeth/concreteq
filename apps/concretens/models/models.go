package models

import (
	"time"
)

// Topic represents an SNS topic.
type Topic struct {
	TopicArn    string            `json:"topicArn"`
	Name        string            `json:"name"`
	Attributes  map[string]string `json:"attributes"`
	CreatedTime time.Time         `json:"createdTime"`
}

// Subscription represents a subscription to a topic.
type Subscription struct {
	SubscriptionArn string            `json:"subscriptionArn"`
	TopicArn        string            `json:"topicArn"`
	Protocol        string            `json:"protocol"` // e.g., "http", "https", "sqs", "email"
	Endpoint        string            `json:"endpoint"`
	Owner           string            `json:"owner"`
	Attributes      map[string]string `json:"attributes"`
	FilterPolicy    string            `json:"filterPolicy,omitempty"`
	// New fields for SNS Parity
	Status            string `json:"Status"`            // e.g. "Active", "PendingConfirmation"
	ConfirmationToken string `json:"ConfirmationToken"` // Token for handshake
	RawDelivery       bool   `json:"RawDelivery"`       // If true, sends raw message body
}

// PublishRequest represents a request to publish a message.
type PublishRequest struct {
	TopicArn          string            `json:"topicArn"`
	Message           string            `json:"message"`
	Subject           string            `json:"subject,omitempty"`
	MessageStructure  string            `json:"messageStructure,omitempty"`
	MessageAttributes map[string]string `json:"messageAttributes,omitempty"`
}

// PublishResponse represents the response after publishing.
type PublishResponse struct {
	MessageId string `json:"messageId"`
}

// DeliveryTask represents a task to deliver a message to a subscriber.
type DeliveryTask struct {
	TaskID          string    `json:"taskId"`
	SubscriptionArn string    `json:"subscriptionArn"`
	MessageID       string    `json:"messageId"`
	VisibleAfter    time.Time `json:"visibleAfter"`
	RetryCount      int       `json:"retryCount"`
}

// Message represents a persisted message.
type Message struct {
	MessageID         string            `json:"messageId"`
	TopicArn          string            `json:"topicArn"`
	Message           string            `json:"message"`
	Subject           string            `json:"subject,omitempty"`
	MessageAttributes map[string]string `json:"messageAttributes,omitempty"`
	PublishedTime     time.Time         `json:"publishedTime"`
}
