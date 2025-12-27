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
	FifoTopic   bool              `json:"fifoTopic"` // Parity: derived from attributes or name
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
	TopicArn               string            `json:"topicArn"`
	Message                string            `json:"message"`
	Subject                string            `json:"subject,omitempty"`
	MessageStructure       string            `json:"messageStructure,omitempty"`
	MessageAttributes      map[string]string `json:"messageAttributes,omitempty"`
	MessageGroupId         string            `json:"messageGroupId,omitempty"`         // FIFO
	MessageDeduplicationId string            `json:"messageDeduplicationId,omitempty"` // FIFO
}

// PublishResponse represents the response after publishing.
type PublishResponse struct {
	MessageId      string `json:"messageId"`
	SequenceNumber string `json:"sequenceNumber,omitempty"` // FIFO
}

// PublishBatchRequestEntry represents a single message in a batch.
type PublishBatchRequestEntry struct {
	Id                     string            `json:"id"`
	Message                string            `json:"message"`
	Subject                string            `json:"subject,omitempty"`
	MessageStructure       string            `json:"messageStructure,omitempty"`
	MessageAttributes      map[string]string `json:"messageAttributes,omitempty"`
	MessageGroupId         string            `json:"messageGroupId,omitempty"`
	MessageDeduplicationId string            `json:"messageDeduplicationId,omitempty"`
}

// PublishBatchRequest represents a batch publication request.
type PublishBatchRequest struct {
	TopicArn                   string                     `json:"topicArn"`
	PublishBatchRequestEntries []PublishBatchRequestEntry `json:"publishBatchRequestEntries"`
}

// PublishBatchResultEntry represents success.
type PublishBatchResultEntry struct {
	Id             string `json:"id"`
	MessageId      string `json:"messageId"`
	SequenceNumber string `json:"sequenceNumber,omitempty"`
}

// BatchResultErrorEntry represents failure.
type BatchResultErrorEntry struct {
	Id          string `json:"id"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	SenderFault bool   `json:"senderFault"`
}

// PublishBatchResponse represents the response.
type PublishBatchResponse struct {
	Successful []PublishBatchResultEntry `json:"successful"`
	Failed     []BatchResultErrorEntry   `json:"failed"`
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
	MessageID              string            `json:"messageId"`
	TopicArn               string            `json:"topicArn"`
	Message                string            `json:"message"`
	Subject                string            `json:"subject,omitempty"`
	MessageAttributes      map[string]string `json:"messageAttributes,omitempty"`
	PublishedTime          time.Time         `json:"publishedTime"`
	MessageGroupId         string            `json:"messageGroupId,omitempty"`         // FIFO
	MessageDeduplicationId string            `json:"messageDeduplicationId,omitempty"` // FIFO
	SequenceNumber         string            `json:"sequenceNumber,omitempty"`         // FIFO: Simulated
}

// Tag represents a key-value pair.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// TagResourceRequest represents a request to add tags to a resource.
type TagResourceRequest struct {
	ResourceArn string `json:"resourceArn"`
	Tags        []Tag  `json:"tags"`
}

// UntagResourceRequest represents a request to remove tags from a resource.
type UntagResourceRequest struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

// ListTagsForResourceRequest represents a request to list tags for a resource.
type ListTagsForResourceRequest struct {
	ResourceArn string `json:"resourceArn"`
}

// ListTagsForResourceResponse represents the response containing tags.
type ListTagsForResourceResponse struct {
	Tags []Tag `json:"tags"`
}
