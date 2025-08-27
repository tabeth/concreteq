// Package models contains the data structures used throughout the application.
// These structures define the shape of API requests and responses, as well as the
// internal representation of data stored in the database. They are often referred to
// as Data Transfer Objects (DTOs).
package models

// CreateQueueRequest maps to the input of the SQS CreateQueue action.
// It includes the queue name and any attributes (like VisibilityTimeout) or tags.
type CreateQueueRequest struct {
	QueueName  string            `json:"QueueName"`
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"tags"`
}

// CreateQueueResponse maps to the output of a successful SQS CreateQueue action.
// It returns the URL of the newly created queue.
type CreateQueueResponse struct {
	QueueURL string `json:"QueueUrl"`
}

// ListQueuesRequest defines the parameters for the SQS ListQueues action.
// It supports pagination (MaxResults, NextToken) and filtering by prefix.
type ListQueuesRequest struct {
	MaxResults      int    `json:"MaxResults"`
	NextToken       string `json:"NextToken"`
	QueueNamePrefix string `json:"QueueNamePrefix"`
}

// ListQueuesResponse defines the structure for the SQS ListQueues action's output.
// It provides a list of queue URLs and a token for fetching the next page of results.
type ListQueuesResponse struct {
	QueueUrls []string `json:"QueueUrls"`
	NextToken string   `json:"NextToken,omitempty"`
}

// GetQueueAttributesRequest defines the parameters for the SQS GetQueueAttributes action.
type GetQueueAttributesRequest struct {
	QueueUrl       string   `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames"`
}

// GetQueueAttributesResponse defines the structure for the SQS GetQueueAttributes action's output.
type GetQueueAttributesResponse struct {
	Attributes map[string]string `json:"Attributes"`
}

type SetQueueAttributesRequest struct {
	QueueUrl   string            `json:"QueueUrl"`
	Attributes map[string]string `json:"Attributes"`
}

type GetQueueUrlRequest struct {
	QueueName              string `json:"QueueName"`
	QueueOwnerAWSAccountId string `json:"QueueOwnerAWSAccountId,omitempty"`
}

type GetQueueUrlResponse struct {
	QueueUrl string `json:"QueueUrl"`
}

type ChangeMessageVisibilityBatchRequestEntry struct {
	Id                string `json:"Id"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

type ChangeMessageVisibilityBatchRequest struct {
	QueueUrl string                                     `json:"QueueUrl"`
	Entries  []ChangeMessageVisibilityBatchRequestEntry `json:"Entries"`
}

type ChangeMessageVisibilityBatchResponse struct {
	Successful []ChangeMessageVisibilityBatchResultEntry `json:"Successful"`
	Failed     []BatchResultErrorEntry                   `json:"Failed"`
}

type ChangeMessageVisibilityBatchResultEntry struct {
	Id string `json:"Id"`
}

// MessageAttributeValue represents the value of a custom message attribute in SQS.
// It can hold string, binary, or lists of these types.
type MessageAttributeValue struct {
	BinaryListValues [][]byte `json:"BinaryListValues,omitempty"`
	BinaryValue      []byte   `json:"BinaryValue,omitempty"`
	DataType         string   `json:"DataType"`
	StringListValues []string `json:"StringListValues,omitempty"`
	StringValue      *string  `json:"StringValue,omitempty"`
}

// MessageSystemAttributeValue is similar to MessageAttributeValue but for system-level attributes.
// An example is the AWSTraceHeader for X-Ray integration.
type MessageSystemAttributeValue struct {
	BinaryListValues [][]byte `json:"BinaryListValues,omitempty"`
	BinaryValue      []byte   `json:"BinaryValue,omitempty"`
	DataType         string   `json:"DataType"`
	StringListValues []string `json:"StringListValues,omitempty"`
	StringValue      *string  `json:"StringValue,omitempty"`
}

// SendMessageRequest maps to the input of the SQS SendMessage action.
// It contains the message body, optional delay, and attributes for standard queues,
// as well as required group and deduplication IDs for FIFO queues.
type SendMessageRequest struct {
	DelaySeconds            *int32                             `json:"DelaySeconds,omitempty"`
	MessageAttributes       map[string]MessageAttributeValue   `json:"MessageAttributes,omitempty"`
	MessageBody             string                             `json:"MessageBody"`
	MessageDeduplicationId  *string                            `json:"MessageDeduplicationId,omitempty"`
	MessageGroupId          *string                            `json:"MessageGroupId,omitempty"`
	MessageSystemAttributes map[string]MessageSystemAttributeValue `json:"MessageSystemAttributes,omitempty"`
	QueueUrl                string                             `json:"QueueUrl"`
}

// SendMessageResponse maps to the output of a successful SQS SendMessage action.
// It includes the unique ID assigned to the message and MD5 hashes for verification.
// For FIFO queues, it also includes a sequence number.
type SendMessageResponse struct {
	MD5OfMessageAttributes       *string `json:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageBody             string  `json:"MD5OfMessageBody"`
	MD5OfMessageSystemAttributes *string `json:"MD5OfMessageSystemAttributes,omitempty"`
	MessageId                    string  `json:"MessageId"`
	SequenceNumber               *string `json:"SequenceNumber,omitempty"`
}

// Message is the internal representation of a message within the storage system.
// It combines the public SQS fields with internal state management fields,
// such as when the message becomes visible again (VisibleAfter) and how many times
// it has been received (ReceivedCount). This struct is not directly exposed via the API.
type Message struct {
	ID                 string
	Body               string
	Attributes         map[string]MessageAttributeValue
	SystemAttributes   map[string]MessageSystemAttributeValue
	MD5OfBody          string
	MD5OfAttributes    string
	MD5OfSysAttributes string
	VisibleAfter       int64 // Timestamp for when the message is no longer in-flight.
	ReceivedCount      int   // Tracks the number of times a message has been received.
	FirstReceived      int64 // Timestamp of the first time the message was received.
	// Fields for FIFO queues and standard SQS attributes.
	SenderId       string `json:"SenderId,omitempty"`
	SentTimestamp  int64  `json:"SentTimestamp,omitempty"`
	MessageGroupId string `json:"MessageGroupId,omitempty"`
	SequenceNumber int64  `json:"SequenceNumber,omitempty"`
}

// DeleteQueueRequest defines the parameters for the SQS DeleteQueue action.
type DeleteQueueRequest struct {
	QueueUrl string `json:"QueueUrl"`
}

// PurgeQueueRequest defines the parameters for the SQS PurgeQueue action.
type PurgeQueueRequest struct {
	QueueUrl string `json:"QueueUrl"`
}

// ReceiveMessageRequest maps to the input of the SQS ReceiveMessage action.
// It allows callers to specify how many messages to get, how long to wait (long polling),
// and how long the message should be hidden from other consumers (VisibilityTimeout).
type ReceiveMessageRequest struct {
	AttributeNames            []string `json:"AttributeNames"`
	MaxNumberOfMessages       int      `json:"MaxNumberOfMessages"`
	MessageAttributeNames     []string `json:"MessageAttributeNames"`
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames"`
	QueueUrl                  string   `json:"QueueUrl"`
	ReceiveRequestAttemptId   string   `json:"ReceiveRequestAttemptId"` // For FIFO queue receive deduplication.
	VisibilityTimeout         int      `json:"VisibilityTimeout"`
	WaitTimeSeconds           int      `json:"WaitTimeSeconds"`
}

// ReceiveMessageResponse defines the structure for the SQS ReceiveMessage action's output.
// It contains a list of received messages, if any.
type ReceiveMessageResponse struct {
	Messages []ResponseMessage `json:"Messages"`
}

// ResponseMessage represents a single message as returned to the client from a ReceiveMessage call.
// It includes a ReceiptHandle, which is a temporary token required to delete or modify the message.
type ResponseMessage struct {
	Attributes             map[string]string                `json:"Attributes"`
	Body                   string                           `json:"Body"`
	MD5OfBody              string                           `json:"MD5OfBody"`
	MD5OfMessageAttributes *string                          `json:"MD5OfMessageAttributes,omitempty"`
	MessageAttributes      map[string]MessageAttributeValue `json:"MessageAttributes,omitempty"`
	MessageId              string                           `json:"MessageId"`
	ReceiptHandle          string                           `json:"ReceiptHandle"`
}

// DeleteMessageRequest defines the parameters for the SQS DeleteMessage action.
// It requires the queue's URL and the specific ReceiptHandle of the message to be deleted.
type DeleteMessageRequest struct {
	QueueUrl      string `json:"QueueUrl"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

// ChangeMessageVisibilityRequest defines the parameters for the SQS ChangeMessageVisibility action.
type ChangeMessageVisibilityRequest struct {
	QueueUrl          string `json:"QueueUrl"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

// ErrorResponse defines the standard AWS JSON error response format.
// This ensures that clients interacting with this service can parse errors in a familiar way.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// --- Batch Operation Models ---

// SendMessageBatchRequest defines the parameters for the SQS SendMessageBatch action.
type SendMessageBatchRequest struct {
	QueueUrl string                         `json:"QueueUrl"`
	Entries  []SendMessageBatchRequestEntry `json:"Entries"`
}

// SendMessageBatchRequestEntry defines a single message within a batch send request.
// Each entry has a unique ID within the batch for correlating results.
type SendMessageBatchRequestEntry struct {
	Id                      string                             `json:"Id"`
	MessageBody             string                             `json:"MessageBody"`
	DelaySeconds            *int32                             `json:"DelaySeconds,omitempty"`
	MessageAttributes       map[string]MessageAttributeValue   `json:"MessageAttributes,omitempty"`
	MessageSystemAttributes map[string]MessageSystemAttributeValue `json:"MessageSystemAttributes,omitempty"`
	MessageDeduplicationId  *string                            `json:"MessageDeduplicationId,omitempty"`
	MessageGroupId          *string                            `json:"MessageGroupId,omitempty"`
}

// SendMessageBatchResponse defines the structure for the SQS SendMessageBatch action's output.
// It separates results into successful and failed entries.
type SendMessageBatchResponse struct {
	Successful []SendMessageBatchResultEntry `json:"Successful"`
	Failed     []BatchResultErrorEntry       `json:"Failed"`
}

// SendMessageBatchResultEntry contains the details of a successfully sent message in a batch.
// It mirrors the single SendMessageResponse but includes the original entry ID.
type SendMessageBatchResultEntry struct {
	Id                           string  `json:"Id"`
	MessageId                    string  `json:"MessageId"`
	MD5OfMessageBody             string  `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes       *string `json:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes *string `json:"MD5OfMessageSystemAttributes,omitempty"`
	SequenceNumber               *string `json:"SequenceNumber,omitempty"`
}

// BatchResultErrorEntry contains the details of a failed message in a batch operation.
// It includes the original ID, an error code, a message, and whether the sender was at fault.
type BatchResultErrorEntry struct {
	Id          string `json:"Id"`
	Code        string `json:"Code"`
	Message     string `json:"Message"`
	SenderFault bool   `json:"SenderFault"`
}

// DeleteMessageBatchRequest defines the parameters for the SQS DeleteMessageBatch action.
type DeleteMessageBatchRequest struct {
	QueueUrl string                           `json:"QueueUrl"`
	Entries  []DeleteMessageBatchRequestEntry `json:"Entries"`
}

// DeleteMessageBatchRequestEntry defines a single message to be deleted in a batch.
type DeleteMessageBatchRequestEntry struct {
	Id            string `json:"Id"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

// DeleteMessageBatchResponse defines the structure for the SQS DeleteMessageBatch action's output.
type DeleteMessageBatchResponse struct {
	Successful []DeleteMessageBatchResultEntry `json:"Successful"`
	Failed     []BatchResultErrorEntry         `json:"Failed"`
}

// DeleteMessageBatchResultEntry contains the ID of a successfully deleted message in a batch.
type DeleteMessageBatchResultEntry struct {
	Id string `json:"Id"`
}
