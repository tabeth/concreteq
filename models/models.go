// Package models contains the data structures used throughout the application.
// These structures define the shape of API requests and responses, as well as the
// internal representation of data stored in the database. They are often referred to
// as Data Transfer Objects (DTOs).
package models

// CreateQueueRequest maps to the input of the SQS CreateQueue action.
// It includes the queue name and any attributes (like VisibilityTimeout) or tags.
type CreateQueueRequest struct {
	// QueueName is the name of the queue to be created.
	QueueName string `json:"QueueName"`
	// Attributes is a map of attributes for the queue (e.g., "VisibilityTimeout", "FifoQueue").
	Attributes map[string]string `json:"Attributes"`
	// Tags is a map of key-value pairs to attach to the queue.
	Tags map[string]string `json:"tags"`
}

// CreateQueueResponse maps to the output of a successful SQS CreateQueue action.
// It returns the URL of the newly created queue.
type CreateQueueResponse struct {
	// QueueURL is the URL of the created queue.
	QueueURL string `json:"QueueUrl"`
}

// ListQueuesRequest defines the parameters for the SQS ListQueues action.
// It supports pagination (MaxResults, NextToken) and filtering by prefix.
type ListQueuesRequest struct {
	// MaxResults is the maximum number of results to return in a single call.
	MaxResults int `json:"MaxResults"`
	// NextToken is the token to retrieve the next page of results.
	NextToken string `json:"NextToken"`
	// QueueNamePrefix is an optional filter to list only queues starting with this prefix.
	QueueNamePrefix string `json:"QueueNamePrefix"`
}

// ListQueuesResponse defines the structure for the SQS ListQueues action's output.
// It provides a list of queue URLs and a token for fetching the next page of results.
type ListQueuesResponse struct {
	// QueueUrls is a list of URLs of the queues that match the request.
	QueueUrls []string `json:"QueueUrls"`
	// NextToken is the token to use for the next ListQueues request.
	NextToken string `json:"NextToken,omitempty"`
}

// GetQueueAttributesRequest defines the parameters for the SQS GetQueueAttributes action.
type GetQueueAttributesRequest struct {
	// QueueUrl is the URL of the queue to retrieve attributes for.
	QueueUrl string `json:"QueueUrl"`
	// AttributeNames is a list of attributes to retrieve (e.g., "All", "VisibilityTimeout").
	AttributeNames []string `json:"AttributeNames"`
}

// GetQueueAttributesResponse defines the structure for the SQS GetQueueAttributes action's output.
type GetQueueAttributesResponse struct {
	// Attributes is a map of the requested queue attributes.
	Attributes map[string]string `json:"Attributes"`
}

// MessageAttributeValue represents the value of a custom message attribute in SQS.
// It can hold string, binary, or lists of these types.
type MessageAttributeValue struct {
	// BinaryListValues is a list of binary values.
	BinaryListValues [][]byte `json:"BinaryListValues,omitempty"`
	// BinaryValue is a binary value.
	BinaryValue []byte `json:"BinaryValue,omitempty"`
	// DataType indicates the type of the attribute (e.g., "String", "Number", "Binary").
	DataType string `json:"DataType"`
	// StringListValues is a list of string values.
	StringListValues []string `json:"StringListValues,omitempty"`
	// StringValue is a string value.
	StringValue *string `json:"StringValue,omitempty"`
}

// MessageSystemAttributeValue is similar to MessageAttributeValue but for system-level attributes.
// An example is the AWSTraceHeader for X-Ray integration.
type MessageSystemAttributeValue struct {
	// BinaryListValues is a list of binary values.
	BinaryListValues [][]byte `json:"BinaryListValues,omitempty"`
	// BinaryValue is a binary value.
	BinaryValue []byte `json:"BinaryValue,omitempty"`
	// DataType indicates the type of the attribute.
	DataType string `json:"DataType"`
	// StringListValues is a list of string values.
	StringListValues []string `json:"StringListValues,omitempty"`
	// StringValue is a string value.
	StringValue *string `json:"StringValue,omitempty"`
}

// SendMessageRequest maps to the input of the SQS SendMessage action.
// It contains the message body, optional delay, and attributes for standard queues,
// as well as required group and deduplication IDs for FIFO queues.
type SendMessageRequest struct {
	// DelaySeconds is the number of seconds to delay the message (0-900).
	DelaySeconds *int32 `json:"DelaySeconds,omitempty"`
	// MessageAttributes is a map of custom attributes for the message.
	MessageAttributes map[string]MessageAttributeValue `json:"MessageAttributes,omitempty"`
	// MessageBody is the body of the message.
	MessageBody string `json:"MessageBody"`
	// MessageDeduplicationId is the token used for deduplication of sent messages (FIFO only).
	MessageDeduplicationId *string `json:"MessageDeduplicationId,omitempty"`
	// MessageGroupId is the tag that specifies the group the message belongs to (FIFO only).
	MessageGroupId *string `json:"MessageGroupId,omitempty"`
	// MessageSystemAttributes is a map of system attributes for the message.
	MessageSystemAttributes map[string]MessageSystemAttributeValue `json:"MessageSystemAttributes,omitempty"`
	// QueueUrl is the URL of the queue to send the message to.
	QueueUrl string `json:"QueueUrl"`
}

// SendMessageResponse maps to the output of a successful SQS SendMessage action.
// It includes the unique ID assigned to the message and MD5 hashes for verification.
// For FIFO queues, it also includes a sequence number.
type SendMessageResponse struct {
	// MD5OfMessageAttributes is the MD5 digest of the message attributes.
	MD5OfMessageAttributes *string `json:"MD5OfMessageAttributes,omitempty"`
	// MD5OfMessageBody is the MD5 digest of the message body.
	MD5OfMessageBody string `json:"MD5OfMessageBody"`
	// MD5OfMessageSystemAttributes is the MD5 digest of the message system attributes.
	MD5OfMessageSystemAttributes *string `json:"MD5OfMessageSystemAttributes,omitempty"`
	// MessageId is the unique identifier for the sent message.
	MessageId string `json:"MessageId"`
	// SequenceNumber is the sequence number for the message (FIFO only).
	SequenceNumber *string `json:"SequenceNumber,omitempty"`
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
	// QueueUrl is the URL of the queue to delete.
	QueueUrl string `json:"QueueUrl"`
}

// PurgeQueueRequest defines the parameters for the SQS PurgeQueue action.
type PurgeQueueRequest struct {
	// QueueUrl is the URL of the queue to purge.
	QueueUrl string `json:"QueueUrl"`
}

// ReceiveMessageRequest maps to the input of the SQS ReceiveMessage action.
// It allows callers to specify how many messages to get, how long to wait (long polling),
// and how long the message should be hidden from other consumers (VisibilityTimeout).
type ReceiveMessageRequest struct {
	// AttributeNames is a list of attributes that need to be returned along with each message.
	AttributeNames []string `json:"AttributeNames"`
	// MaxNumberOfMessages is the maximum number of messages to return (1-10).
	MaxNumberOfMessages int `json:"MaxNumberOfMessages"`
	// MessageAttributeNames is a list of message attributes to retrieve.
	MessageAttributeNames []string `json:"MessageAttributeNames"`
	// MessageSystemAttributeNames is a list of system attributes to retrieve.
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames"`
	// QueueUrl is the URL of the queue to receive messages from.
	QueueUrl string `json:"QueueUrl"`
	// ReceiveRequestAttemptId is the deduplication token for the receive request (FIFO only).
	ReceiveRequestAttemptId string `json:"ReceiveRequestAttemptId"`
	// VisibilityTimeout is the duration (in seconds) that the received messages are hidden from subsequent retrieve requests.
	VisibilityTimeout int `json:"VisibilityTimeout"`
	// WaitTimeSeconds is the duration (in seconds) for which the call waits for a message to arrive in the queue before returning.
	WaitTimeSeconds int `json:"WaitTimeSeconds"`
}

// ReceiveMessageResponse defines the structure for the SQS ReceiveMessage action's output.
// It contains a list of received messages, if any.
type ReceiveMessageResponse struct {
	// Messages is the list of messages received.
	Messages []ResponseMessage `json:"Messages"`
}

// ResponseMessage represents a single message as returned to the client from a ReceiveMessage call.
// It includes a ReceiptHandle, which is a temporary token required to delete or modify the message.
type ResponseMessage struct {
	// Attributes is a map of the requested attributes.
	Attributes map[string]string `json:"Attributes"`
	// Body is the body of the message.
	Body string `json:"Body"`
	// MD5OfBody is the MD5 digest of the message body.
	MD5OfBody string `json:"MD5OfBody"`
	// MD5OfMessageAttributes is the MD5 digest of the message attributes.
	MD5OfMessageAttributes *string `json:"MD5OfMessageAttributes,omitempty"`
	// MessageAttributes is a map of the custom message attributes.
	MessageAttributes map[string]MessageAttributeValue `json:"MessageAttributes,omitempty"`
	// MessageId is the unique identifier of the message.
	MessageId string `json:"MessageId"`
	// ReceiptHandle is the token used to delete or change the visibility of the message.
	ReceiptHandle string `json:"ReceiptHandle"`
}

// DeleteMessageRequest defines the parameters for the SQS DeleteMessage action.
// It requires the queue's URL and the specific ReceiptHandle of the message to be deleted.
type DeleteMessageRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// ReceiptHandle is the handle associated with the message to delete.
	ReceiptHandle string `json:"ReceiptHandle"`
}

// ChangeMessageVisibilityRequest defines the parameters for the SQS ChangeMessageVisibility action.
type ChangeMessageVisibilityRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// ReceiptHandle is the handle associated with the message.
	ReceiptHandle string `json:"ReceiptHandle"`
	// VisibilityTimeout is the new value for the message's visibility timeout (in seconds).
	VisibilityTimeout int `json:"VisibilityTimeout"`
}

// ErrorResponse defines the standard AWS JSON error response format.
// This ensures that clients interacting with this service can parse errors in a familiar way.
type ErrorResponse struct {
	// Type is the error code (e.g., "InvalidParameterValue").
	Type string `json:"__type"`
	// Message is the descriptive error message.
	Message string `json:"message"`
}

// --- Batch Operation Models ---

// SendMessageBatchRequest defines the parameters for the SQS SendMessageBatch action.
type SendMessageBatchRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// Entries is a list of SendMessageBatchRequestEntry items.
	Entries []SendMessageBatchRequestEntry `json:"Entries"`
}

// SendMessageBatchRequestEntry defines a single message within a batch send request.
// Each entry has a unique ID within the batch for correlating results.
type SendMessageBatchRequestEntry struct {
	// Id is a unique identifier for the entry within the batch.
	Id string `json:"Id"`
	// MessageBody is the body of the message.
	MessageBody string `json:"MessageBody"`
	// DelaySeconds is the number of seconds to delay the message.
	DelaySeconds *int32 `json:"DelaySeconds,omitempty"`
	// MessageAttributes is a map of custom attributes for the message.
	MessageAttributes map[string]MessageAttributeValue `json:"MessageAttributes,omitempty"`
	// MessageSystemAttributes is a map of system attributes for the message.
	MessageSystemAttributes map[string]MessageSystemAttributeValue `json:"MessageSystemAttributes,omitempty"`
	// MessageDeduplicationId is the token used for deduplication (FIFO only).
	MessageDeduplicationId *string `json:"MessageDeduplicationId,omitempty"`
	// MessageGroupId is the group ID for the message (FIFO only).
	MessageGroupId *string `json:"MessageGroupId,omitempty"`
}

// SendMessageBatchResponse defines the structure for the SQS SendMessageBatch action's output.
// It separates results into successful and failed entries.
type SendMessageBatchResponse struct {
	// Successful is a list of entries that were successfully sent.
	Successful []SendMessageBatchResultEntry `json:"Successful"`
	// Failed is a list of entries that failed to be sent.
	Failed []BatchResultErrorEntry `json:"Failed"`
}

// SendMessageBatchResultEntry contains the details of a successfully sent message in a batch.
// It mirrors the single SendMessageResponse but includes the original entry ID.
type SendMessageBatchResultEntry struct {
	// Id is the identifier of the message in the batch request.
	Id string `json:"Id"`
	// MessageId is the unique identifier assigned to the message.
	MessageId string `json:"MessageId"`
	// MD5OfMessageBody is the MD5 digest of the message body.
	MD5OfMessageBody string `json:"MD5OfMessageBody"`
	// MD5OfMessageAttributes is the MD5 digest of the message attributes.
	MD5OfMessageAttributes *string `json:"MD5OfMessageAttributes,omitempty"`
	// MD5OfMessageSystemAttributes is the MD5 digest of the message system attributes.
	MD5OfMessageSystemAttributes *string `json:"MD5OfMessageSystemAttributes,omitempty"`
	// SequenceNumber is the sequence number of the message (FIFO only).
	SequenceNumber *string `json:"SequenceNumber,omitempty"`
}

// BatchResultErrorEntry contains the details of a failed message in a batch operation.
// It includes the original ID, an error code, a message, and whether the sender was at fault.
type BatchResultErrorEntry struct {
	// Id is the identifier of the message in the batch request.
	Id string `json:"Id"`
	// Code is the error code.
	Code string `json:"Code"`
	// Message is a description of the error.
	Message string `json:"Message"`
	// SenderFault indicates whether the error was due to the sender's request.
	SenderFault bool `json:"SenderFault"`
}

// DeleteMessageBatchRequest defines the parameters for the SQS DeleteMessageBatch action.
type DeleteMessageBatchRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// Entries is a list of DeleteMessageBatchRequestEntry items.
	Entries []DeleteMessageBatchRequestEntry `json:"Entries"`
}

// DeleteMessageBatchRequestEntry defines a single message to be deleted in a batch.
type DeleteMessageBatchRequestEntry struct {
	// Id is a unique identifier for the entry within the batch.
	Id string `json:"Id"`
	// ReceiptHandle is the handle associated with the message to delete.
	ReceiptHandle string `json:"ReceiptHandle"`
}

// DeleteMessageBatchResponse defines the structure for the SQS DeleteMessageBatch action's output.
type DeleteMessageBatchResponse struct {
	// Successful is a list of entries that were successfully deleted.
	Successful []DeleteMessageBatchResultEntry `json:"Successful"`
	// Failed is a list of entries that failed to be deleted.
	Failed []BatchResultErrorEntry `json:"Failed"`
}

// DeleteMessageBatchResultEntry contains the ID of a successfully deleted message in a batch.
type DeleteMessageBatchResultEntry struct {
	// Id is the identifier of the message in the batch request.
	Id string `json:"Id"`
}

// SetQueueAttributesRequest defines the parameters for the SQS SetQueueAttributes action.
type SetQueueAttributesRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// Attributes is a map of attributes to set.
	Attributes map[string]string `json:"Attributes"`
}

// GetQueueURLRequest defines the parameters for the SQS GetQueueUrl action.
type GetQueueURLRequest struct {
	// QueueName is the name of the queue.
	QueueName string `json:"QueueName"`
	// QueueOwnerAWSAccountId is the AWS account ID of the queue owner.
	QueueOwnerAWSAccountId string `json:"QueueOwnerAWSAccountId,omitempty"`
}

// GetQueueURLResponse defines the structure for the SQS GetQueueUrl action's output.
type GetQueueURLResponse struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
}

// ChangeMessageVisibilityBatchRequest defines the parameters for the SQS ChangeMessageVisibilityBatch action.
type ChangeMessageVisibilityBatchRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// Entries is a list of ChangeMessageVisibilityBatchRequestEntry items.
	Entries []ChangeMessageVisibilityBatchRequestEntry `json:"Entries"`
}

// ChangeMessageVisibilityBatchRequestEntry defines a single entry in a ChangeMessageVisibilityBatch request.
type ChangeMessageVisibilityBatchRequestEntry struct {
	// Id is a unique identifier for the entry within the batch.
	Id string `json:"Id"`
	// ReceiptHandle is the handle associated with the message.
	ReceiptHandle string `json:"ReceiptHandle"`
	// VisibilityTimeout is the new visibility timeout in seconds.
	VisibilityTimeout int `json:"VisibilityTimeout"`
}

// ChangeMessageVisibilityBatchResponse defines the structure for the SQS ChangeMessageVisibilityBatch action's output.
type ChangeMessageVisibilityBatchResponse struct {
	// Successful is a list of entries that were successfully processed.
	Successful []ChangeMessageVisibilityBatchResultEntry `json:"Successful"`
	// Failed is a list of entries that failed.
	Failed []BatchResultErrorEntry `json:"Failed"`
}

// ChangeMessageVisibilityBatchResultEntry contains the ID of a successfully changed message in a batch.
type ChangeMessageVisibilityBatchResultEntry struct {
	// Id is the identifier of the message in the batch request.
	Id string `json:"Id"`
}

// ListQueueTagsRequest defines the parameters for the SQS ListQueueTags action.
type ListQueueTagsRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
}

// ListQueueTagsResponse defines the structure for the SQS ListQueueTags action's output.
type ListQueueTagsResponse struct {
	// Tags is a map of the queue's tags.
	Tags map[string]string `json:"Tags"`
}

// TagQueueRequest defines the parameters for the SQS TagQueue action.
type TagQueueRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// Tags is a map of tags to add to the queue.
	Tags map[string]string `json:"Tags"`
}

// TagQueueResponse defines the structure for the SQS TagQueue action's output.
type TagQueueResponse struct{}

// UntagQueueRequest defines the parameters for the SQS UntagQueue action.
type UntagQueueRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// TagKeys is a list of tag keys to remove.
	TagKeys []string `json:"TagKeys"`
}

// UntagQueueResponse defines the structure for the SQS UntagQueue action's output.
type UntagQueueResponse struct{}

// StartMessageMoveTaskRequest defines the parameters for the SQS StartMessageMoveTask action.
type StartMessageMoveTaskRequest struct {
	// SourceArn is the ARN of the queue to move messages from.
	SourceArn string `json:"SourceArn"`
	// DestinationArn is the ARN of the queue to move messages to.
	DestinationArn string `json:"DestinationArn,omitempty"`
	// MaxNumberOfMessagesPerSecond is the maximum number of messages to move per second.
	MaxNumberOfMessagesPerSecond int `json:"MaxNumberOfMessagesPerSecond,omitempty"`
}

// StartMessageMoveTaskResponse defines the structure for the SQS StartMessageMoveTask action's output.
type StartMessageMoveTaskResponse struct {
	// TaskHandle is the identifier for the started move task.
	TaskHandle string `json:"TaskHandle"`
}

// CancelMessageMoveTaskRequest defines the parameters for the SQS CancelMessageMoveTask action.
type CancelMessageMoveTaskRequest struct {
	// TaskHandle is the identifier of the task to cancel.
	TaskHandle string `json:"TaskHandle"`
}

// CancelMessageMoveTaskResponse defines the structure for the SQS CancelMessageMoveTask action's output.
type CancelMessageMoveTaskResponse struct {
	// ApproximateNumberOfMessagesMoved is the number of messages moved so far.
	ApproximateNumberOfMessagesMoved int64 `json:"ApproximateNumberOfMessagesMoved"`
	// ApproximateNumberOfMessagesToMove is the estimated number of messages remaining to move.
	ApproximateNumberOfMessagesToMove int64 `json:"ApproximateNumberOfMessagesToMove"`
	// FailureReason is the reason for task failure, if applicable.
	FailureReason string `json:"FailureReason,omitempty"`
	// SourceArn is the ARN of the source queue.
	SourceArn string `json:"SourceArn"`
	// Status is the current status of the task (e.g., "RUNNING", "COMPLETED", "CANCELLED", "FAILED").
	Status string `json:"Status"`
	// TaskHandle is the identifier of the task.
	TaskHandle string `json:"TaskHandle"`
}

// ListMessageMoveTasksRequest defines the parameters for the SQS ListMessageMoveTasks action.
type ListMessageMoveTasksRequest struct {
	// SourceArn is the ARN of the source queue.
	SourceArn string `json:"SourceArn"`
	// MaxResults is the maximum number of results to return.
	MaxResults int `json:"MaxResults,omitempty"`
}

// ListMessageMoveTasksResultEntry represents a single task in the list response.
type ListMessageMoveTasksResultEntry struct {
	// ApproximateNumberOfMessagesMoved is the number of messages moved.
	ApproximateNumberOfMessagesMoved int64 `json:"ApproximateNumberOfMessagesMoved"`
	// ApproximateNumberOfMessagesToMove is the estimated number of messages remaining.
	ApproximateNumberOfMessagesToMove int64 `json:"ApproximateNumberOfMessagesToMove"`
	// DestinationArn is the ARN of the destination queue.
	DestinationArn string `json:"DestinationArn"`
	// FailureReason is the reason for failure, if any.
	FailureReason string `json:"FailureReason,omitempty"`
	// MaxNumberOfMessagesPerSecond is the configured speed limit for the task.
	MaxNumberOfMessagesPerSecond int `json:"MaxNumberOfMessagesPerSecond,omitempty"`
	// SourceArn is the ARN of the source queue.
	SourceArn string `json:"SourceArn"`
	// StartedTimestamp is the timestamp when the task was started.
	StartedTimestamp int64 `json:"StartedTimestamp"`
	// Status is the status of the task.
	Status string `json:"Status"`
	// TaskHandle is the identifier of the task.
	TaskHandle string `json:"TaskHandle"`
}

// ListMessageMoveTasksResponse defines the structure for the SQS ListMessageMoveTasks action's output.
type ListMessageMoveTasksResponse struct {
	// Results is a list of message move tasks.
	Results []ListMessageMoveTasksResultEntry `json:"Results"`
}

type CreateTokenRequest struct {
	AccountId string `json:"AccountId"`
}

type CreateTokenResponse struct {
	Token string `json:"Token"`
}

// ListDeadLetterSourceQueuesRequest defines the parameters for the SQS ListDeadLetterSourceQueues action.
type ListDeadLetterSourceQueuesRequest struct {
	// QueueUrl is the URL of the dead-letter queue.
	QueueUrl string `json:"QueueUrl"`
	// MaxResults is the maximum number of results to return.
	MaxResults int `json:"MaxResults"`
	// NextToken is the token to retrieve the next page of results.
	NextToken string `json:"NextToken"`
}

// ListDeadLetterSourceQueuesResponse defines the structure for the SQS ListDeadLetterSourceQueues action's output.
type ListDeadLetterSourceQueuesResponse struct {
	// QueueUrls is a list of URLs of the queues that use the specified queue as a dead-letter queue.
	QueueUrls []string `json:"queueUrls"`
	// NextToken is the token to use for the next ListDeadLetterSourceQueues request.
	NextToken string `json:"NextToken,omitempty"`
}

// AddPermissionRequest defines the parameters for the SQS AddPermission action.
type AddPermissionRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// Label is a unique identifier for the permission statement.
	Label string `json:"Label"`
	// AWSAccountIds is a list of AWS account IDs to grant permission to.
	AWSAccountIds []string `json:"AWSAccountIds"`
	// Actions is a list of actions to allow (e.g., "SendMessage").
	Actions []string `json:"Actions"`
}

// RemovePermissionRequest defines the parameters for the SQS RemovePermission action.
type RemovePermissionRequest struct {
	// QueueUrl is the URL of the queue.
	QueueUrl string `json:"QueueUrl"`
	// Label is the unique identifier of the permission statement to remove.
	Label string `json:"Label"`
}
