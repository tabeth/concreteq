package models

// CreateQueueRequest represents the request body for creating a queue.
type CreateQueueRequest struct {
	QueueName  string            `json:"QueueName"`
	Attributes map[string]string `json:"Attributes"`
	Tags       map[string]string `json:"tags"`
}

// CreateQueueResponse represents the response body for a successful queue creation.
type CreateQueueResponse struct {
	QueueURL string `json:"QueueUrl"`
}

// ListQueuesRequest defines the parameters for listing queues.
type ListQueuesRequest struct {
	MaxResults      int    `json:"MaxResults"`
	NextToken       string `json:"NextToken"`
	QueueNamePrefix string `json:"QueueNamePrefix"`
}

// ListQueuesResponse defines the structure for the list queues response.
type ListQueuesResponse struct {
	QueueUrls []string `json:"QueueUrls"`
	NextToken string   `json:"NextToken,omitempty"`
}

// MessageAttributeValue represents the value of a message attribute.
type MessageAttributeValue struct {
	BinaryListValues [][]byte `json:"BinaryListValues,omitempty"`
	BinaryValue      []byte   `json:"BinaryValue,omitempty"`
	DataType         string   `json:"DataType"`
	StringListValues []string `json:"StringListValues,omitempty"`
	StringValue      *string  `json:"StringValue,omitempty"`
}

// MessageSystemAttributeValue represents the value of a message system attribute.
type MessageSystemAttributeValue struct {
	BinaryListValues [][]byte `json:"BinaryListValues,omitempty"`
	BinaryValue      []byte   `json:"BinaryValue,omitempty"`
	DataType         string   `json:"DataType"`
	StringListValues []string `json:"StringListValues,omitempty"`
	StringValue      *string  `json:"StringValue,omitempty"`
}

// SendMessageRequest represents the request body for sending a message.
type SendMessageRequest struct {
	DelaySeconds            *int32                             `json:"DelaySeconds,omitempty"`
	MessageAttributes       map[string]MessageAttributeValue   `json:"MessageAttributes,omitempty"`
	MessageBody             string                             `json:"MessageBody"`
	MessageDeduplicationId  *string                            `json:"MessageDeduplicationId,omitempty"`
	MessageGroupId          *string                            `json:"MessageGroupId,omitempty"`
	MessageSystemAttributes map[string]MessageSystemAttributeValue `json:"MessageSystemAttributes,omitempty"`
	QueueUrl                string                             `json:"QueueUrl"`
}

// SendMessageResponse represents the response body for a successful message send.
type SendMessageResponse struct {
	MD5OfMessageAttributes       *string `json:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageBody             string  `json:"MD5OfMessageBody"`
	MD5OfMessageSystemAttributes *string `json:"MD5OfMessageSystemAttributes,omitempty"`
	MessageId                    string  `json:"MessageId"`
	SequenceNumber               *string `json:"SequenceNumber,omitempty"`
}

// Message represents a message in the queue. This is for internal storage.
type Message struct {
	ID                 string
	Body               string
	Attributes         map[string]MessageAttributeValue
	SystemAttributes   map[string]MessageSystemAttributeValue
	MD5OfBody          string
	MD5OfAttributes    string
	MD5OfSysAttributes string
	VisibleAfter       int64
	ReceivedCount      int
	FirstReceived      int64
	// New fields for FIFO and system attributes
	SenderId       string `json:"SenderId,omitempty"`
	SentTimestamp  int64  `json:"SentTimestamp,omitempty"`
	MessageGroupId string `json:"MessageGroupId,omitempty"`
	SequenceNumber int64  `json:"SequenceNumber,omitempty"`
}

// DeleteQueueRequest defines the parameters for deleting a queue.
type DeleteQueueRequest struct {
	QueueUrl string `json:"QueueUrl"`
}

// PurgeQueueRequest defines the parameters for purging a queue.
type PurgeQueueRequest struct {
	QueueUrl string `json:"QueueUrl"`
}

// ReceiveMessageRequest defines the parameters for receiving messages from a queue.
type ReceiveMessageRequest struct {
	AttributeNames            []string `json:"AttributeNames"`
	MaxNumberOfMessages       int      `json:"MaxNumberOfMessages"`
	MessageAttributeNames     []string `json:"MessageAttributeNames"`
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames"`
	QueueUrl                  string   `json:"QueueUrl"`
	ReceiveRequestAttemptId   string   `json:"ReceiveRequestAttemptId"`
	VisibilityTimeout         int      `json:"VisibilityTimeout"`
	WaitTimeSeconds           int      `json:"WaitTimeSeconds"`
}

// ReceiveMessageResponse defines the structure for the receive message response.
type ReceiveMessageResponse struct {
	Messages []ResponseMessage `json:"Messages"`
}

// ResponseMessage represents a single message returned by ReceiveMessage.
type ResponseMessage struct {
	Attributes             map[string]string         `json:"Attributes"`
	Body                   string                    `json:"Body"`
	MD5OfBody              string                    `json:"MD5OfBody"`
	MD5OfMessageAttributes *string                   `json:"MD5OfMessageAttributes,omitempty"`
	MessageAttributes      map[string]MessageAttributeValue `json:"MessageAttributes,omitempty"`
	MessageId              string                    `json:"MessageId"`
	ReceiptHandle          string                    `json:"ReceiptHandle"`
}

// DeleteMessageRequest defines the parameters for deleting a message.
type DeleteMessageRequest struct {
	QueueUrl      string `json:"QueueUrl"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

// ErrorResponse defines the standard AWS JSON error response format.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// SendMessageBatchRequest defines the parameters for SendMessageBatch action.
type SendMessageBatchRequest struct {
	QueueUrl string                         `json:"QueueUrl"`
	Entries  []SendMessageBatchRequestEntry `json:"Entries"`
}

// SendMessageBatchRequestEntry defines a single message to be sent in a batch.
type SendMessageBatchRequestEntry struct {
	Id                      string                             `json:"Id"`
	MessageBody             string                             `json:"MessageBody"`
	DelaySeconds            *int32                             `json:"DelaySeconds,omitempty"`
	MessageAttributes       map[string]MessageAttributeValue   `json:"MessageAttributes,omitempty"`
	MessageSystemAttributes map[string]MessageSystemAttributeValue `json:"MessageSystemAttributes,omitempty"`
	MessageDeduplicationId  *string                            `json:"MessageDeduplicationId,omitempty"`
	MessageGroupId          *string                            `json:"MessageGroupId,omitempty"`
}

// SendMessageBatchResponse defines the structure for the SendMessageBatch response.
type SendMessageBatchResponse struct {
	Successful []SendMessageBatchResultEntry `json:"Successful"`
	Failed     []BatchResultErrorEntry       `json:"Failed"`
}

// SendMessageBatchResultEntry contains the details of a successfully sent message in a batch.
type SendMessageBatchResultEntry struct {
	Id                           string  `json:"Id"`
	MessageId                    string  `json:"MessageId"`
	MD5OfMessageBody             string  `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes       *string `json:"MD5OfMessageAttributes,omitempty"`
	MD5OfMessageSystemAttributes *string `json:"MD5OfMessageSystemAttributes,omitempty"`
	SequenceNumber               *string `json:"SequenceNumber,omitempty"`
}

// BatchResultErrorEntry contains the details of a failed message in a batch.
type BatchResultErrorEntry struct {
	Id          string `json:"Id"`
	Code        string `json:"Code"`
	Message     string `json:"Message"`
	SenderFault bool   `json:"SenderFault"`
}
