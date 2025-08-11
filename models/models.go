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
}
