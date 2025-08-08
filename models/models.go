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
