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
