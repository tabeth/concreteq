package models

// CreateQueueRequest represents the request body for creating a queue.
type CreateQueueRequest struct {
	QueueName string `json:"QueueName"`
}

// CreateQueueResponse represents the response body for a successful queue creation.
type CreateQueueResponse struct {
	QueueURL string `json:"QueueUrl"`
}
