package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/store"

	"github.com/go-chi/chi/v5"
)

// App encapsulates the application's dependencies.
type App struct {
	Store store.Store
}

// RegisterSQSHandlers registers all SQS API handlers with the given Chi router.
func (app *App) RegisterSQSHandlers(r *chi.Mux) {
	// Root handler for RPC-style requests
	r.Post("/", app.RootSQSHandler)

	// Legacy REST-ful routes that can be phased out
	// r.Post("/queues", app.CreateQueueHandler)
	// r.Delete("/queues/{queueName}", app.DeleteQueueHandler)
	// r.Get("/queues", app.ListQueuesHandler)
	// r.Post("/queues/{queueName}/purge", app.PurgeQueueHandler)

	// Message Management
	r.Post("/queues/{queueName}/messages/batch", app.SendMessageBatchHandler)
	r.Get("/queues/{queueName}/messages", app.ReceiveMessageHandler)
	r.Delete("/queues/{queueName}/messages/{receiptHandle}", app.DeleteMessageHandler)
	r.Post("/queues/{queueName}/messages/batch-delete", app.DeleteMessageBatchHandler)
	r.Patch("/queues/{queueName}/messages/{receiptHandle}", app.ChangeMessageVisibilityHandler)
	r.Post("/queues/{queueName}/messages/batch-visibility", app.ChangeMessageVisibilityBatchHandler)

	// Permissions
	r.Post("/queues/{queueName}/permissions", app.AddPermissionHandler)
	r.Delete("/queues/{queueName}/permissions/{label}", app.RemovePermissionHandler)

	// Tagging
	r.Get("/queues/{queueName}/tags", app.ListQueueTagsHandler)
	r.Post("/queues/{queueName}/tags", app.TagQueueHandler)
	r.Delete("/queues/{queueName}/tags", app.UntagQueueHandler)

	// Dead-Letter Queues
	r.Get("/dead-letter-source-queues", app.ListDeadLetterSourceQueuesHandler)

	// Message Move Tasks
	r.Post("/message-move-tasks", app.StartMessageMoveTaskHandler)
	r.Post("/message-move-tasks/{taskHandle}/cancel", app.CancelMessageMoveTaskHandler)
	r.Get("/message-move-tasks", app.ListMessageMoveTasksHandler)
}

// RootSQSHandler acts as a dispatcher for RPC-style SQS requests.
func (app *App) RootSQSHandler(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Amz-Target")

	parts := strings.Split(target, ".")
	if len(parts) != 2 || parts[0] != "AmazonSQS" {
		http.Error(w, "Invalid X-Amz-Target header", http.StatusBadRequest)
		return
	}
	action := parts[1]

	switch action {
	case "SendMessage":
		app.SendMessageHandler(w, r)
	case "CreateQueue":
		app.CreateQueueHandler(w, r)
	case "DeleteQueue":
		app.DeleteQueueHandler(w, r)
	case "ListQueues":
		app.ListQueuesHandler(w, r)
	case "PurgeQueue":
		app.PurgeQueueHandler(w, r)
	default:
		http.Error(w, "Unsupported operation: "+action, http.StatusBadRequest)
	}
}


// SQS queue name validation regex.
// A queue name can have up to 80 characters.
// Valid values: alphanumeric characters, hyphens (-), and underscores (_).
// For FIFO queues, the name must end with the .fifo suffix.
var queueNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}(\.fifo)?$`)

// validateIntAttribute checks if a string attribute can be parsed as an int and is within the given range.
func validateIntAttribute(valStr string, min, max int) error {
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return fmt.Errorf("must be an integer")
	}
	if val < min || val > max {
		return fmt.Errorf("must be between %d and %d", min, max)
	}
	return nil
}

// validateAttributes checks the special SQS attributes for valid values.
func validateAttributes(attributes map[string]string) error {
	for key, val := range attributes {
		var err error
		switch key {
		case "DelaySeconds":
			err = validateIntAttribute(val, 0, 900)
		case "MaximumMessageSize":
			err = validateIntAttribute(val, 1024, 262144)
		case "MessageRetentionPeriod":
			err = validateIntAttribute(val, 60, 1209600)
		case "ReceiveMessageWaitTimeSeconds":
			err = validateIntAttribute(val, 0, 20)
		case "VisibilityTimeout":
			err = validateIntAttribute(val, 0, 43200)
		case "FifoQueue", "ContentBasedDeduplication":
			if val != "true" && val != "false" {
				err = fmt.Errorf("must be 'true' or 'false'")
			}
		case "RedrivePolicy":
			var policy struct {
				DeadLetterTargetArn string `json:"deadLetterTargetArn"`
				MaxReceiveCount     string `json:"maxReceiveCount"`
			}
			if jsonErr := json.Unmarshal([]byte(val), &policy); jsonErr != nil {
				err = errors.New("must be a valid JSON object")
			} else {
				if policy.DeadLetterTargetArn == "" {
					err = errors.New("deadLetterTargetArn is required")
				}
				if count, convErr := strconv.Atoi(policy.MaxReceiveCount); convErr != nil || count < 1 || count > 1000 {
					err = errors.New("maxReceiveCount must be an integer between 1 and 1000")
				}
			}
		case "DeduplicationScope":
			if val != "messageGroup" && val != "queue" {
				err = fmt.Errorf("must be 'messageGroup' or 'queue'")
			}
		case "FifoThroughputLimit":
			if val != "perQueue" && val != "perMessageGroupId" {
				err = fmt.Errorf("must be 'perQueue' or 'perMessageGroupId'")
			}
		}
		if err != nil {
			return fmt.Errorf("invalid value for %s: %w", key, err)
		}
	}
	return nil
}

// CreateQueueHandler handles requests to create a new queue.
func (app *App) CreateQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.CreateQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate queue name format
	if !queueNameRegex.MatchString(req.QueueName) {
		http.Error(w, "Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length.", http.StatusBadRequest)
		return
	}

	// Validate FIFO queue naming conventions
	isFifo := strings.HasSuffix(req.QueueName, ".fifo")
	fifoAttr, fifoAttrExists := req.Attributes["FifoQueue"]

	if isFifo && (!fifoAttrExists || fifoAttr != "true") {
		http.Error(w, "Queue name ends in .fifo but FifoQueue attribute is not 'true'", http.StatusBadRequest)
		return
	}
	if !isFifo && fifoAttrExists && fifoAttr == "true" {
		http.Error(w, "FifoQueue attribute is 'true' but queue name does not end in .fifo", http.StatusBadRequest)
		return
	}

	// Validate special attributes
	if err := validateAttributes(req.Attributes); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// FIFO-specific attribute validation
	if !isFifo {
		if _, exists := req.Attributes["DeduplicationScope"]; exists {
			http.Error(w, "DeduplicationScope is only valid for FIFO queues", http.StatusBadRequest)
			return
		}
		if _, exists := req.Attributes["FifoThroughputLimit"]; exists {
			http.Error(w, "FifoThroughputLimit is only valid for FIFO queues", http.StatusBadRequest)
			return
		}
	}

	// High-throughput FIFO validation
	if scope, exists := req.Attributes["DeduplicationScope"]; exists {
		if limit, limitExists := req.Attributes["FifoThroughputLimit"]; limitExists {
			if limit == "perMessageGroupId" && scope != "messageGroup" {
				http.Error(w, "FifoThroughputLimit can be set to perMessageGroupId only when DeduplicationScope is messageGroup", http.StatusBadRequest)
				return
			}
		}
	}

	err := app.Store.CreateQueue(r.Context(), req.QueueName, req.Attributes, req.Tags)
	if err != nil {
		if errors.Is(err, store.ErrQueueAlreadyExists) {
			http.Error(w, "Queue already exists", http.StatusConflict)
			return
		}
		http.Error(w, "Failed to create queue", http.StatusInternalServerError)
		return
	}

	// Construct the queue URL
	// In a real application, this would be based on the request's host and scheme.
	queueURL := fmt.Sprintf("http://localhost:8080/queues/%s", req.QueueName)

	resp := models.CreateQueueResponse{
		QueueURL: queueURL,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

// DeleteQueueHandler handles requests to delete a queue.
func (app *App) DeleteQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.DeleteQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueueUrl == "" {
		http.Error(w, "MissingParameter: The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	err := app.Store.DeleteQueue(r.Context(), queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			// SQS returns a 400 status code for this error.
			http.Error(w, "QueueDoesNotExist: The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		http.Error(w, "Failed to delete queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
func (app *App) ListQueuesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListQueuesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// SQS allows this to be a GET or POST, so an empty body is fine.
	}

	// Call store to get queue names
	queueNames, newNextToken, err := app.Store.ListQueues(r.Context(), req.MaxResults, req.NextToken, req.QueueNamePrefix)
	if err != nil {
		http.Error(w, "Failed to list queues", http.StatusInternalServerError)
		return
	}

	// If MaxResults was not specified, limit to 1000 results.
	// The store doesn't return a next token in this case, which is correct.
	if req.MaxResults == 0 && len(queueNames) > 1000 {
		queueNames = queueNames[:1000]
	}

	// Construct full queue URLs
	queueURLs := make([]string, len(queueNames))
	for i, name := range queueNames {
		queueURLs[i] = fmt.Sprintf("http://localhost:8080/queues/%s", name)
	}

	// Create response
	resp := models.ListQueuesResponse{
		QueueUrls: queueURLs,
		NextToken: newNextToken,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
func (app *App) GetQueueAttributesHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) SetQueueAttributesHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) GetQueueUrlHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) PurgeQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.PurgeQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueueUrl == "" {
		http.Error(w, "MissingParameter: The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	err := app.Store.PurgeQueue(r.Context(), queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			http.Error(w, "QueueDoesNotExist: The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrPurgeQueueInProgress) {
			http.Error(w, "PurgeQueueInProgress: Indicates that the specified queue previously received a PurgeQueue request within the last 60 seconds.", http.StatusBadRequest)
			return
		}
		http.Error(w, "Failed to purge queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// isValidSqsChars checks if a string contains only valid SQS characters.
// Alphanumeric characters, and !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
func isValidSqsChars(s string) bool {
	for _, r := range s {
		isAlphanumeric := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		isPunctuation := strings.ContainsRune("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", r)
		if !isAlphanumeric && !isPunctuation {
			return false
		}
	}
	return true
}

func (app *App) SendMessageHandler(w http.ResponseWriter, r *http.Request) {
	var req models.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// --- Comprehensive Validation ---

	// QueueUrl
	if req.QueueUrl == "" {
		http.Error(w, "MissingParameter: The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	// MessageBody
	if len(req.MessageBody) < 1 || len(req.MessageBody) > 256*1024 { // 1 char to 256 KiB
		http.Error(w, "InvalidParameterValue: The message body must be between 1 and 262144 bytes long.", http.StatusBadRequest)
		return
	}

	// DelaySeconds
	if req.DelaySeconds != nil {
		if *req.DelaySeconds < 0 || *req.DelaySeconds > 900 {
			http.Error(w, "InvalidParameterValue: Value for parameter DelaySeconds is invalid. Reason: Must be an integer from 0 to 900.", http.StatusBadRequest)
			return
		}
		if strings.HasSuffix(queueName, ".fifo") {
			http.Error(w, "InvalidParameterValue: The request include parameter that is not valid for this queue type. Reason: DelaySeconds is not supported for FIFO queues.", http.StatusBadRequest)
			return
		}
	}

	isFifo := strings.HasSuffix(queueName, ".fifo")

	// MessageDeduplicationId
	if req.MessageDeduplicationId != nil {
		if !isFifo {
			http.Error(w, "InvalidParameterValue: MessageDeduplicationId is supported only for FIFO queues.", http.StatusBadRequest)
			return
		}
		if len(*req.MessageDeduplicationId) > 128 {
			http.Error(w, "InvalidParameterValue: MessageDeduplicationId can be up to 128 characters long.", http.StatusBadRequest)
			return
		}
		if !isValidSqsChars(*req.MessageDeduplicationId) {
			http.Error(w, "InvalidParameterValue: MessageDeduplicationId can only contain alphanumeric characters and punctuation.", http.StatusBadRequest)
			return
		}
	}

	// MessageGroupId
	if req.MessageGroupId != nil {
		if len(*req.MessageGroupId) > 128 {
			http.Error(w, "InvalidParameterValue: MessageGroupId can be up to 128 characters long.", http.StatusBadRequest)
			return
		}
		if !isValidSqsChars(*req.MessageGroupId) {
			http.Error(w, "InvalidParameterValue: MessageGroupId can only contain alphanumeric characters and punctuation.", http.StatusBadRequest)
			return
		}
	}
	if isFifo && req.MessageGroupId == nil {
		http.Error(w, "MissingParameter: The request must contain a MessageGroupId.", http.StatusBadRequest)
		return
	}

	// MessageAttributes
	if len(req.MessageAttributes) > 10 {
		http.Error(w, "InvalidParameterValue: Number of message attributes cannot exceed 10.", http.StatusBadRequest)
		return
	}
	for name, attr := range req.MessageAttributes {
		if len(name) == 0 {
			http.Error(w, "InvalidParameterValue: Message attribute name cannot be empty.", http.StatusBadRequest)
			return
		}
		if attr.DataType == "" {
			http.Error(w, "InvalidParameterValue: DataType of message attribute '"+name+"' is required.", http.StatusBadRequest)
			return
		}
	}

	// MessageSystemAttributes
	for name, attr := range req.MessageSystemAttributes {
		if name != "AWSTraceHeader" {
			http.Error(w, "InvalidParameterValue: '"+name+"' is not a valid message system attribute.", http.StatusBadRequest)
			return
		}
		if attr.DataType != "String" {
			http.Error(w, "InvalidParameterValue: DataType of AWSTraceHeader must be String.", http.StatusBadRequest)
			return
		}
	}


	// Call the store to send the message
	resp, err := app.Store.SendMessage(r.Context(), queueName, &req)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			http.Error(w, "QueueDoesNotExist: The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		http.Error(w, "Failed to send message", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
func (app *App) SendMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) ReceiveMessageHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) DeleteMessageHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) DeleteMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) ChangeMessageVisibilityHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) ChangeMessageVisibilityBatchHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) AddPermissionHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) RemovePermissionHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) ListQueueTagsHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) TagQueueHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) UntagQueueHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) ListDeadLetterSourceQueuesHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) StartMessageMoveTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) CancelMessageMoveTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) ListMessageMoveTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
