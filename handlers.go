package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// sendErrorResponse is a helper to send a JSON-formatted AWS error.
func (app *App) sendErrorResponse(w http.ResponseWriter, errorType string, message string, statusCode int) {
	errResp := models.ErrorResponse{
		Type:    errorType,
		Message: message,
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(errResp)
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
	// r.Get("/queues/{queueName}/messages", app.ReceiveMessageHandler) // This is now handled by the RootSQSHandler
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
		app.sendErrorResponse(w, "InvalidAction", "Invalid X-Amz-Target header", http.StatusBadRequest)
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
	case "ReceiveMessage":
		app.ReceiveMessageHandler(w, r)
	case "DeleteMessage":
		app.DeleteMessageHandler(w, r)
	default:
		app.sendErrorResponse(w, "UnsupportedOperation", "Unsupported operation: "+action, http.StatusBadRequest)
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
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate queue name format
	if !queueNameRegex.MatchString(req.QueueName) {
		app.sendErrorResponse(w, "InvalidParameterValue", "Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length.", http.StatusBadRequest)
		return
	}

	// Validate FIFO queue naming conventions
	isFifo := strings.HasSuffix(req.QueueName, ".fifo")
	fifoAttr, fifoAttrExists := req.Attributes["FifoQueue"]

	if isFifo && (!fifoAttrExists || fifoAttr != "true") {
		app.sendErrorResponse(w, "InvalidParameterValue", "Queue name ends in .fifo but FifoQueue attribute is not 'true'", http.StatusBadRequest)
		return
	}
	if !isFifo && fifoAttrExists && fifoAttr == "true" {
		app.sendErrorResponse(w, "InvalidParameterValue", "FifoQueue attribute is 'true' but queue name does not end in .fifo", http.StatusBadRequest)
		return
	}

	// Validate special attributes
	if err := validateAttributes(req.Attributes); err != nil {
		app.sendErrorResponse(w, "InvalidAttributeName", err.Error(), http.StatusBadRequest)
		return
	}

	// FIFO-specific attribute validation
	if !isFifo {
		if _, exists := req.Attributes["DeduplicationScope"]; exists {
			app.sendErrorResponse(w, "InvalidParameterValue", "DeduplicationScope is only valid for FIFO queues", http.StatusBadRequest)
			return
		}
		if _, exists := req.Attributes["FifoThroughputLimit"]; exists {
			app.sendErrorResponse(w, "InvalidParameterValue", "FifoThroughputLimit is only valid for FIFO queues", http.StatusBadRequest)
			return
		}
	}

	// High-throughput FIFO validation
	if scope, exists := req.Attributes["DeduplicationScope"]; exists {
		if limit, limitExists := req.Attributes["FifoThroughputLimit"]; limitExists {
			if limit == "perMessageGroupId" && scope != "messageGroup" {
				app.sendErrorResponse(w, "InvalidParameterValue", "FifoThroughputLimit can be set to perMessageGroupId only when DeduplicationScope is messageGroup", http.StatusBadRequest)
				return
			}
		}
	}

	err := app.Store.CreateQueue(r.Context(), req.QueueName, req.Attributes, req.Tags)
	if err != nil {
		if errors.Is(err, store.ErrQueueAlreadyExists) {
			app.sendErrorResponse(w, "QueueAlreadyExists", "Queue already exists", http.StatusConflict)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to create queue", http.StatusInternalServerError)
		return
	}

	// Construct the queue URL dynamically from the request host.
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	queueURL := fmt.Sprintf("%s://%s/queues/%s", scheme, r.Host, req.QueueName)

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
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	err := app.Store.DeleteQueue(r.Context(), queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to delete queue", http.StatusInternalServerError)
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
		app.sendErrorResponse(w, "InternalFailure", "Failed to list queues", http.StatusInternalServerError)
		return
	}

	// If MaxResults was not specified, limit to 1000 results.
	// The store doesn't return a next token in this case, which is correct.
	if req.MaxResults == 0 && len(queueNames) > 1000 {
		queueNames = queueNames[:1000]
	}

	// Construct full queue URLs
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	queueURLs := make([]string, len(queueNames))
	for i, name := range queueNames {
		queueURLs[i] = fmt.Sprintf("%s://%s/queues/%s", scheme, r.Host, name)
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
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	err := app.Store.PurgeQueue(r.Context(), queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrPurgeQueueInProgress) {
			app.sendErrorResponse(w, "PurgeQueueInProgress", "Indicates that the specified queue previously received a PurgeQueue request within the last 60 seconds.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to purge queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

var validMessageSystemAttributeNames = map[string]bool{
	"All":                              true,
	"SenderId":                         true,
	"SentTimestamp":                    true,
	"ApproximateReceiveCount":          true,
	"ApproximateFirstReceiveTimestamp": true,
	"SequenceNumber":                   true,
	"MessageDeduplicationId":           true,
	"MessageGroupId":                   true,
	"AWSTraceHeader":                   true,
	"DeadLetterQueueSourceArn":         true,
}

// isValidMessageAttributeName validates the format of a custom message attribute name.
func isValidMessageAttributeName(name string) bool {
	if len(name) > 256 {
		return false
	}
	if strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") {
		return false
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	// The name can contain alphanumeric characters and the underscore (_), hyphen (-), and period (.).
	match, _ := regexp.MatchString(`^[a-zA-Z0-9_.-]+$`, name)
	return match
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
	// 1. Read the entire body into a byte slice first.
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	// 2. Unmarshal into a generic map to bypass potential struct unmarshaling bugs.
	var data map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid JSON format", http.StatusBadRequest)
		return
	}

	// 3. Manually construct the SendMessageRequest from the map.
	// This provides a safe intermediate step.
	var req models.SendMessageRequest
	if qURL, ok := data["QueueUrl"].(string); ok {
		req.QueueUrl = qURL
	}
	if mBody, ok := data["MessageBody"].(string); ok {
		req.MessageBody = mBody
	}
	if delay, ok := data["DelaySeconds"].(float64); ok {
		delay32 := int32(delay)
		req.DelaySeconds = &delay32
	}
	if dedupID, ok := data["MessageDeduplicationId"].(string); ok {
		req.MessageDeduplicationId = &dedupID
	}
	if groupID, ok := data["MessageGroupId"].(string); ok {
		req.MessageGroupId = &groupID
	}
	// Note: A full implementation would also handle MessageAttributes and MessageSystemAttributes.
	// For now, we are only handling the fields required for validation logic to pass.

	// --- Comprehensive Validation ---

	// QueueUrl
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	// MessageBody
	if len(req.MessageBody) < 1 || len(req.MessageBody) > 256*1024 { // 1 char to 256 KiB
		app.sendErrorResponse(w, "InvalidParameterValue", "The message body must be between 1 and 262144 bytes long.", http.StatusBadRequest)
		return
	}

	// DelaySeconds
	if req.DelaySeconds != nil {
		if *req.DelaySeconds < 0 || *req.DelaySeconds > 900 {
			app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter DelaySeconds is invalid. Reason: Must be an integer from 0 to 900.", http.StatusBadRequest)
			return
		}
		if strings.HasSuffix(queueName, ".fifo") {
			app.sendErrorResponse(w, "InvalidParameterValue", "The request include parameter that is not valid for this queue type. Reason: DelaySeconds is not supported for FIFO queues.", http.StatusBadRequest)
			return
		}
	}

	isFifo := strings.HasSuffix(queueName, ".fifo")

	// MessageDeduplicationId
	if req.MessageDeduplicationId != nil {
		if !isFifo {
			app.sendErrorResponse(w, "InvalidParameterValue", "MessageDeduplicationId is supported only for FIFO queues.", http.StatusBadRequest)
			return
		}
		if len(*req.MessageDeduplicationId) > 128 {
			app.sendErrorResponse(w, "InvalidParameterValue", "MessageDeduplicationId can be up to 128 characters long.", http.StatusBadRequest)
			return
		}
		if !isValidSqsChars(*req.MessageDeduplicationId) {
			app.sendErrorResponse(w, "InvalidParameterValue", "MessageDeduplicationId can only contain alphanumeric characters and punctuation.", http.StatusBadRequest)
			return
		}
	}

	// MessageGroupId
	if req.MessageGroupId != nil {
		if len(*req.MessageGroupId) > 128 {
			app.sendErrorResponse(w, "InvalidParameterValue", "MessageGroupId can be up to 128 characters long.", http.StatusBadRequest)
			return
		}
		if !isValidSqsChars(*req.MessageGroupId) {
			app.sendErrorResponse(w, "InvalidParameterValue", "MessageGroupId can only contain alphanumeric characters and punctuation.", http.StatusBadRequest)
			return
		}
	}
	if isFifo && req.MessageGroupId == nil {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a MessageGroupId.", http.StatusBadRequest)
		return
	}

	// MessageAttributes
	if len(req.MessageAttributes) > 10 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Number of message attributes cannot exceed 10.", http.StatusBadRequest)
		return
	}
	for name, attr := range req.MessageAttributes {
		if len(name) == 0 {
			app.sendErrorResponse(w, "InvalidParameterValue", "Message attribute name cannot be empty.", http.StatusBadRequest)
			return
		}
		if attr.DataType == "" {
			app.sendErrorResponse(w, "InvalidParameterValue", "DataType of message attribute '"+name+"' is required.", http.StatusBadRequest)
			return
		}
	}

	// MessageSystemAttributes
	for name, attr := range req.MessageSystemAttributes {
		if name != "AWSTraceHeader" {
			app.sendErrorResponse(w, "InvalidParameterValue", "'"+name+"' is not a valid message system attribute.", http.StatusBadRequest)
			return
		}
		if attr.DataType != "String" {
			app.sendErrorResponse(w, "InvalidParameterValue", "DataType of AWSTraceHeader must be String.", http.StatusBadRequest)
			return
		}
	}


	// Call the store to send the message
	resp, err := app.Store.SendMessage(r.Context(), queueName, &req)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to send message", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
func (app *App) SendMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	var req models.SendMessageBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// --- Comprehensive Validation ---

	// QueueUrl
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}

	// EmptyBatchRequest
	if len(req.Entries) == 0 {
		app.sendErrorResponse(w, "EmptyBatchRequest", "The batch request doesn't contain any entries.", http.StatusBadRequest)
		return
	}

	// TooManyEntriesInBatchRequest
	if len(req.Entries) > 10 {
		app.sendErrorResponse(w, "TooManyEntriesInBatchRequest", "The batch request contains more entries than permissible.", http.StatusBadRequest)
		return
	}

	// BatchEntryIdsNotDistinct and other per-entry validation
	ids := make(map[string]struct{})
	totalPayloadSize := 0
	queueName := chi.URLParam(r, "queueName")

	for _, entry := range req.Entries {
		// Check for distinct IDs
		if _, exists := ids[entry.Id]; exists {
			app.sendErrorResponse(w, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.", http.StatusBadRequest)
			return
		}
		ids[entry.Id] = struct{}{}

		// Validate ID format (basic check)
		if len(entry.Id) > 80 || !isValidSqsChars(entry.Id) {
			app.sendErrorResponse(w, "InvalidBatchEntryId", "The Id of a batch entry in a batch request doesn't abide by the specification.", http.StatusBadRequest)
			return
		}

		// Validate message body size
		if len(entry.MessageBody) > 256*1024 {
			app.sendErrorResponse(w, "InvalidParameterValue", fmt.Sprintf("Message body for entry with Id '%s' is too long.", entry.Id), http.StatusBadRequest)
			return
		}
		totalPayloadSize += len(entry.MessageBody)

		// Per-entry validation that can be handled by the store layer for partial success
		// is now moved to the store. We only perform batch-level validation here.
	}

	// BatchRequestTooLong
	if totalPayloadSize > 256*1024 {
		app.sendErrorResponse(w, "BatchRequestTooLong", "The length of all the messages put together is more than the limit.", http.StatusBadRequest)
		return
	}

	// Call the store to send the message batch
	resp, err := app.Store.SendMessageBatch(r.Context(), queueName, &req)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		// For other errors, the store layer might have already populated the "Failed" part of the response.
		// If the error is a catastrophic transaction failure, we might need to construct a failure response for all entries.
		// For now, we assume the store handles partial failures gracefully.
		app.sendErrorResponse(w, "InternalFailure", "Failed to send message batch", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
func (app *App) ReceiveMessageHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ReceiveMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate QueueUrl
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	// Validate MaxNumberOfMessages
	if req.MaxNumberOfMessages < 1 || req.MaxNumberOfMessages > 10 {
		// If the parameter is not specified, it defaults to 1, which is valid.
		// SQS error message for out-of-range value.
		if req.MaxNumberOfMessages != 0 {
			app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter MaxNumberOfMessages is invalid. Reason: Must be an integer from 1 to 10.", http.StatusBadRequest)
			return
		}
	}

	// Validate WaitTimeSeconds
	if req.WaitTimeSeconds < 0 || req.WaitTimeSeconds > 20 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter WaitTimeSeconds is invalid. Reason: Must be an integer from 0 to 20.", http.StatusBadRequest)
		return
	}

	// Validate VisibilityTimeout
	if req.VisibilityTimeout < 0 || req.VisibilityTimeout > 43200 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter VisibilityTimeout is invalid. Reason: Must be an integer from 0 to 43200.", http.StatusBadRequest)
		return
	}

	// Validate ReceiveRequestAttemptId
	if req.ReceiveRequestAttemptId != "" {
		if len(req.ReceiveRequestAttemptId) > 128 {
			app.sendErrorResponse(w, "InvalidParameterValue", "ReceiveRequestAttemptId can be up to 128 characters long.", http.StatusBadRequest)
			return
		}
		if !isValidSqsChars(req.ReceiveRequestAttemptId) {
			app.sendErrorResponse(w, "InvalidParameterValue", "ReceiveRequestAttemptId can only contain alphanumeric characters and punctuation.", http.StatusBadRequest)
			return
		}
	}

	// Validate MessageSystemAttributeNames and AttributeNames
	allSystemAttributes := append(req.MessageSystemAttributeNames, req.AttributeNames...)
	for _, name := range allSystemAttributes {
		if !validMessageSystemAttributeNames[name] {
			app.sendErrorResponse(w, "InvalidAttributeName", "The attribute '"+name+"' is not supported.", http.StatusBadRequest)
			return
		}
	}

	// Validate MessageAttributeNames
	for _, name := range req.MessageAttributeNames {
		if !isValidMessageAttributeName(name) {
			// A more specific error could be returned depending on why it's invalid
			app.sendErrorResponse(w, "InvalidAttributeName", "The attribute name '"+name+"' is invalid.", http.StatusBadRequest)
			return
		}
	}

	// Call the store to receive messages
	resp, err := app.Store.ReceiveMessage(r.Context(), queueName, &req)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to receive messages", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
func (app *App) DeleteMessageHandler(w http.ResponseWriter, r *http.Request) {
	var req models.DeleteMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	if req.ReceiptHandle == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a ReceiptHandle.", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)

	err := app.Store.DeleteMessage(r.Context(), queueName, req.ReceiptHandle)
	if err != nil {
		if errors.Is(err, store.ErrInvalidReceiptHandle) {
			app.sendErrorResponse(w, "ReceiptHandleIsInvalid", "The specified receipt handle isn't valid.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			// This case is important if the queue is deleted after a message is received.
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to delete message", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
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
