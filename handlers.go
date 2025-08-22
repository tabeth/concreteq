package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/store"

	"github.com/go-chi/chi/v5"
)

// App encapsulates the application's dependencies, primarily the storage interface.
// This struct is used as the receiver for our HTTP handlers, giving them access
// to the database connection pool (via the Store interface) and other dependencies.
// This is a common pattern for dependency injection in Go web services.
type App struct {
	Store store.Store
}

// sendErrorResponse is a convenience helper function to format and send error responses
// that are compatible with the AWS SQS API. It sets the appropriate headers and
// marshals the error into the standard JSON format expected by AWS clients.
func (app *App) sendErrorResponse(w http.ResponseWriter, errorType string, message string, statusCode int) {
	errResp := models.ErrorResponse{
		Type:    errorType,
		Message: message,
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(errResp)
}

// RegisterSQSHandlers registers all the SQS API endpoint handlers with the Chi router.
// It sets up both the main RPC-style endpoint and several REST-ful endpoints.
func (app *App) RegisterSQSHandlers(r *chi.Mux) {
	// Most AWS services, including SQS, use a single RPC-style endpoint (`/`)
	// where the action is specified in the `X-Amz-Target` header.
	// This is the primary handler for that style of request.
	r.Post("/", app.RootSQSHandler)

	// The commented-out routes below suggest a previous or alternative REST-ful routing scheme.
	// While SQS has some REST-like qualities, the `X-Amz-Target` header is the canonical way.
	// r.Post("/queues", app.CreateQueueHandler)
	// r.Delete("/queues/{queueName}", app.DeleteQueueHandler)
	// r.Get("/queues", app.ListQueuesHandler)
	// r.Post("/queues/{queueName}/purge", app.PurgeQueueHandler)

	// The following routes are for actions that are not dispatched through the RootSQSHandler.
	// These might be for compatibility or specific use cases.
	r.Post("/queues/{queueName}/messages/batch", app.SendMessageBatchHandler)
	r.Delete("/queues/{queueName}/messages/{receiptHandle}", app.DeleteMessageHandler)
	r.Post("/queues/{queueName}/messages/batch-delete", app.DeleteMessageBatchHandler)
	r.Patch("/queues/{queueName}/messages/{receiptHandle}", app.ChangeMessageVisibilityHandler)
	r.Post("/queues/{queueName}/messages/batch-visibility", app.ChangeMessageVisibilityBatchHandler)

	// --- Placeholder handlers for unimplemented SQS features ---
	r.Post("/queues/{queueName}/permissions", app.AddPermissionHandler)
	r.Delete("/queues/{queueName}/permissions/{label}", app.RemovePermissionHandler)
	r.Get("/queues/{queueName}/tags", app.ListQueueTagsHandler)
	r.Post("/queues/{queueName}/tags", app.TagQueueHandler)
	r.Delete("/queues/{queueName}/tags", app.UntagQueueHandler)
	r.Get("/dead-letter-source-queues", app.ListDeadLetterSourceQueuesHandler)
	r.Post("/message-move-tasks", app.StartMessageMoveTaskHandler)
	r.Post("/message-move-tasks/{taskHandle}/cancel", app.CancelMessageMoveTaskHandler)
	r.Get("/message-move-tasks", app.ListMessageMoveTasksHandler)
}

// RootSQSHandler acts as a dispatcher for the primary SQS RPC-style endpoint.
// It inspects the `X-Amz-Target` header to determine which SQS action is being requested
// and calls the appropriate handler function. This is a common pattern for multiplexing
// multiple actions over a single URL endpoint.
func (app *App) RootSQSHandler(w http.ResponseWriter, r *http.Request) {
	// The target header format is typically "AmazonSQS.<ActionName>".
	target := r.Header.Get("X-Amz-Target")

	parts := strings.Split(target, ".")
	if len(parts) != 2 || parts[0] != "AmazonSQS" {
		app.sendErrorResponse(w, "InvalidAction", "Invalid X-Amz-Target header", http.StatusBadRequest)
		return
	}
	action := parts[1]

	// A switch statement is used to route the request to the correct handler
	// based on the action name extracted from the header.
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
	case "DeleteMessageBatch":
		app.DeleteMessageBatchHandler(w, r)
	case "AddPermission":
		app.AddPermissionHandler(w, r)
	default:
		app.sendErrorResponse(w, "UnsupportedOperation", "Unsupported operation: "+action, http.StatusBadRequest)
	}
}

// --- Validation Helpers ---

// SQS queue name validation regex, based on the official AWS SQS documentation.
// A queue name can have up to 80 characters.
// Valid values: alphanumeric characters, hyphens (-), and underscores (_).
// For FIFO queues, the name must end with the .fifo suffix.
var queueNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}(\.fifo)?$`)
var labelRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
var awsAccountIDRegex = regexp.MustCompile(`^\d{12}$`)

// validateIntAttribute is a helper for checking if a string can be parsed as an integer
// and falls within a specified min/max range. This is used for validating queue attributes.
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

// validateAttributes performs validation for all SQS-specific queue attributes.
// It centralizes the validation logic for attributes like 'VisibilityTimeout',
// 'FifoQueue', and 'RedrivePolicy'.
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
			// RedrivePolicy is a JSON-encoded string, so it requires unmarshaling to validate.
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

// --- Queue Management Handlers ---

// CreateQueueHandler handles requests to create a new queue.
// It performs extensive validation on the queue name and attributes before
// calling the storage layer to persist the queue.
func (app *App) CreateQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.CreateQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate queue name format according to SQS rules.
	if !queueNameRegex.MatchString(req.QueueName) {
		app.sendErrorResponse(w, "InvalidParameterValue", "Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length.", http.StatusBadRequest)
		return
	}

	// Validate that the FifoQueue attribute is consistent with the queue name's .fifo suffix.
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

	// Validate all other provided attributes.
	if err := validateAttributes(req.Attributes); err != nil {
		app.sendErrorResponse(w, "InvalidAttributeName", err.Error(), http.StatusBadRequest)
		return
	}

	// Ensure that FIFO-specific attributes are only used with FIFO queues.
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

	// Validate inter-dependencies between FIFO attributes.
	if scope, exists := req.Attributes["DeduplicationScope"]; exists {
		if limit, limitExists := req.Attributes["FifoThroughputLimit"]; limitExists {
			if limit == "perMessageGroupId" && scope != "messageGroup" {
				app.sendErrorResponse(w, "InvalidParameterValue", "FifoThroughputLimit can be set to perMessageGroupId only when DeduplicationScope is messageGroup", http.StatusBadRequest)
				return
			}
		}
	}

	// After all validation passes, delegate to the storage layer to create the queue.
	err := app.Store.CreateQueue(r.Context(), req.QueueName, req.Attributes, req.Tags)
	if err != nil {
		// Handle specific errors returned from the store, like the queue already existing.
		if errors.Is(err, store.ErrQueueAlreadyExists) {
			app.sendErrorResponse(w, "QueueAlreadyExists", "Queue already exists", http.StatusConflict)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to create queue", http.StatusInternalServerError)
		return
	}

	// Construct the queue URL dynamically based on the incoming request's host.
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

// DeleteQueueHandler handles requests to delete an existing queue.
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
	// The queue name is extracted from the last path segment of the URL.
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

// ListQueuesHandler handles requests to list all queues.
func (app *App) ListQueuesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListQueuesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// SQS allows ListQueues to be called via GET or POST. An empty body for a POST
		// is acceptable, so we ignore decoding errors and proceed with default values.
	}

	// Delegate to the storage layer to fetch the list of queues.
	queueNames, newNextToken, err := app.Store.ListQueues(r.Context(), req.MaxResults, req.NextToken, req.QueueNamePrefix)
	if err != nil {
		app.sendErrorResponse(w, "InternalFailure", "Failed to list queues", http.StatusInternalServerError)
		return
	}

	// SQS limits the result to 1000 queues if MaxResults is not specified.
	if req.MaxResults == 0 && len(queueNames) > 1000 {
		queueNames = queueNames[:1000]
	}

	// The storage layer returns queue names; the handler is responsible for
	// constructing the full queue URLs for the response.
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	queueURLs := make([]string, len(queueNames))
	for i, name := range queueNames {
		queueURLs[i] = fmt.Sprintf("%s://%s/queues/%s", scheme, r.Host, name)
	}

	resp := models.ListQueuesResponse{
		QueueUrls: queueURLs,
		NextToken: newNextToken,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetQueueAttributesHandler is a placeholder for a not-yet-implemented feature.
func (app *App) GetQueueAttributesHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

// SetQueueAttributesHandler is a placeholder for a not-yet-implemented feature.
func (app *App) SetQueueAttributesHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

// GetQueueUrlHandler is a placeholder for a not-yet-implemented feature.
func (app *App) GetQueueUrlHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}

// PurgeQueueHandler handles requests to delete all messages from a queue.
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
		// This error enforces the SQS rule that you can't purge a queue more
		// than once every 60 seconds.
		if errors.Is(err, store.ErrPurgeQueueInProgress) {
			app.sendErrorResponse(w, "PurgeQueueInProgress", "Indicates that the specified queue previously received a PurgeQueue request within the last 60 seconds.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to purge queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// --- Message Management Handlers and Helpers ---

// validMessageSystemAttributeNames is a set of the allowed system attribute names for messages.
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

// isValidMessageAttributeName validates the format of a custom message attribute name against SQS rules.
func isValidMessageAttributeName(name string) bool {
	if len(name) > 256 {
		return false
	}
	// Custom attributes cannot start with "aws." or "amazon.".
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
// This is used for parameters like MessageDeduplicationId and MessageGroupId.
// Valid characters are alphanumeric and: !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
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

// SendMessageHandler handles requests to send a single message to a queue.
func (app *App) SendMessageHandler(w http.ResponseWriter, r *http.Request) {
	// This handler demonstrates a robust way to parse incoming JSON.
	// 1. Read the entire body. This prevents issues with streaming decoders
	//    leaving the connection in an indeterminate state on error.
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Failed to read request body", http.StatusBadRequest)
		return
	}

	// 2. Unmarshal into a generic map. This is more resilient to unexpected fields
	//    or type mismatches (e.g., a number sent as a string) than unmarshaling
	//    directly into a struct.
	var data map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid JSON format", http.StatusBadRequest)
		return
	}

	// 3. Manually construct the target request struct from the map. This allows for
	//    graceful type conversions (e.g., float64 from JSON to int32 in the struct)
	//    and provides a safe intermediate step for validation.
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
	// Note: A full implementation would also handle MessageAttributes and MessageSystemAttributes here.

	// --- Comprehensive SQS Validation ---
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	if len(req.MessageBody) < 1 || len(req.MessageBody) > 256*1024 { // 1 char to 256 KiB
		app.sendErrorResponse(w, "InvalidParameterValue", "The message body must be between 1 and 262144 bytes long.", http.StatusBadRequest)
		return
	}

	// DelaySeconds is not allowed for FIFO queues.
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

	// MessageDeduplicationId is only allowed for FIFO queues.
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

	// MessageGroupId is required for FIFO queues.
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

	// Validate message attributes.
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

	// Validate system attributes.
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

	// After validation, delegate to the storage layer.
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

// SendMessageBatchHandler handles requests to send up to 10 messages in a single call.
func (app *App) SendMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Cannot read request body", http.StatusBadRequest)
		return
	}

	// Validate the total size of the batch request payload.
	if len(bodyBytes) > 256*1024 {
		app.sendErrorResponse(w, "BatchRequestTooLong", "The length of all the messages put together is more than the limit.", http.StatusBadRequest)
		return
	}

	var req models.SendMessageBatchRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// --- Batch-level Validation ---
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	if len(req.Entries) == 0 {
		app.sendErrorResponse(w, "EmptyBatchRequest", "The batch request doesn't contain any entries.", http.StatusBadRequest)
		return
	}
	if len(req.Entries) > 10 {
		app.sendErrorResponse(w, "TooManyEntriesInBatchRequest", "The batch request contains more entries than permissible.", http.StatusBadRequest)
		return
	}

	// --- Per-Entry Validation ---
	ids := make(map[string]struct{})
	totalPayloadSize := 0
	queueName := chi.URLParam(r, "queueName")

	for _, entry := range req.Entries {
		// Ensure all entry IDs within a single batch request are unique.
		if _, exists := ids[entry.Id]; exists {
			app.sendErrorResponse(w, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.", http.StatusBadRequest)
			return
		}
		ids[entry.Id] = struct{}{}

		if len(entry.Id) > 80 || !isValidSqsChars(entry.Id) {
			app.sendErrorResponse(w, "InvalidBatchEntryId", "The Id of a batch entry in a batch request doesn't abide by the specification.", http.StatusBadRequest)
			return
		}

		if len(entry.MessageBody) > 256*1024 {
			app.sendErrorResponse(w, "InvalidParameterValue", fmt.Sprintf("Message body for entry with Id '%s' is too long.", entry.Id), http.StatusBadRequest)
			return
		}
		totalPayloadSize += len(entry.MessageBody)

		// Note: More complex per-entry validation (like checking DelaySeconds for a FIFO queue)
		// is delegated to the store layer, which can produce partial success/failure responses.
		// The handler only performs validations that would cause the entire batch to fail.
	}

	if totalPayloadSize > 256*1024 {
		app.sendErrorResponse(w, "BatchRequestTooLong", "The length of all the messages put together is more than the limit.", http.StatusBadRequest)
		return
	}

	// Delegate to the storage layer, which will handle individual message processing.
	resp, err := app.Store.SendMessageBatch(r.Context(), queueName, &req)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to send message batch", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ReceiveMessageHandler handles requests to retrieve messages from a queue.
func (app *App) ReceiveMessageHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ReceiveMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// Perform validation on all request parameters.
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	// MaxNumberOfMessages must be between 1 and 10.
	if req.MaxNumberOfMessages < 1 || req.MaxNumberOfMessages > 10 {
		// SQS defaults to 1 if the parameter is not specified. A value of 0 is invalid if provided.
		if req.MaxNumberOfMessages != 0 {
			app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter MaxNumberOfMessages is invalid. Reason: Must be an integer from 1 to 10.", http.StatusBadRequest)
			return
		}
	}

	// WaitTimeSeconds enables long polling and must be between 0 and 20.
	if req.WaitTimeSeconds < 0 || req.WaitTimeSeconds > 20 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter WaitTimeSeconds is invalid. Reason: Must be an integer from 0 to 20.", http.StatusBadRequest)
		return
	}

	// VisibilityTimeout (if specified) overrides the queue's default.
	if req.VisibilityTimeout < 0 || req.VisibilityTimeout > 43200 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter VisibilityTimeout is invalid. Reason: Must be an integer from 0 to 43200.", http.StatusBadRequest)
		return
	}

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

	// Validate that only supported attribute names are requested.
	allSystemAttributes := append(req.MessageSystemAttributeNames, req.AttributeNames...)
	for _, name := range allSystemAttributes {
		if !validMessageSystemAttributeNames[name] {
			app.sendErrorResponse(w, "InvalidAttributeName", "The attribute '"+name+"' is not supported.", http.StatusBadRequest)
			return
		}
	}

	for _, name := range req.MessageAttributeNames {
		if !isValidMessageAttributeName(name) {
			app.sendErrorResponse(w, "InvalidAttributeName", "The attribute name '"+name+"' is invalid.", http.StatusBadRequest)
			return
		}
	}

	// Delegate to the storage layer, which will handle the long polling logic.
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

// DeleteMessageHandler handles requests to delete a single message.
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
		// This can happen if the queue is deleted after a message is received but before it's deleted.
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to delete message", http.StatusInternalServerError)
		return
	}

	// A successful deletion returns a 200 OK with no body.
	w.WriteHeader(http.StatusOK)
}

// DeleteMessageBatchHandler handles requests to delete up to 10 messages in a single call.
func (app *App) DeleteMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	var req models.DeleteMessageBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// Batch-level validation.
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	if len(req.Entries) == 0 {
		app.sendErrorResponse(w, "EmptyBatchRequest", "The batch request doesn't contain any entries.", http.StatusBadRequest)
		return
	}
	if len(req.Entries) > 10 {
		app.sendErrorResponse(w, "TooManyEntriesInBatchRequest", "The batch request contains more entries than permissible.", http.StatusBadRequest)
		return
	}

	// Ensure entry IDs are distinct within the batch.
	ids := make(map[string]struct{})
	for _, entry := range req.Entries {
		if _, exists := ids[entry.Id]; exists {
			app.sendErrorResponse(w, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.", http.StatusBadRequest)
			return
		}
		ids[entry.Id] = struct{}{}
	}

	queueName := path.Base(req.QueueUrl)
	// Delegate to the store, which will return a partial success/failure response.
	resp, err := app.Store.DeleteMessageBatch(r.Context(), queueName, req.Entries)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to delete message batch", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --- Unimplemented Handlers ---

func (app *App) ChangeMessageVisibilityHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) ChangeMessageVisibilityBatchHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) AddPermissionHandler(w http.ResponseWriter, r *http.Request) {
	var req models.AddPermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	if req.Label == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a Label.", http.StatusBadRequest)
		return
	}
	if !labelRegex.MatchString(req.Label) {
		app.sendErrorResponse(w, "InvalidParameterValue", "Invalid label: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length.", http.StatusBadRequest)
		return
	}
	if len(req.AWSAccountIds) == 0 {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain at least one AWSAccountId.", http.StatusBadRequest)
		return
	}
	if len(req.Actions) == 0 {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain at least one Action.", http.StatusBadRequest)
		return
	}

	u, err := url.Parse(req.QueueUrl)
	if err != nil {
		app.sendErrorResponse(w, "InvalidAddress", "Invalid QueueUrl", http.StatusBadRequest)
		return
	}

	// This is a simplification. In a real multi-tenant SQS, the account ID and region
	// would be parsed from the request's host or path. For this self-contained service,
	// we'll use placeholder values.
	queueOwnerAWSAccountID := "123456789012" // Placeholder
	region := "us-east-1"                   // Placeholder
	queueName := path.Base(u.Path)

	var principalARNs []string
	for _, id := range req.AWSAccountIds {
		if !awsAccountIDRegex.MatchString(id) {
			app.sendErrorResponse(w, "InvalidParameterValue", "Invalid AWSAccountId: "+id, http.StatusBadRequest)
			return
		}
		principalARNs = append(principalARNs, "arn:aws:iam::"+id+":root")
	}

	statement := models.Statement{
		Sid:    req.Label,
		Effect: "Allow",
		Principal: models.Principal{
			AWS: principalARNs,
		},
		Action:   req.Actions,
		Resource: fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, queueOwnerAWSAccountID, queueName),
	}

	err = app.Store.AddPermission(r.Context(), queueName, statement)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to add permission", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
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
