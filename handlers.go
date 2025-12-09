package main

import (
	"context"
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
	r.Post("/queues/{queueName}/messages/batch-visibility", app.ChangeMessageVisibilityBatchHandler)

	// --- Placeholder handlers for unimplemented SQS features ---
	r.Post("/queues/{queueName}/permissions", app.AddPermissionHandler)
	r.Delete("/queues/{queueName}/permissions/{label}", app.RemovePermissionHandler)
	r.Get("/queues/{queueName}/tags", app.ListQueueTagsHandler)
	r.Post("/queues/{queueName}/tags", app.TagQueueHandler)
	r.Delete("/queues/{queueName}/tags", app.UntagQueueHandler)
	r.Get("/dead-letter-source-queues", app.ListDeadLetterSourceQueuesHandler)
	r.Post("/message-move-tasks/{taskHandle}/cancel", app.CancelMessageMoveTaskHandler)
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
	case "ChangeMessageVisibility":
		app.ChangeMessageVisibilityHandler(w, r)
	case "GetQueueAttributes":
		app.GetQueueAttributesHandler(w, r)
	case "SetQueueAttributes":
		app.SetQueueAttributesHandler(w, r)
	case "GetQueueUrl":
		app.GetQueueUrlHandler(w, r)
	case "SendMessageBatch":
		app.SendMessageBatchHandler(w, r)
	case "ChangeMessageVisibilityBatch":
		app.ChangeMessageVisibilityBatchHandler(w, r)
	case "AddPermission":
		app.AddPermissionHandler(w, r)
	case "RemovePermission":
		app.RemovePermissionHandler(w, r)
	case "ListQueueTags":
		app.ListQueueTagsHandler(w, r)
	case "TagQueue":
		app.TagQueueHandler(w, r)
	case "UntagQueue":
		app.UntagQueueHandler(w, r)
	case "ListDeadLetterSourceQueues":
		app.ListDeadLetterSourceQueuesHandler(w, r)
	case "StartMessageMoveTask":
		app.StartMessageMoveTaskHandler(w, r)
	case "CancelMessageMoveTask":
		app.CancelMessageMoveTaskHandler(w, r)
	case "ListMessageMoveTasks":
		app.ListMessageMoveTasksHandler(w, r)
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
var batchEntryIdRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
var arnRegex = regexp.MustCompile(`^arn:aws:sqs:[a-z0-9-]+:[0-9]+:[a-zA-Z0-9_-]{1,80}(\.fifo)?$`)
var labelRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
var awsAccountIdRegex = regexp.MustCompile(`^\d{12}$`)

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

	// After all validation passes, delegate to the storage layer.
	existingAttributes, err := app.Store.CreateQueue(r.Context(), req.QueueName, req.Attributes, req.Tags)
	if err != nil {
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

	// Handle idempotency based on SQS rules.
	if existingAttributes != nil {
		// Queue already existed. Check if attributes are identical.
		if !areAttributesEqual(req.Attributes, existingAttributes) {
			app.sendErrorResponse(w, "QueueNameExists", "A queue with this name already exists with different attributes.", http.StatusBadRequest)
			return
		}
		// Attributes are the same, so it's an idempotent success.
		w.WriteHeader(http.StatusOK)
	} else {
		// Queue was newly created.
		w.WriteHeader(http.StatusCreated)
	}

	json.NewEncoder(w).Encode(resp)
}

// areAttributesEqual compares two maps of queue attributes for equality.
// It handles the case where one map is nil.
func areAttributesEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || v != w {
			return false
		}
	}
	return true
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

	// Validate MaxResults according to SQS documentation (1-1000).
	if req.MaxResults < 0 || req.MaxResults > 1000 {
		// Note: A value of 0 is valid if MaxResults is omitted, but invalid if specified as 0.
		// The JSON decoding to an int makes this distinction impossible without a pointer.
		// For now, we treat 0 as "not specified". An explicit 0 would be a client error.
		// SQS docs are specific that the range is 1-1000.
		app.sendErrorResponse(w, "InvalidParameterValue", "MaxResults must be an integer between 1 and 1000.", http.StatusBadRequest)
		return
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
	}

	// SQS docs: NextToken is only returned if MaxResults was specified.
	if req.MaxResults > 0 {
		resp.NextToken = newNextToken
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetQueueAttributesHandler handles requests to retrieve attributes for a specified queue.
// It validates the request, fetches all attributes from the storage layer, and then
// filters them based on the list of attribute names provided by the client.
func (app *App) GetQueueAttributesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.GetQueueAttributesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// Perform batch-level validation before calling the store.
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	// Fetch all attributes for the queue from the storage layer first.
	allAttributes, err := app.Store.GetQueueAttributes(r.Context(), queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to get queue attributes", http.StatusInternalServerError)
		return
	}

	// SQS-specific attributes that are not stored directly but are derived or have default values.
	// This ensures consistency with the SQS API.
	allAttributes["QueueArn"] = fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:%s", queueName)
	if _, ok := allAttributes["VisibilityTimeout"]; !ok {
		allAttributes["VisibilityTimeout"] = "30"
	}
	if _, ok := allAttributes["ReceiveMessageWaitTimeSeconds"]; !ok {
		allAttributes["ReceiveMessageWaitTimeSeconds"] = "0"
	}
	if _, ok := allAttributes["MessageRetentionPeriod"]; !ok {
		allAttributes["MessageRetentionPeriod"] = "345600" // 4 days
	}
	if _, ok := allAttributes["MaximumMessageSize"]; !ok {
		allAttributes["MaximumMessageSize"] = "262144" // 256 KiB
	}
	if _, ok := allAttributes["DelaySeconds"]; !ok {
		allAttributes["DelaySeconds"] = "0"
	}
	if _, ok := allAttributes["KmsDataKeyReusePeriodSeconds"]; !ok {
		allAttributes["KmsDataKeyReusePeriodSeconds"] = "300" // 5 minutes
	}

	// Now, filter the attributes based on the client's request.
	responseAttributes := make(map[string]string)
	wantsAll := false
	if len(req.AttributeNames) > 0 && req.AttributeNames[0] == "All" {
		wantsAll = true
	}

	if wantsAll {
		responseAttributes = allAttributes
	} else {
		for _, name := range req.AttributeNames {
			// First, validate that the requested attribute name is a valid SQS attribute.
			if !validQueueAttributeNames[name] {
				app.sendErrorResponse(w, "InvalidAttributeName", "The specified attribute "+name+" does not exist.", http.StatusBadRequest)
				return
			}
			// If the attribute exists in our map (either from store or defaulted), add it to the response.
			if val, ok := allAttributes[name]; ok {
				responseAttributes[name] = val
			}
			// If a valid attribute is requested but not present (e.g. RedrivePolicy), it's omitted, not an error.
		}
	}

	resp := models.GetQueueAttributesResponse{
		Attributes: responseAttributes,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// SetQueueAttributesHandler handles requests to set attributes for a specified queue.
func (app *App) SetQueueAttributesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.SetQueueAttributesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}

	if len(req.Attributes) == 0 {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain Attributes.", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)

	// Validate attributes before sending to store.
	if err := validateSettableAttributes(req.Attributes); err != nil {
		if serr, ok := err.(*SqsError); ok {
			app.sendErrorResponse(w, serr.Type, serr.Message, http.StatusBadRequest)
		} else {
			app.sendErrorResponse(w, "InvalidAttributeValue", err.Error(), http.StatusBadRequest)
		}
		return
	}

	err := app.Store.SetQueueAttributes(r.Context(), queueName, req.Attributes)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to set queue attributes.", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// GetQueueUrlHandler handles requests to retrieve the URL for a queue.
func (app *App) GetQueueUrlHandler(w http.ResponseWriter, r *http.Request) {
	var req models.GetQueueURLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.QueueName == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueName.", http.StatusBadRequest)
		return
	}

	// Delegate to the store, but fall back to constructing the URL if the store doesn't provide it.
	// This is for compatibility with stores that might only verify existence.
	_, err := app.Store.GetQueueURL(r.Context(), req.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to get queue URL.", http.StatusInternalServerError)
		return
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	queueURL := fmt.Sprintf("%s://%s/queues/%s", scheme, r.Host, req.QueueName)

	resp := models.GetQueueURLResponse{
		QueueUrl: queueURL,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
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

// SqsError is a custom error type for returning specific AWS-style errors.
type SqsError struct {
	Type    string
	Message string
}

func (e *SqsError) Error() string {
	return e.Message
}

// A map of attributes that can be set after a queue is created.
var settableQueueAttributes = map[string]bool{
	"DelaySeconds":                  true,
	"MaximumMessageSize":            true,
	"MessageRetentionPeriod":        true,
	"Policy":                        true,
	"ReceiveMessageWaitTimeSeconds": true,
	"VisibilityTimeout":             true,
	"RedrivePolicy":                 true,
	"KmsMasterKeyId":                true,
	"KmsDataKeyReusePeriodSeconds":  true,
	"SqsManagedSseEnabled":          true,
	// Note: FIFO-related attributes like ContentBasedDeduplication are often settable,
	// but depend on the queue type. For simplicity, we assume they are settable if validated.
	"ContentBasedDeduplication": true,
}

// validateSettableAttributes performs validation for attributes that can be changed
// on an existing queue. It returns a more specific SqsError type.
func validateSettableAttributes(attributes map[string]string) error {
	if err := validateAttributes(attributes); err != nil {
		return err
	}
	for key := range attributes {
		if !settableQueueAttributes[key] {
			return &SqsError{
				Type:    "InvalidAttributeName",
				Message: fmt.Sprintf("Attribute %s cannot be changed.", key),
			}
		}
	}
	return nil
}

// --- Message Management Handlers and Helpers ---

// All valid queue attribute names for GetQueueAttributes
var validQueueAttributeNames = map[string]bool{
	"All":    true,
	"Policy": true,
	"VisibilityTimeout":                 true,
	"MaximumMessageSize":                true,
	"MessageRetentionPeriod":            true,
	"ApproximateNumberOfMessages":       true,
	"ApproximateNumberOfMessagesNotVisible": true,
	"CreatedTimestamp":                  true,
	"LastModifiedTimestamp":             true,
	"QueueArn":                          true,
	"ApproximateNumberOfMessagesDelayed":    true,
	"DelaySeconds":                      true,
	"ReceiveMessageWaitTimeSeconds":     true,
	"RedrivePolicy":                     true,
	"FifoQueue":                         true,
	"ContentBasedDeduplication":         true,
	"KmsMasterKeyId":                    true,
	"KmsDataKeyReusePeriodSeconds":      true,
	"DeduplicationScope":                true,
	"FifoThroughputLimit":               true,
	"RedriveAllowPolicy":                true,
	"SqsManagedSseEnabled":              true,
}

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
	if name == "All" {
		return true
	}
	// Handle wildcard case: prefix.*
	if strings.HasSuffix(name, ".*") {
		prefix := name[:len(name)-2]
		// The prefix itself must be a valid attribute name part.
		if len(prefix) == 0 {
			return true // ".*" is valid on its own to match all custom attributes
		}
		// Let's reuse the existing validation logic for the prefix.
		name = prefix
	}

	// Custom attributes cannot start with "aws." or "amazon.".
	if strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") {
		return false
	}
	// The name can contain alphanumeric characters and the underscore (_), hyphen (-), and period (.).
	// The `.` character is allowed, but not at the beginning or end, and not in sequence.
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return false
	}
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

// isSqsMessageBodyValid checks if the message body contains only valid characters
// as per the SQS documentation.
func isSqsMessageBodyValid(body string) bool {
	for _, r := range body {
		if !((r == 0x9) || (r == 0xA) || (r == 0xD) ||
			(r >= 0x20 && r <= 0xD7FF) ||
			(r >= 0xE000 && r <= 0xFFFD) ||
			(r >= 0x10000 && r <= 0x10FFFF)) {
			return false
		}
	}
	return true
}

// calculateMessageSize calculates the total size of the message including
// body and attributes, as per SQS limits.
func calculateMessageSize(body string, attributes map[string]models.MessageAttributeValue) int {
	totalSize := len(body)
	for name, attr := range attributes {
		totalSize += len(name)
		totalSize += len(attr.DataType)
		if attr.StringValue != nil {
			totalSize += len(*attr.StringValue)
		}
		if attr.BinaryValue != nil {
			totalSize += len(attr.BinaryValue)
		}
	}
	return totalSize
}

func (app *App) validateSendMessageRequest(req *models.SendMessageRequest, queueName string) error {
	if req.QueueUrl == "" {
		return errors.New("MissingParameter: The request must contain a QueueUrl.")
	}

	if len(req.MessageBody) < 1 {
		return errors.New("InvalidParameterValue: The message body must be between 1 and 262144 bytes long.")
	}

	// Validate total message size (body + attributes)
	totalSize := calculateMessageSize(req.MessageBody, req.MessageAttributes)
	if totalSize > 262144 {
		return errors.New("InvalidParameterValue: The message body must be between 1 and 262144 bytes long.")
	}

	if !isSqsMessageBodyValid(req.MessageBody) {
		return errors.New("InvalidMessageContents: The message contains characters outside the allowed set.")
	}

	if req.DelaySeconds != nil {
		if *req.DelaySeconds < 0 || *req.DelaySeconds > 900 {
			return errors.New("InvalidParameterValue: Value for parameter DelaySeconds is invalid. Reason: Must be an integer from 0 to 900.")
		}
		if strings.HasSuffix(queueName, ".fifo") {
			return errors.New("InvalidParameterValue: The request include parameter that is not valid for this queue type. Reason: DelaySeconds is not supported for FIFO queues.")
		}
	}

	isFifo := strings.HasSuffix(queueName, ".fifo")

	if req.MessageDeduplicationId != nil {
		if !isFifo {
			return errors.New("InvalidParameterValue: MessageDeduplicationId is supported only for FIFO queues.")
		}
		if len(*req.MessageDeduplicationId) > 128 {
			return errors.New("InvalidParameterValue: MessageDeduplicationId can be up to 128 characters long.")
		}
		if !isValidSqsChars(*req.MessageDeduplicationId) {
			return errors.New("InvalidParameterValue: MessageDeduplicationId can only contain alphanumeric characters and punctuation.")
		}
	}

	if isFifo {
		if req.MessageGroupId == nil {
			return errors.New("MissingParameter: The request must contain a MessageGroupId.")
		}
		if len(*req.MessageGroupId) > 128 {
			return errors.New("InvalidParameterValue: MessageGroupId can be up to 128 characters long.")
		}
		if !isValidSqsChars(*req.MessageGroupId) {
			return errors.New("InvalidParameterValue: MessageGroupId can only contain alphanumeric characters and punctuation.")
		}
	}

	if len(req.MessageAttributes) > 10 {
		return errors.New("InvalidParameterValue: Number of message attributes cannot exceed 10.")
	}
	for name, attr := range req.MessageAttributes {
		if !isValidMessageAttributeName(name) {
			return errors.New("InvalidParameterValue: Message attribute name '" + name + "' is invalid.")
		}
		if len(name) == 0 {
			return errors.New("InvalidParameterValue: Message attribute name cannot be empty.")
		}
		if attr.DataType == "" {
			return errors.New("InvalidParameterValue: DataType of message attribute '" + name + "' is required.")
		}
	}

	for name, attr := range req.MessageSystemAttributes {
		if name != "AWSTraceHeader" {
			return errors.New("InvalidParameterValue: '" + name + "' is not a valid message system attribute.")
		}
		if attr.DataType != "String" {
			return errors.New("InvalidParameterValue: DataType of AWSTraceHeader must be String.")
		}
	}

	return nil
}

// SendMessageHandler handles requests to send a single message to a queue.
func (app *App) SendMessageHandler(w http.ResponseWriter, r *http.Request) {
	var req models.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid JSON format", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)
	if err := app.validateSendMessageRequest(&req, queueName); err != nil {
		parts := strings.SplitN(err.Error(), ": ", 2)
		app.sendErrorResponse(w, parts[0], parts[1], http.StatusBadRequest)
		return
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
	// Note: We can't strictly validate the payload size just by len(bodyBytes) here because
	// the SQS limit applies to sum of (Body + Attributes) for all messages, not the JSON request size.
	// However, a sanity check is fine.

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
	queueName := path.Base(req.QueueUrl)

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

		// Calculate individual message size (Body + Attributes)
		msgSize := calculateMessageSize(entry.MessageBody, entry.MessageAttributes)

		if msgSize > 262144 {
			app.sendErrorResponse(w, "InvalidParameterValue", fmt.Sprintf("Message body for entry with Id '%s' is too long.", entry.Id), http.StatusBadRequest)
			return
		}
		totalPayloadSize += msgSize

		// Note: More complex per-entry validation (like checking DelaySeconds for a FIFO queue)
		// is delegated to the store layer, which can produce partial success/failure responses.
		// The handler only performs validations that would cause the entire batch to fail.
	}

	if totalPayloadSize > 262144 {
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

	// Ensure entry IDs are valid and distinct within the batch.
	ids := make(map[string]struct{})
	for _, entry := range req.Entries {
		if !batchEntryIdRegex.MatchString(entry.Id) {
			app.sendErrorResponse(w, "InvalidBatchEntryId", "The Id of a batch entry in a batch request doesn't abide by the specification.", http.StatusBadRequest)
			return
		}
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

// ChangeMessageVisibilityHandler handles requests to change the visibility timeout of a message.
func (app *App) ChangeMessageVisibilityHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ChangeMessageVisibilityRequest
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

	// Validate VisibilityTimeout range (0 to 12 hours)
	if req.VisibilityTimeout < 0 || req.VisibilityTimeout > 43200 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter VisibilityTimeout is invalid. Reason: Must be an integer from 0 to 43200.", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)

	err := app.Store.ChangeMessageVisibility(r.Context(), queueName, req.ReceiptHandle, req.VisibilityTimeout)
	if err != nil {
		if errors.Is(err, store.ErrInvalidReceiptHandle) {
			app.sendErrorResponse(w, "ReceiptHandleIsInvalid", "The specified receipt handle isn't valid.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		// A message not being in flight is a specific case of an invalid receipt handle.
		if errors.Is(err, store.ErrMessageNotInflight) {
			app.sendErrorResponse(w, "MessageNotInflight", "The specified message isn't in flight.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to change message visibility", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// ChangeMessageVisibilityBatchHandler handles requests to change the visibility timeout of multiple messages.
func (app *App) ChangeMessageVisibilityBatchHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ChangeMessageVisibilityBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	if len(req.Entries) > 10 {
		app.sendErrorResponse(w, "TooManyEntriesInBatchRequest", "The batch request contains more entries than permissible.", http.StatusBadRequest)
		return
	}

	ids := make(map[string]struct{})
	for _, entry := range req.Entries {
		if _, exists := ids[entry.Id]; exists {
			app.sendErrorResponse(w, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.", http.StatusBadRequest)
			return
		}
		ids[entry.Id] = struct{}{}
	}

	queueName := path.Base(req.QueueUrl)

	// Delegate the core logic, including partial failures, to the store layer.
	resp, err := app.Store.ChangeMessageVisibilityBatch(r.Context(), queueName, req.Entries)
	if err != nil {
		// This error is for entire-batch failures, e.g., the queue doesn't exist.
		// In this case, we fail all entries.
		failedResp := &models.ChangeMessageVisibilityBatchResponse{
			Successful: []models.ChangeMessageVisibilityBatchResultEntry{},
			Failed:     []models.BatchResultErrorEntry{},
		}
		var code string
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			code = "QueueDoesNotExist"
		} else {
			code = "InternalError"
		}

		for _, entry := range req.Entries {
			failedResp.Failed = append(failedResp.Failed, models.BatchResultErrorEntry{
				Id:          entry.Id,
				Code:        code,
				Message:     err.Error(),
				SenderFault: false,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK) // Batch operations return 200 even if all entries fail.
		json.NewEncoder(w).Encode(failedResp)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// AddPermissionHandler handles requests to add permissions to a queue.
func (app *App) AddPermissionHandler(w http.ResponseWriter, r *http.Request) {
	var req models.AddPermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
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
	if len(req.AWSAccountIds) == 0 {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain AWSAccountIds.", http.StatusBadRequest)
		return
	}
	if len(req.Actions) == 0 {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain Actions.", http.StatusBadRequest)
		return
	}

	// Validate Label (alphanumeric, hyphens, underscores, up to 80 chars)
	if len(req.Label) > 80 || !labelRegex.MatchString(req.Label) {
		app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter Label is invalid.", http.StatusBadRequest)
		return
	}

	// Validate Actions (SQS actions usually start with SQS: or just action name like SendMessage)
	// We won't strictly validate the action names against a list, but they should be non-empty strings.
	for _, action := range req.Actions {
		if action == "" {
			app.sendErrorResponse(w, "InvalidParameterValue", "Action names cannot be empty.", http.StatusBadRequest)
			return
		}
	}

	// Validate AWSAccountIds (12 digit numbers)
	for _, id := range req.AWSAccountIds {
		if !awsAccountIdRegex.MatchString(id) {
			app.sendErrorResponse(w, "InvalidParameterValue", "Invalid AWS Account ID: "+id, http.StatusBadRequest)
			return
		}
	}

	queueName := path.Base(req.QueueUrl)

	err := app.Store.AddPermission(r.Context(), queueName, req.Label, req.AWSAccountIds, req.Actions)
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
// RemovePermissionHandler handles requests to remove permissions from a queue.
func (app *App) RemovePermissionHandler(w http.ResponseWriter, r *http.Request) {
	var req models.RemovePermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
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

	queueName := path.Base(req.QueueUrl)

	err := app.Store.RemovePermission(r.Context(), queueName, req.Label)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrLabelDoesNotExist) {
			app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter Label is invalid. Reason: The specified label does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to remove permission", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
// ListQueueTagsHandler handles requests to list tags for a queue.
func (app *App) ListQueueTagsHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListQueueTagsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	tags, err := app.Store.ListQueueTags(r.Context(), queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to list queue tags", http.StatusInternalServerError)
		return
	}

	resp := models.ListQueueTagsResponse{
		Tags: tags,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// TagQueueHandler handles requests to add tags to a queue.
func (app *App) TagQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.TagQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	if len(req.Tags) == 0 {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain Tags.", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)

	err := app.Store.TagQueue(r.Context(), queueName, req.Tags)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to tag queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// UntagQueueHandler handles requests to remove tags from a queue.
func (app *App) UntagQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.UntagQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	if len(req.TagKeys) == 0 {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain TagKeys.", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)

	err := app.Store.UntagQueue(r.Context(), queueName, req.TagKeys)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to untag queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
// ListDeadLetterSourceQueuesHandler handles requests to list queues that use a specific queue as a DLQ.
func (app *App) ListDeadLetterSourceQueuesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListDeadLetterSourceQueuesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}

	// Validate MaxResults.
	if req.MaxResults < 0 || req.MaxResults > 1000 {
		app.sendErrorResponse(w, "InvalidParameterValue", "MaxResults must be an integer between 1 and 1000.", http.StatusBadRequest)
		return
	}

	// Determine if we need to set a default for MaxResults.
	// If MaxResults is 0 (unspecified in JSON), we treat it as 1000 (SQS default).
	maxResults := req.MaxResults
	if maxResults == 0 {
		maxResults = 1000
	}

	sourceQueues, newNextToken, err := app.Store.ListDeadLetterSourceQueues(r.Context(), req.QueueUrl, maxResults, req.NextToken)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to list dead letter source queues", http.StatusInternalServerError)
		return
	}

	// Construct full URLs for the response.
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	queueURLs := make([]string, len(sourceQueues))
	for i, name := range sourceQueues {
		queueURLs[i] = fmt.Sprintf("%s://%s/queues/%s", scheme, r.Host, name)
	}

	resp := models.ListDeadLetterSourceQueuesResponse{
		QueueUrls: queueURLs,
	}
	if newNextToken != "" {
		resp.NextToken = newNextToken
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
func (app *App) validateSourceArn(ctx context.Context, sourceArn string) error {
	if !arnRegex.MatchString(sourceArn) {
		return &SqsError{Type: "InvalidParameterValue", Message: "Invalid SourceArn format."}
	}
	parts := strings.Split(sourceArn, ":")
	queueName := parts[len(parts)-1]

	_, err := app.Store.GetQueueAttributes(ctx, queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			return &SqsError{Type: "QueueDoesNotExist", Message: "The specified queue does not exist."}
		}
		return err
	}
	return nil
}

// StartMessageMoveTaskHandler handles requests to start a task that moves messages between queues.
func (app *App) StartMessageMoveTaskHandler(w http.ResponseWriter, r *http.Request) {
	var req models.StartMessageMoveTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.SourceArn == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a SourceArn.", http.StatusBadRequest)
		return
	}

	if err := app.validateSourceArn(r.Context(), req.SourceArn); err != nil {
		if serr, ok := err.(*SqsError); ok {
			app.sendErrorResponse(w, serr.Type, serr.Message, http.StatusBadRequest)
		} else {
			app.sendErrorResponse(w, "InternalFailure", "Failed to validate SourceArn", http.StatusInternalServerError)
		}
		return
	}

	// Note: DestinationArn is optional. If not provided, messages are moved back to the source queue
	// (redriven from DLQ to source). However, in the standard API, DestinationArn is usually required
	// if moving to a different queue. If moving from DLQ to source, it might be inferred.
	// For simplicity, we pass whatever is provided.

	taskHandle, err := app.Store.StartMessageMoveTask(r.Context(), req.SourceArn, req.DestinationArn)
	if err != nil {
		// Map errors if necessary
		app.sendErrorResponse(w, "InternalFailure", "Failed to start message move task", http.StatusInternalServerError)
		return
	}

	resp := models.StartMessageMoveTaskResponse{
		TaskHandle: taskHandle,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// CancelMessageMoveTaskHandler handles requests to cancel a running message move task.
func (app *App) CancelMessageMoveTaskHandler(w http.ResponseWriter, r *http.Request) {
	var req models.CancelMessageMoveTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.TaskHandle == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a TaskHandle.", http.StatusBadRequest)
		return
	}

	err := app.Store.CancelMessageMoveTask(r.Context(), req.TaskHandle)
	if err != nil {
		if err.Error() == "ResourceNotFoundException" {
			app.sendErrorResponse(w, "ResourceNotFoundException", "The specified task does not exist.", http.StatusNotFound)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", "Failed to cancel message move task", http.StatusInternalServerError)
		return
	}

	// The response for CancelMessageMoveTask should ideally return the latest status.
	// Since our Store method currently returns void (error), we return a basic response or
	// we should fetch the task details.
	// SQS API docs say it returns ApproximateNumberOfMessagesMoved etc.
	// I'll return a mock response with "CANCELLED" status as the store method updated it.
	// A proper implementation would return the updated task object from the store.

	resp := models.CancelMessageMoveTaskResponse{
		TaskHandle: req.TaskHandle,
		Status:     "CANCELLED",
		// Other fields would be populated from the actual task state if we fetched it.
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ListMessageMoveTasksHandler handles requests to list message move tasks for a source queue.
func (app *App) ListMessageMoveTasksHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListMessageMoveTasksRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "The request is not a valid JSON.", http.StatusBadRequest)
		return
	}

	if req.SourceArn == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a SourceArn.", http.StatusBadRequest)
		return
	}

	if err := app.validateSourceArn(r.Context(), req.SourceArn); err != nil {
		if serr, ok := err.(*SqsError); ok {
			app.sendErrorResponse(w, serr.Type, serr.Message, http.StatusBadRequest)
		} else {
			app.sendErrorResponse(w, "InternalFailure", "Failed to validate SourceArn", http.StatusInternalServerError)
		}
		return
	}

	results, err := app.Store.ListMessageMoveTasks(r.Context(), req.SourceArn)
	if err != nil {
		app.sendErrorResponse(w, "InternalFailure", "Failed to list message move tasks", http.StatusInternalServerError)
		return
	}

	resp := models.ListMessageMoveTasksResponse{
		Results: results,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
