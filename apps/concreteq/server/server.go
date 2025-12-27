package server

import (
	"context"
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

func (app *App) sendErrorResponse(w http.ResponseWriter, errorType string, message string, statusCode int) {
	errResp := models.ErrorResponse{
		Type:    errorType,
		Message: message,
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(errResp)
}

func (app *App) RegisterSQSHandlers(r *chi.Mux) {
	r.Post("/", app.RootSQSHandler)
	r.Post("/queues/{queueName}", app.RootSQSHandler)
	r.Post("/queues/{queueName}/messages/batch", app.SendMessageBatchHandler)
	r.Delete("/queues/{queueName}/messages/{receiptHandle}", app.DeleteMessageHandler)
	r.Post("/queues/{queueName}/messages/batch-delete", app.DeleteMessageBatchHandler)
	r.Post("/queues/{queueName}/messages/batch-visibility", app.ChangeMessageVisibilityBatchHandler)

	r.Post("/queues/{queueName}/permissions", app.AddPermissionHandler)
	r.Delete("/queues/{queueName}/permissions/{label}", app.RemovePermissionHandler)
	r.Get("/queues/{queueName}/tags", app.ListQueueTagsHandler)
	r.Post("/queues/{queueName}/tags", app.TagQueueHandler)
	r.Delete("/queues/{queueName}/tags", app.UntagQueueHandler)
	r.Get("/dead-letter-source-queues", app.ListDeadLetterSourceQueuesHandler)
	r.Post("/message-move-tasks/{taskHandle}/cancel", app.CancelMessageMoveTaskHandler)
}

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

// --- Validation Helpers & Constants ---
var queueNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}(\.fifo)?$`)
var arnRegex = regexp.MustCompile(`^arn:aws:sqs:[a-z0-9-]+:[0-9]+:[a-zA-Z0-9_-]{1,80}(\.fifo)?$`)
var validIdRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
var validAccountIdRegex = regexp.MustCompile(`^[0-9]{12}$`)

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

func ValidateAttributes(attributes map[string]string) error {
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
		case "RedriveAllowPolicy":
			var policy struct {
				RedrivePermission string   `json:"redrivePermission"`
				SourceQueueArns   []string `json:"sourceQueueArns"`
			}
			if jsonErr := json.Unmarshal([]byte(val), &policy); jsonErr != nil {
				err = errors.New("must be a valid JSON object")
			} else {
				if policy.RedrivePermission != "allowAll" && policy.RedrivePermission != "denyAll" && policy.RedrivePermission != "byQueue" {
					err = errors.New("redrivePermission must be one of: allowAll, denyAll, byQueue")
				} else if policy.RedrivePermission == "byQueue" {
					if len(policy.SourceQueueArns) == 0 {
						err = errors.New("sourceQueueArns is required when redrivePermission is byQueue")
					}
					for _, arn := range policy.SourceQueueArns {
						if !arnRegex.MatchString(arn) {
							err = fmt.Errorf("invalid sourceQueueArn: %s", arn)
							break
						}
					}
				}
			}
		case "Policy":
			if !json.Valid([]byte(val)) {
				err = errors.New("must be a valid JSON object")
			}
		case "KmsMasterKeyId":
			if len(strings.TrimSpace(val)) == 0 {
				err = errors.New("must not be empty")
			}
		case "KmsDataKeyReusePeriodSeconds":
			err = validateIntAttribute(val, 60, 86400)
		case "SqsManagedSseEnabled":
			if val != "true" && val != "false" {
				err = fmt.Errorf("must be 'true' or 'false'")
			}
		}
		if err != nil {
			return fmt.Errorf("invalid value for %s: %w", key, err)
		}
	}
	return nil
}

// IsSqsMessageBodyValid checks if the message body contains only allowed SQS characters.
// Allowed: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF
func IsSqsMessageBodyValid(body string) bool {
	for _, r := range body {
		if (r == 0x9 || r == 0xA || r == 0xD) ||
			(r >= 0x20 && r <= 0xD7FF) ||
			(r >= 0xE000 && r <= 0xFFFD) ||
			(r >= 0x10000 && r <= 0x10FFFF) {
			continue
		}
		return false
	}
	return true
}

// --- Implementation of Handlers (Including Message Ops) ---

func (app *App) SendMessageHandler(w http.ResponseWriter, r *http.Request) {
	// Limit body size before decoding
	r.Body = http.MaxBytesReader(w, r.Body, 262144) // 256KB cap for read

	var req models.SendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidParameterValue", "The message body must be between 1 and 262144 bytes long.", http.StatusBadRequest)
		return
	}
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)
	isFifo := strings.HasSuffix(queueName, ".fifo")
	if isFifo {
		if req.DelaySeconds != nil && *req.DelaySeconds > 0 {
			app.sendErrorResponse(w, "InvalidParameterValue", "The request include parameter that is not valid for this queue type. Reason: DelaySeconds is not supported for FIFO queues.", http.StatusBadRequest)
			return
		}
		if req.MessageGroupId == nil || *req.MessageGroupId == "" {
			app.sendErrorResponse(w, "MissingParameter", "The request must contain a MessageGroupId.", http.StatusBadRequest)
			return
		}
	} else {
		if req.MessageDeduplicationId != nil && *req.MessageDeduplicationId != "" {
			app.sendErrorResponse(w, "InvalidParameterValue", "MessageDeduplicationId is supported only for FIFO queues.", http.StatusBadRequest)
			return
		}
		if req.MessageGroupId != nil && *req.MessageGroupId != "" {
			app.sendErrorResponse(w, "InvalidParameterValue", "MessageGroupId is supported only for FIFO queues.", http.StatusBadRequest)
			return
		}
	}

	// Validate Body Size (100KB strict limit for FDB single value compatibility)
	// We map this to InvalidParameterValue SQS error
	if len(req.MessageBody) == 0 {
		app.sendErrorResponse(w, "InvalidParameterValue", "The message body must be between 1 and 262144 bytes long.", http.StatusBadRequest)
		return
	}
	if len(req.MessageBody) > 100*1024 { // 100KB Limit
		app.sendErrorResponse(w, "InvalidParameterValue", "The message body must be between 1 and 262144 bytes long (ConcreteQ Limit: 100KB).", http.StatusBadRequest)
		return
	}

	// Validate Body Characters
	if !IsSqsMessageBodyValid(req.MessageBody) {
		app.sendErrorResponse(w, "InvalidMessageContents", "The message contains characters outside the allowed set.", http.StatusBadRequest)
		return
	}

	if len(req.MessageAttributes) > 10 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Number of message attributes [11] exceeds the allowed maximum [10].", http.StatusBadRequest)
		return
	}

	// Store expects store.Message, models.SendMessageResponse comes from... store?
	// Let's check store interface. It returns *models.SendMessageResponse.

	resp, err := app.Store.SendMessage(r.Context(), queueName, &req)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if strings.Contains(err.Error(), "unsupported") { // DelaySeconds/FIFO mismatch etc
			app.sendErrorResponse(w, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
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
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)

	// Validate Parameters
	if req.MaxNumberOfMessages != nil {
		if *req.MaxNumberOfMessages < 1 || *req.MaxNumberOfMessages > 10 {
			app.sendErrorResponse(w, "InvalidParameterValue", "MaxNumberOfMessages must be between 1 and 10.", http.StatusBadRequest)
			return
		}
	}

	if req.WaitTimeSeconds != nil {
		if *req.WaitTimeSeconds < 0 || *req.WaitTimeSeconds > 20 {
			app.sendErrorResponse(w, "InvalidParameterValue", "WaitTimeSeconds must be between 0 and 20.", http.StatusBadRequest)
			return
		}
	}

	if req.VisibilityTimeout != nil {
		if *req.VisibilityTimeout < 0 || *req.VisibilityTimeout > 43200 {
			app.sendErrorResponse(w, "InvalidParameterValue", "VisibilityTimeout must be between 0 and 43200.", http.StatusBadRequest)
			return
		}
	}

	if len(req.ReceiveRequestAttemptId) > 128 {
		app.sendErrorResponse(w, "InvalidParameterValue", "ReceiveRequestAttemptId length cannot exceed 128 characters", http.StatusBadRequest)
		return
	}

	resp, err := app.Store.ReceiveMessage(r.Context(), queueName, &req)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if strings.Contains(err.Error(), "unsupported attribute") {
			app.sendErrorResponse(w, "InvalidAttributeName", err.Error(), http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
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
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrInvalidReceiptHandle) {
			app.sendErrorResponse(w, "ReceiptHandleIsInvalid", "The specified receipt handle isn't valid.", http.StatusBadRequest)
			return
		}
		// SQS is idempotent for already deleted messages
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (app *App) SendMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	var req models.SendMessageBatchRequest
	// SQS total payload limit 256KB
	r.Body = http.MaxBytesReader(w, r.Body, 262144)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		if err.Error() == "http: request body too large" {
			app.sendErrorResponse(w, "BatchRequestTooLong", "The length of all the messages put together is more than the limit.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

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

	queueName := path.Base(req.QueueUrl)
	isFifo := strings.HasSuffix(queueName, ".fifo")

	// Check for Duplicate IDs and per-entry validation
	ids := make(map[string]bool)
	totalSize := 0
	var immediateFailed []models.BatchResultErrorEntry

	validIdRegex := regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
	for _, entry := range req.Entries {
		if ids[entry.Id] {
			app.sendErrorResponse(w, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.", http.StatusBadRequest)
			return
		}
		if !validIdRegex.MatchString(entry.Id) {
			app.sendErrorResponse(w, "InvalidBatchEntryId", "The Id of a batch entry in a batch request doesn't abide by the specification.", http.StatusBadRequest)
			return
		}
		ids[entry.Id] = true

		entryFailed := false
		if isFifo {
			if entry.DelaySeconds != nil && *entry.DelaySeconds > 0 {
				immediateFailed = append(immediateFailed, models.BatchResultErrorEntry{
					Id:          entry.Id,
					Code:        "InvalidParameterValue",
					Message:     "DelaySeconds is not supported for FIFO queues.",
					SenderFault: true,
				})
				entryFailed = true
			} else if entry.MessageGroupId == nil || *entry.MessageGroupId == "" {
				immediateFailed = append(immediateFailed, models.BatchResultErrorEntry{
					Id:          entry.Id,
					Code:        "MissingParameter",
					Message:     "The request must contain a MessageGroupId.",
					SenderFault: true,
				})
				entryFailed = true
			}
		} else {
			if entry.MessageDeduplicationId != nil && *entry.MessageDeduplicationId != "" {
				immediateFailed = append(immediateFailed, models.BatchResultErrorEntry{
					Id:          entry.Id,
					Code:        "InvalidParameterValue",
					Message:     "MessageDeduplicationId is supported only for FIFO queues.",
					SenderFault: true,
				})
				entryFailed = true
			} else if entry.MessageGroupId != nil && *entry.MessageGroupId != "" {
				immediateFailed = append(immediateFailed, models.BatchResultErrorEntry{
					Id:          entry.Id,
					Code:        "InvalidParameterValue",
					Message:     "MessageGroupId is supported only for FIFO queues.",
					SenderFault: true,
				})
				entryFailed = true
			}
		}

		if !entryFailed && len(entry.MessageAttributes) > 10 {
			immediateFailed = append(immediateFailed, models.BatchResultErrorEntry{
				Id:          entry.Id,
				Code:        "InvalidParameterValue",
				Message:     "The number of message attributes cannot exceed 10.",
				SenderFault: true,
			})
			entryFailed = true
		}

		if !entryFailed {
			totalSize += len(entry.MessageBody)
			if len(entry.MessageBody) > 100*1024 {
				immediateFailed = append(immediateFailed, models.BatchResultErrorEntry{
					Id:          entry.Id,
					Code:        "InvalidParameterValue",
					Message:     "The message body must be between 1 and 102400 bytes.",
					SenderFault: true,
				})
				entryFailed = true
			} else {
				for name, attr := range entry.MessageAttributes {
					totalSize += len(name) + len(attr.BinaryValue)
					if attr.StringValue != nil {
						totalSize += len(*attr.StringValue)
					}
				}
			}
		}
	}

	if totalSize > 256*1024 {
		app.sendErrorResponse(w, "BatchRequestTooLong", "The length of all the messages put together is more than the limit.", http.StatusBadRequest)
		return
	}

	// Filter out immediately failed entries before passing to store
	originalEntries := req.Entries
	validEntries := []models.SendMessageBatchRequestEntry{}
	for _, entry := range originalEntries {
		failed := false
		for _, f := range immediateFailed {
			if f.Id == entry.Id {
				failed = true
				break
			}
		}
		if !failed {
			validEntries = append(validEntries, entry)
		}
	}

	var resp *models.SendMessageBatchResponse
	var err error

	if len(validEntries) > 0 {
		req.Entries = validEntries
		resp, err = app.Store.SendMessageBatch(r.Context(), queueName, &req)
	} else {
		resp = &models.SendMessageBatchResponse{
			Successful: []models.SendMessageBatchResultEntry{},
			Failed:     []models.BatchResultErrorEntry{},
		}
	}

	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	if resp.Successful == nil {
		resp.Successful = []models.SendMessageBatchResultEntry{}
	}
	if resp.Failed == nil {
		resp.Failed = []models.BatchResultErrorEntry{}
	}

	// Merge immediate failures
	resp.Failed = append(resp.Failed, immediateFailed...)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (app *App) DeleteMessageBatchHandler(w http.ResponseWriter, r *http.Request) {
	var req models.DeleteMessageBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

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

	ids := make(map[string]bool)
	validIdRegex := regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
	for _, entry := range req.Entries {
		if ids[entry.Id] {
			app.sendErrorResponse(w, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.", http.StatusBadRequest)
			return
		}
		if !validIdRegex.MatchString(entry.Id) {
			app.sendErrorResponse(w, "InvalidBatchEntryId", "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.", http.StatusBadRequest)
			return
		}
		ids[entry.Id] = true
	}

	queueName := path.Base(req.QueueUrl)
	resp, err := app.Store.DeleteMessageBatch(r.Context(), queueName, req.Entries)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (app *App) ChangeMessageVisibilityBatchHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ChangeMessageVisibilityBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

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

	ids := make(map[string]bool)
	validIdRegex := regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)
	for _, entry := range req.Entries {
		if ids[entry.Id] {
			app.sendErrorResponse(w, "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.", http.StatusBadRequest)
			return
		}
		if !validIdRegex.MatchString(entry.Id) {
			app.sendErrorResponse(w, "InvalidBatchEntryId", "The Id of a batch entry in a batch request doesn't abide by the specification.", http.StatusBadRequest)
			return
		}
		ids[entry.Id] = true
	}

	queueName := path.Base(req.QueueUrl)
	resp, err := app.Store.ChangeMessageVisibilityBatch(r.Context(), queueName, req.Entries)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			// Construct batch failure for all entries
			failed := make([]models.BatchResultErrorEntry, len(req.Entries))
			for i, e := range req.Entries {
				failed[i] = models.BatchResultErrorEntry{
					Id:          e.Id,
					Code:        "QueueDoesNotExist",
					Message:     "queue does not exist",
					SenderFault: false,
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(models.ChangeMessageVisibilityBatchResponse{
				Successful: []models.ChangeMessageVisibilityBatchResultEntry{},
				Failed:     failed,
			})
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	if resp.Successful == nil {
		resp.Successful = []models.ChangeMessageVisibilityBatchResultEntry{}
	}
	if resp.Failed == nil {
		resp.Failed = []models.BatchResultErrorEntry{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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
	if len(req.Label) > 80 || !validIdRegex.MatchString(req.Label) {
		app.sendErrorResponse(w, "InvalidParameterValue", "The label must be alphanumeric, hyphen, or underscore and at most 80 characters.", http.StatusBadRequest)
		return
	}
	for _, id := range req.AWSAccountIds {
		if !validAccountIdRegex.MatchString(id) {
			app.sendErrorResponse(w, "InvalidParameterValue", "AWS account ID must be 12 digits.", http.StatusBadRequest)
			return
		}
	}

	queueName := path.Base(req.QueueUrl)
	if err := app.Store.AddPermission(r.Context(), queueName, req.Label, req.AWSAccountIds, req.Actions); err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (app *App) RemovePermissionHandler(w http.ResponseWriter, r *http.Request) {
	var req models.RemovePermissionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	queueName := path.Base(req.QueueUrl)
	if err := app.Store.RemovePermission(r.Context(), queueName, req.Label); err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrLabelDoesNotExist) {
			app.sendErrorResponse(w, "InvalidParameterValue", "The specified label does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (app *App) ListQueueTagsHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListQueueTagsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
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
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.ListQueueTagsResponse{Tags: tags})
}
func (app *App) TagQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.TagQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
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
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
func (app *App) UntagQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.UntagQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
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
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
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
	queueName := path.Base(req.QueueUrl)

	// SQS defaults
	maxResults := 1000
	if req.MaxResults != nil {
		if *req.MaxResults < 1 || *req.MaxResults > 1000 {
			app.sendErrorResponse(w, "InvalidParameterValue", "MaxResults must be an integer between 1 and 1000.", http.StatusBadRequest)
			return
		}
		maxResults = *req.MaxResults
	}

	names, nextToken, err := app.Store.ListDeadLetterSourceQueues(r.Context(), queueName, maxResults, req.NextToken)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	urls := make([]string, len(names))
	for i, n := range names {
		urls[i] = fmt.Sprintf("http://%s/queues/%s", r.Host, n)
	}

	resp := models.ListDeadLetterSourceQueuesResponse{QueueUrls: urls, NextToken: nextToken}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
func (app *App) StartMessageMoveTaskHandler(w http.ResponseWriter, r *http.Request) {
	var req models.StartMessageMoveTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.SourceArn == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a SourceArn.", http.StatusBadRequest)
		return
	}
	if !arnRegex.MatchString(req.SourceArn) {
		app.sendErrorResponse(w, "InvalidParameterValue", "Invalid SourceArn.", http.StatusBadRequest)
		return
	}

	taskHandle, err := app.Store.StartMessageMoveTask(r.Context(), req.SourceArn, req.DestinationArn)
	if err != nil {
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(models.StartMessageMoveTaskResponse{TaskHandle: taskHandle})
}

func (app *App) CancelMessageMoveTaskHandler(w http.ResponseWriter, r *http.Request) {
	var req models.CancelMessageMoveTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.TaskHandle == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a TaskHandle.", http.StatusBadRequest)
		return
	}

	if err := app.Store.CancelMessageMoveTask(r.Context(), req.TaskHandle); err != nil {
		if err.Error() == "ResourceNotFoundException" {
			app.sendErrorResponse(w, "ResourceNotFoundException", "The specified task handle does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(models.CancelMessageMoveTaskResponse{
		TaskHandle: req.TaskHandle,
		Status:     "CANCELLED",
	})
}

func (app *App) ListMessageMoveTasksHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListMessageMoveTasksRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.SourceArn == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a SourceArn.", http.StatusBadRequest)
		return
	}

	tasks, err := app.Store.ListMessageMoveTasks(r.Context(), req.SourceArn)
	if err != nil {
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(models.ListMessageMoveTasksResponse{Results: tasks})
}
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
	if req.VisibilityTimeout == nil || *req.VisibilityTimeout < 0 || *req.VisibilityTimeout > 43200 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Value for parameter VisibilityTimeout is invalid. Reason: Must be an integer from 0 to 43200.", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)
	err := app.Store.ChangeMessageVisibility(r.Context(), queueName, req.ReceiptHandle, *req.VisibilityTimeout)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrInvalidReceiptHandle) || strings.Contains(err.Error(), "invalid receipt handle") {
			app.sendErrorResponse(w, "ReceiptHandleIsInvalid", "The specified receipt handle isn't valid.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrMessageNotInflight) || strings.Contains(err.Error(), "isn't in flight") {
			app.sendErrorResponse(w, "MessageNotInflight", "The specified message isn't in flight.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// --- Re-include Create/List/Get/Set handlers from before ---

func (app *App) checkRedrivePolicy(ctx context.Context, sourceQueueName string, redrivePolicyJson string) error {
	var policy struct {
		DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
		MaxReceiveCount     interface{} `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(redrivePolicyJson), &policy); err != nil {
		return errors.New("RedrivePolicy must be a valid JSON object")
	}
	if policy.DeadLetterTargetArn == "" {
		return errors.New("deadLetterTargetArn is required in RedrivePolicy")
	}

	// Parse MaxReceiveCount
	var maxReceiveCount int
	switch v := policy.MaxReceiveCount.(type) {
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return errors.New("maxReceiveCount must be an integer")
		}
		maxReceiveCount = i
	case float64:
		maxReceiveCount = int(v)
	default:
		// SQS sometimes accepts no maxReceiveCount? No, it's required.
		return errors.New("maxReceiveCount must be an integer")
	}

	if maxReceiveCount < 1 || maxReceiveCount > 1000 {
		return errors.New("maxReceiveCount must be between 1 and 1000")
	}

	// Parse DLQ Name from ARN
	parts := strings.Split(policy.DeadLetterTargetArn, ":")
	if len(parts) < 6 {
		return errors.New("Invalid deadLetterTargetArn")
	}
	dlqName := parts[5]

	// Verify DLQ Exists
	dlqAttrs, err := app.Store.GetQueueAttributes(ctx, dlqName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			// SQS specific error message?
			return fmt.Errorf("Value %s for parameter RedrivePolicy is invalid. Reason: Dead letter target does not exist.", policy.DeadLetterTargetArn)
		}
		return err
	}

	// Check RedriveAllowPolicy
	if allowPolicyStr, ok := dlqAttrs["RedriveAllowPolicy"]; ok {
		var allowPolicy struct {
			RedrivePermission string   `json:"redrivePermission"`
			SourceQueueArns   []string `json:"sourceQueueArns"`
		}
		if json.Unmarshal([]byte(allowPolicyStr), &allowPolicy) == nil {
			if allowPolicy.RedrivePermission == "denyAll" {
				return fmt.Errorf("Value %s for parameter RedrivePolicy is invalid. Reason: Queue %s does not allow this queue to use it as a dead letter queue.", policy.DeadLetterTargetArn, policy.DeadLetterTargetArn)
			} else if allowPolicy.RedrivePermission == "byQueue" {
				sourceQueueArn := fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:%s", sourceQueueName)
				allowed := false
				for _, arn := range allowPolicy.SourceQueueArns {
					if arn == sourceQueueArn {
						allowed = true
						break
					}
				}
				if !allowed {
					return fmt.Errorf("Value %s for parameter RedrivePolicy is invalid. Reason: Queue %s does not allow this queue to use it as a dead letter queue.", policy.DeadLetterTargetArn, policy.DeadLetterTargetArn)
				}
			}
		}
	}

	// Check FIFO Compatibility
	sourceIsFifo := strings.HasSuffix(sourceQueueName, ".fifo")
	dlqIsFifo := strings.HasSuffix(dlqName, ".fifo")

	if sourceIsFifo != dlqIsFifo {
		return fmt.Errorf("Value %s for parameter RedrivePolicy is invalid. Reason: The dead letter queue of a FIFO queue must also be a FIFO queue.", policy.DeadLetterTargetArn)
	}

	return nil
}

func (app *App) CreateQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.CreateQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate Queue Name
	if len(req.QueueName) == 0 || len(req.QueueName) > 80 {
		app.sendErrorResponse(w, "InvalidParameterValue", "Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length.", http.StatusBadRequest)
		return
	}
	for _, char := range req.QueueName {
		if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.') {
			app.sendErrorResponse(w, "InvalidParameterValue", "Invalid queue name: Can only include alphanumeric characters, hyphens, and underscores. 1 to 80 in length.", http.StatusBadRequest)
			return
		}
	}

	// Validate Attributes
	if err := ValidateAttributes(req.Attributes); err != nil {
		// Map backend validation errors to SQS error types
		if strings.Contains(err.Error(), "invalid value for") {
			app.sendErrorResponse(w, "InvalidAttributeValue", err.Error(), http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}

	// FIFO Validation
	isFifo := strings.HasSuffix(req.QueueName, ".fifo")
	isFifoAttr := req.Attributes["FifoQueue"] == "true"
	if isFifo && !isFifoAttr {
		app.sendErrorResponse(w, "InvalidParameterValue", "Queue name ends in .fifo but FifoQueue attribute is not 'true'", http.StatusBadRequest)
		return
	}
	if !isFifo {
		if isFifoAttr {
			app.sendErrorResponse(w, "InvalidParameterValue", "FifoQueue attribute is 'true' but queue name does not end in .fifo", http.StatusBadRequest)
			return
		}
		// Check for FIFO-only attributes on standard queue
		fifoAttrs := []string{"ContentBasedDeduplication", "DeduplicationScope", "FifoThroughputLimit"}
		for _, attr := range fifoAttrs {
			if _, ok := req.Attributes[attr]; ok {
				app.sendErrorResponse(w, "InvalidParameterValue", fmt.Sprintf("%s is only valid for FIFO queues", attr), http.StatusBadRequest)
				return
			}
		}
	}

	// Validate RedrivePolicy if present
	if policy, ok := req.Attributes["RedrivePolicy"]; ok {
		if err := app.checkRedrivePolicy(r.Context(), req.QueueName, policy); err != nil {
			app.sendErrorResponse(w, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
			return
		}
	}

	existingAttributes, err := app.Store.CreateQueue(r.Context(), req.QueueName, req.Attributes, req.Tags)
	if err != nil {
		app.sendErrorResponse(w, "InternalFailure", "Failed to create queue", http.StatusInternalServerError)
		return
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	queueURL := fmt.Sprintf("%s://%s/queues/%s", scheme, r.Host, req.QueueName)
	resp := models.CreateQueueResponse{QueueURL: queueURL}
	w.Header().Set("Content-Type", "application/json")

	if existingAttributes != nil {
		// Check if attributes match
		for k, v := range req.Attributes {
			if existingVal, ok := existingAttributes[k]; ok && existingVal != v {
				app.sendErrorResponse(w, "QueueNameExists", "A queue with this name already exists with different attributes.", http.StatusBadRequest)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	json.NewEncoder(w).Encode(resp)
}

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
	if err := app.Store.DeleteQueue(r.Context(), queueName); err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (app *App) ListQueuesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListQueuesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}

	maxResults := 1000
	if req.MaxResults != nil {
		if *req.MaxResults < 1 || *req.MaxResults > 1000 {
			app.sendErrorResponse(w, "InvalidParameterValue", "MaxResults must be an integer between 1 and 1000.", http.StatusBadRequest)
			return
		}
		maxResults = *req.MaxResults
	}

	names, nextToken, err := app.Store.ListQueues(r.Context(), maxResults, req.NextToken, req.QueueNamePrefix)
	if err != nil {
		app.sendErrorResponse(w, "InternalFailure", "Failed to list queues", http.StatusInternalServerError)
		return
	}

	// According to tests, if MaxResults is nil, we don't return NextToken
	if req.MaxResults == nil {
		nextToken = ""
	}

	urls := make([]string, len(names))
	for i, n := range names {
		urls[i] = fmt.Sprintf("http://%s/queues/%s", r.Host, n)
	}

	// Truncate to 1000 if needed
	if len(urls) > 1000 {
		urls = urls[:1000]
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.ListQueuesResponse{QueueUrls: urls, NextToken: nextToken})
}

func (app *App) GetQueueAttributesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.GetQueueAttributesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)
	attrs, err := app.Store.GetQueueAttributes(r.Context(), queueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	// Default values according to SQS
	allPossibleAttributes := map[string]string{
		"DelaySeconds":                  "0",
		"MaximumMessageSize":            "262144",
		"MessageRetentionPeriod":        "345600",
		"ReceiveMessageWaitTimeSeconds": "0",
		"VisibilityTimeout":             "30",
		"KmsDataKeyReusePeriodSeconds":  "300",
	}

	// SQS valid attributes
	validAttributes := map[string]bool{
		"All":                                   true,
		"Policy":                                true,
		"VisibilityTimeout":                     true,
		"MaximumMessageSize":                    true,
		"MessageRetentionPeriod":                true,
		"ApproximateNumberOfMessages":           true,
		"ApproximateNumberOfMessagesNotVisible": true,
		"CreatedTimestamp":                      true,
		"LastModifiedTimestamp":                 true,
		"QueueArn":                              true,
		"ApproximateNumberOfMessagesDelayed":    true,
		"DelaySeconds":                          true,
		"ReceiveMessageWaitTimeSeconds":         true,
		"RedrivePolicy":                         true,
		"FifoQueue":                             true,
		"ContentBasedDeduplication":             true,
		"KmsMasterKeyId":                        true,
		"KmsDataKeyReusePeriodSeconds":          true,
		"DeduplicationScope":                    true,
		"FifoThroughputLimit":                   true,
		"RedriveAllowPolicy":                    true,
		"SqsManagedSseEnabled":                  true,
	}

	filteredAttrs := make(map[string]string)
	returnAll := false
	for _, attr := range req.AttributeNames {
		if attr == "All" {
			returnAll = true
			break
		}
	}

	if returnAll {
		for k, v := range allPossibleAttributes {
			filteredAttrs[k] = v
		}
		for k, v := range attrs {
			filteredAttrs[k] = v
		}
	} else {
		for _, attr := range req.AttributeNames {
			if val, ok := attrs[attr]; ok {
				filteredAttrs[attr] = val
			} else if val, ok := allPossibleAttributes[attr]; ok {
				filteredAttrs[attr] = val
			} else if attr == "QueueArn" {
				// handled below
			} else if validAttributes[attr] {
				// valid but not set, do not include but also do not error
			} else {
				app.sendErrorResponse(w, "InvalidAttributeName", fmt.Sprintf("The specified attribute %s does not exist.", attr), http.StatusBadRequest)
				return
			}
		}
	}

	// Always provide QueueArn if All or explicitly requested
	shouldIncludeArn := returnAll
	if !shouldIncludeArn {
		for _, attr := range req.AttributeNames {
			if attr == "QueueArn" {
				shouldIncludeArn = true
				break
			}
		}
	}
	if shouldIncludeArn {
		filteredAttrs["QueueArn"] = fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:%s", queueName)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models.GetQueueAttributesResponse{Attributes: filteredAttrs})
}

func (app *App) SetQueueAttributesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.SetQueueAttributesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueueUrl == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueUrl.", http.StatusBadRequest)
		return
	}
	if len(req.Attributes) == 0 {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain the parameter Attributes.", http.StatusBadRequest)
		return
	}

	// Validate Attributes Syntax
	if err := ValidateAttributes(req.Attributes); err != nil {
		if strings.Contains(err.Error(), "invalid value for") {
			app.sendErrorResponse(w, "InvalidAttributeValue", err.Error(), http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
		return
	}

	// Validate Immutable Attributes
	if _, ok := req.Attributes["FifoQueue"]; ok {
		app.sendErrorResponse(w, "InvalidAttributeName", "Attribute FifoQueue cannot be changed.", http.StatusBadRequest)
		return
	}

	queueName := path.Base(req.QueueUrl)

	// Validate RedrivePolicy existence and content if present
	if policy, ok := req.Attributes["RedrivePolicy"]; ok {
		if err := app.checkRedrivePolicy(r.Context(), queueName, policy); err != nil {
			app.sendErrorResponse(w, "InvalidParameterValue", err.Error(), http.StatusBadRequest)
			return
		}
	}

	err := app.Store.SetQueueAttributes(r.Context(), queueName, req.Attributes)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (app *App) GetQueueUrlHandler(w http.ResponseWriter, r *http.Request) {
	var req models.GetQueueURLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		app.sendErrorResponse(w, "InvalidRequest", "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.QueueName == "" {
		app.sendErrorResponse(w, "MissingParameter", "The request must contain a QueueName.", http.StatusBadRequest)
		return
	}
	// Check if queue exists
	_, err := app.Store.GetQueueURL(r.Context(), req.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}

	queueURL := fmt.Sprintf("http://%s/queues/%s", r.Host, req.QueueName)
	json.NewEncoder(w).Encode(models.GetQueueURLResponse{QueueUrl: queueURL})
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
	if err := app.Store.PurgeQueue(r.Context(), queueName); err != nil {
		if errors.Is(err, store.ErrQueueDoesNotExist) {
			app.sendErrorResponse(w, "QueueDoesNotExist", "The specified queue does not exist.", http.StatusBadRequest)
			return
		}
		if errors.Is(err, store.ErrPurgeQueueInProgress) {
			app.sendErrorResponse(w, "PurgeQueueInProgress", "Indicates that the specified queue previously received a PurgeQueue request within the last 60 seconds.", http.StatusBadRequest)
			return
		}
		app.sendErrorResponse(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Helpers
type SqsError struct{ Type, Message string }

func (e *SqsError) Error() string { return e.Message }

var validQueueAttributeNames = map[string]bool{"All": true}
