package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	// Queue Management
	r.Post("/queues", app.CreateQueueHandler)
	r.Delete("/queues/{queueName}", app.DeleteQueueHandler)
	r.Get("/queues", app.ListQueuesHandler)
	r.Get("/queues/{queueName}/attributes", app.GetQueueAttributesHandler)
	r.Put("/queues/{queueName}/attributes", app.SetQueueAttributesHandler)
	r.Get("/queues/url/{queueName}", app.GetQueueUrlHandler)
	r.Post("/queues/{queueName}/purge", app.PurgeQueueHandler)

	// Message Management
	r.Post("/queues/{queueName}/messages", app.SendMessageHandler)
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
	queueURL := fmt.Sprintf("http://localhost:8080/queues/%s", req.QueueName)

	resp := models.CreateQueueResponse{
		QueueURL: queueURL,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

// --- Other Handler Stubs ---

func (app *App) DeleteQueueHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) ListQueuesHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
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
	w.WriteHeader(http.StatusNotImplemented)
}
func (app *App) SendMessageHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
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
