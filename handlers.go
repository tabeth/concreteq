package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"

	"github.com/tabeth/concreteq/models"
	"github.com/tabeth/concreteq/store"

	"github.com/go-chi/chi/v5"
)

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
var queueNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

// CreateQueueHandler handles requests to create a new queue.
func (app *App) CreateQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req models.CreateQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if !queueNameRegex.MatchString(req.QueueName) {
		http.Error(w, "Invalid queue name", http.StatusBadRequest)
		return
	}

	err := app.Store.CreateQueue(r.Context(), req.QueueName)
	if err != nil {
		// A more robust implementation would check for specific error types
		// (e.g., queue already exists) and return different status codes.
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
