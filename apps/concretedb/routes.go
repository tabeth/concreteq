package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/tabeth/concretedb/models" // Use new error package
	"github.com/tabeth/concretedb/service"
)

// DynamoDBHandler now holds the interface, not the concrete service.
type DynamoDBHandler struct {
	tableService service.TableServicer
}

func NewDynamoDBHandler(service service.TableServicer) *DynamoDBHandler {
	return &DynamoDBHandler{tableService: service}
}

func (h *DynamoDBHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Amz-Target")

	if r.Method != http.MethodPost {
		writeError(w, "UnsupportedOperationException", "Only POST requests are supported.", http.StatusMethodNotAllowed)
		return
	}

	switch target {
	case "DynamoDB_20120810.CreateTable":
		h.createTableHandler(w, r)
	// ADDED: Route for the DeleteTable action
	case "DynamoDB_20120810.DeleteTable":
		h.deleteTableHandler(w, r)
	default:
		writeError(w, "UnknownOperationException", "The requested operation is not supported.", http.StatusBadRequest)
	}
}

func writeError(w http.ResponseWriter, errType, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	errResp := ErrorResponse{
		Type:    errType,
		Message: message,
	}
	json.NewEncoder(w).Encode(errResp)
}

// writeAPIError is a helper for our new structured errors.
func writeAPIError(w http.ResponseWriter, err error, statusCode int) {
	var apiErr *models.APIError
	if errors.As(err, &apiErr) {
		writeError(w, apiErr.Type, apiErr.Message, statusCode)
	} else {
		// Fallback for unexpected errors
		writeError(w, "InternalFailure", err.Error(), http.StatusInternalServerError)
	}
}

func writeSuccess(w http.ResponseWriter, payload interface{}, statusCode int) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	if payload != nil {
		json.NewEncoder(w).Encode(payload)
	}
}
