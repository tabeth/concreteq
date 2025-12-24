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
	case "DynamoDB_20120810.ListTables":
		h.listTablesHandler(w, r)
	case "DynamoDB_20120810.DescribeTable":
		h.describeTableHandler(w, r)
	case "DynamoDB_20120810.PutItem":
		h.putItemHandler(w, r)
	case "DynamoDB_20120810.GetItem":
		h.getItemHandler(w, r)
	case "DynamoDB_20120810.DeleteItem":
		h.deleteItemHandler(w, r)
	case "DynamoDB_20120810.UpdateItem":
		h.UpdateItemHandler(w, r)
	case "DynamoDB_20120810.Scan":
		h.ScanHandler(w, r)
	case "DynamoDB_20120810.Query":
		h.QueryHandler(w, r)
	case "DynamoDB_20120810.BatchGetItem":
		h.batchGetItemHandler(w, r)
	case "DynamoDB_20120810.BatchWriteItem":
		h.batchWriteItemHandler(w, r)
	case "DynamoDB_20120810.TransactGetItems":
		h.transactGetItemsHandler(w, r)
	case "DynamoDB_20120810.TransactWriteItems":
		h.transactWriteItemsHandler(w, r)
	default:
		writeError(w, "UnknownOperationException", "The requested operation is not supported.", http.StatusBadRequest)
	}
}

func writeError(w http.ResponseWriter, errType, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	errResp := models.ErrorResponse{
		Type:    errType,
		Message: message,
	}
	json.NewEncoder(w).Encode(errResp)
}

// writeAPIError is a helper for our new structured errors.
func writeAPIError(w http.ResponseWriter, err error, statusCode int) {
	var apiErr *models.APIError
	if errors.As(err, &apiErr) {
		code := statusCode
		if apiErr.Type == "InternalFailure" {
			code = http.StatusInternalServerError
		}
		writeError(w, apiErr.Type, apiErr.Error(), code)
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
