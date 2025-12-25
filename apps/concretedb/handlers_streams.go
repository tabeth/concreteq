package main

import (
	"encoding/json"
	"net/http"

	"github.com/tabeth/concretedb/models"
)

// listStreamsHandler handles the API logic for the ListStreams action.
func (h *DynamoDBHandler) listStreamsHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListStreamsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.ListStreams(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}

// describeStreamHandler handles the API logic for the DescribeStream action.
func (h *DynamoDBHandler) describeStreamHandler(w http.ResponseWriter, r *http.Request) {
	var req models.DescribeStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.DescribeStream(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}

// getShardIteratorHandler handles the API logic for the GetShardIterator action.
func (h *DynamoDBHandler) getShardIteratorHandler(w http.ResponseWriter, r *http.Request) {
	var req models.GetShardIteratorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.GetShardIterator(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}

// getRecordsHandler handles the API logic for the GetRecords action.
func (h *DynamoDBHandler) getRecordsHandler(w http.ResponseWriter, r *http.Request) {
	var req models.GetRecordsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.GetRecords(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}
