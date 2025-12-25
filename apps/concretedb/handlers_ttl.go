package main

import (
	"encoding/json"
	"net/http"

	"github.com/tabeth/concretedb/models"
)

// updateTimeToLiveHandler handles the API logic for the UpdateTimeToLive action.
func (h *DynamoDBHandler) updateTimeToLiveHandler(w http.ResponseWriter, r *http.Request) {
	var req models.UpdateTimeToLiveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.UpdateTimeToLive(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}

// describeTimeToLiveHandler handles the API logic for the DescribeTimeToLive action.
func (h *DynamoDBHandler) describeTimeToLiveHandler(w http.ResponseWriter, r *http.Request) {
	var req models.DescribeTimeToLiveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.DescribeTimeToLive(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}
