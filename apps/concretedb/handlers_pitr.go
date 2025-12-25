package main

import (
	"encoding/json"
	"net/http"

	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/service"
)

func updateContinuousBackupsHandler(svc service.TableServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req models.UpdateContinuousBackupsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.UpdateContinuousBackups(r.Context(), &req)
		if err != nil {
			writeAPIError(w, err, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(resp)
	}
}

func describeContinuousBackupsHandler(svc service.TableServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req models.DescribeContinuousBackupsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.DescribeContinuousBackups(r.Context(), req.TableName)
		if err != nil {
			writeAPIError(w, err, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(resp)
	}
}

func restoreTableToPointInTimeHandler(svc service.TableServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req models.RestoreTableToPointInTimeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.RestoreTableToPointInTime(r.Context(), &req)
		if err != nil {
			writeAPIError(w, err, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(resp)
	}
}
