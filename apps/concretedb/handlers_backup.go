package main

import (
	"encoding/json"
	"net/http"

	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/service"
)

func createBackupHandler(svc service.TableServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req models.CreateBackupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.CreateBackup(r.Context(), &req)
		if err != nil {
			writeAPIError(w, err, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(resp)
	}
}

func deleteBackupHandler(svc service.TableServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req models.DeleteBackupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.DeleteBackup(r.Context(), &req)
		if err != nil {
			writeAPIError(w, err, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(resp)
	}
}

func listBackupsHandler(svc service.TableServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req models.ListBackupsRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.ListBackups(r.Context(), &req)
		if err != nil {
			writeAPIError(w, err, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(resp)
	}
}

func describeBackupHandler(svc service.TableServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req models.DescribeBackupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.DescribeBackup(r.Context(), &req)
		if err != nil {
			writeAPIError(w, err, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(resp)
	}
}

func restoreTableFromBackupHandler(svc service.TableServicer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req models.RestoreTableFromBackupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.RestoreTableFromBackup(r.Context(), &req)
		if err != nil {
			writeAPIError(w, err, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		json.NewEncoder(w).Encode(resp)
	}
}
