package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/tabeth/concretedb/models"
)

// createTableHandler handles the API logic for the CreateTable action.
func (h *DynamoDBHandler) createTableHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	// 1. Map API request model to internal DB model
	tableToCreate := mapRequestToDBTable(&req)

	// 2. Call the service
	createdTable, err := h.tableService.CreateTable(r.Context(), tableToCreate)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest) // Use our new helper
		return
	}

	// 3. Map internal DB model back to API response model
	resp := mapDBTableToCreateResponse(createdTable)
	writeSuccess(w, resp, http.StatusOK)
}

// deleteTableHandler handles the API logic for the DeleteTable action.
func (h *DynamoDBHandler) deleteTableHandler(w http.ResponseWriter, r *http.Request) {
	var req DeleteTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	// Call the service with just the table name
	deletedTable, err := h.tableService.DeleteTable(r.Context(), req.TableName)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest) // Our helper handles apperror types
		return
	}

	// Map the internal models.Table model back to an API response
	resp := mapDBTableToDeleteResponse(deletedTable)
	writeSuccess(w, resp, http.StatusOK)
}

// mapDBTableToDeleteResponse converts the internal DB model to the API response for DeleteTable.
// This is very similar to the CreateTable mapping.
func mapDBTableToDeleteResponse(table *models.Table) *DeleteTableResponse {
	apiDesc := mapDBTableToAPIDescription(table)
	return &DeleteTableResponse{TableDescription: apiDesc}
}

// Helper function to avoid code duplication between create and delete responses.
func mapDBTableToAPIDescription(table *models.Table) TableDescription {
	apiDesc := TableDescription{
		TableName:        table.TableName,
		TableStatus:      string(table.Status),
		ItemCount:        0, // In a real system, you'd have these values
		TableSizeBytes:   0, // Similarly in a real system you'd have real values here too.
		CreationDateTime: float64(table.CreationDateTime.Unix()),
	}
	for _, ad := range table.AttributeDefinitions {
		apiDesc.AttributeDefinitions = append(apiDesc.AttributeDefinitions, AttributeDefinition{
			AttributeName: ad.AttributeName,
			AttributeType: ad.AttributeType,
		})
	}
	for _, kse := range table.KeySchema {
		apiDesc.KeySchema = append(apiDesc.KeySchema, KeySchemaElement{
			AttributeName: kse.AttributeName,
			KeyType:       kse.KeyType,
		})
	}
	apiDesc.ProvisionedThroughput = ProvisionedThroughput{
		ReadCapacityUnits:  table.ProvisionedThroughput.ReadCapacityUnits,
		WriteCapacityUnits: table.ProvisionedThroughput.WriteCapacityUnits,
	}
	return apiDesc
}

func mapDBTableToCreateResponse(table *models.Table) *CreateTableResponse {
	apiDesc := mapDBTableToAPIDescription(table)
	return &CreateTableResponse{TableDescription: apiDesc}
}

// mapRequestToDBTable is responsible for converting API types to DB types.
func mapRequestToDBTable(req *CreateTableRequest) *models.Table {
	dbTable := &models.Table{
		TableName:        req.TableName,
		Status:           models.StatusCreating,
		CreationDateTime: time.Now().UTC(),
	}
	for _, ad := range req.AttributeDefinitions {
		dbTable.AttributeDefinitions = append(dbTable.AttributeDefinitions, models.AttributeDefinition{
			AttributeName: ad.AttributeName,
			AttributeType: ad.AttributeType,
		})
	}
	for _, kse := range req.KeySchema {
		dbTable.KeySchema = append(dbTable.KeySchema, models.KeySchemaElement{
			AttributeName: kse.AttributeName,
			KeyType:       kse.KeyType,
		})
	}
	dbTable.ProvisionedThroughput = models.ProvisionedThroughput{
		ReadCapacityUnits:  req.ProvisionedThroughput.ReadCapacityUnits,
		WriteCapacityUnits: req.ProvisionedThroughput.WriteCapacityUnits,
	}
	return dbTable
}
