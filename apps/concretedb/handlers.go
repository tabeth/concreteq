package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/tabeth/concretedb/models"
)

// createTableHandler handles the API logic for the CreateTable action.
func (h *DynamoDBHandler) createTableHandler(w http.ResponseWriter, r *http.Request) {
	var req models.CreateTableRequest
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
	var req models.DeleteTableRequest
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
func mapDBTableToDeleteResponse(table *models.Table) *models.DeleteTableResponse {
	apiDesc := mapDBTableToAPIDescription(table)
	return &models.DeleteTableResponse{TableDescription: apiDesc}
}

// Helper function to avoid code duplication between create and delete responses.
func mapDBTableToAPIDescription(table *models.Table) models.TableDescription {
	apiDesc := models.TableDescription{
		TableName:        table.TableName,
		TableStatus:      string(table.Status),
		ItemCount:        0, // In a real system, you'd have these values
		TableSizeBytes:   0, // Similarly in a real system you'd have real values here too.
		CreationDateTime: float64(table.CreationDateTime.Unix()),
	}
	for _, ad := range table.AttributeDefinitions {
		apiDesc.AttributeDefinitions = append(apiDesc.AttributeDefinitions, models.AttributeDefinition{
			AttributeName: ad.AttributeName,
			AttributeType: ad.AttributeType,
		})
	}
	for _, kse := range table.KeySchema {
		apiDesc.KeySchema = append(apiDesc.KeySchema, models.KeySchemaElement{
			AttributeName: kse.AttributeName,
			KeyType:       kse.KeyType,
		})
	}
	apiDesc.ProvisionedThroughput = models.ProvisionedThroughput{
		ReadCapacityUnits:  table.ProvisionedThroughput.ReadCapacityUnits,
		WriteCapacityUnits: table.ProvisionedThroughput.WriteCapacityUnits,
	}
	return apiDesc
}

func mapDBTableToCreateResponse(table *models.Table) *models.CreateTableResponse {
	apiDesc := mapDBTableToAPIDescription(table)
	return &models.CreateTableResponse{TableDescription: apiDesc}
}

// mapRequestToDBTable is responsible for converting API types to DB types.
func mapRequestToDBTable(req *models.CreateTableRequest) *models.Table {
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

// listTablesHandler handles the API logic for the ListTables action.
func (h *DynamoDBHandler) listTablesHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ListTablesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	tableNames, lastEval, err := h.tableService.ListTables(r.Context(), req.Limit, req.ExclusiveStartTableName)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	resp := models.ListTablesResponse{
		TableNames:             tableNames,
		LastEvaluatedTableName: lastEval,
	}
	writeSuccess(w, resp, http.StatusOK)
}

// describeTableHandler handles the API logic for the DescribeTable action.
func (h *DynamoDBHandler) describeTableHandler(w http.ResponseWriter, r *http.Request) {
	var req models.DescribeTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	table, err := h.tableService.GetTable(r.Context(), req.TableName)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	apiDesc := mapDBTableToAPIDescription(table)
	resp := models.DescribeTableResponse{
		Table: apiDesc,
	}
	writeSuccess(w, resp, http.StatusOK)
}

// putItemHandler handles the API logic for the PutItem action.
func (h *DynamoDBHandler) putItemHandler(w http.ResponseWriter, r *http.Request) {
	var req models.PutItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	// 2. Call the service
	resp, err := h.tableService.PutItem(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}

// getItemHandler handles the API logic for the GetItem action.
func (h *DynamoDBHandler) getItemHandler(w http.ResponseWriter, r *http.Request) {
	var req models.GetItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.GetItem(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	// If item is nil, we still return success but with an empty/omitted Item field in the response.
	writeSuccess(w, resp, http.StatusOK)
}

// deleteItemHandler handles the API logic for the DeleteItem action.
func (h *DynamoDBHandler) deleteItemHandler(w http.ResponseWriter, r *http.Request) {
	var req models.DeleteItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body.", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.DeleteItem(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}

// UpdateItemHandler handles UpdateItem requests.
func (h *DynamoDBHandler) UpdateItemHandler(w http.ResponseWriter, r *http.Request) {
	var req models.UpdateItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.UpdateItem(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}

// ScanHandler handles Scan requests.
func (h *DynamoDBHandler) ScanHandler(w http.ResponseWriter, r *http.Request) {
	var req models.ScanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.Scan(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}

// QueryHandler handles Query requests.
func (h *DynamoDBHandler) QueryHandler(w http.ResponseWriter, r *http.Request) {
	var req models.QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "SerializationException", "Could not decode request body", http.StatusBadRequest)
		return
	}

	resp, err := h.tableService.Query(r.Context(), &req)
	if err != nil {
		writeAPIError(w, err, http.StatusBadRequest)
		return
	}

	writeSuccess(w, resp, http.StatusOK)
}
