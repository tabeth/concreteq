package main

// AttributeDefinition corresponds to the DynamoDB AttributeDefinition type.
type AttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"` // "S" for string, "N" for number, "B" for binary
}

// KeySchemaElement corresponds to the DynamoDB KeySchemaElement type.
type KeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"` // "HASH" or "RANGE"
}

// ProvisionedThroughput is accepted for compatibility but not used by ConcreteDB.
type ProvisionedThroughput struct {
	ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
	WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
}

// CreateTableRequest mirrors the JSON request body for the CreateTable action.
type CreateTableRequest struct {
	TableName             string                `json:"TableName"`
	AttributeDefinitions  []AttributeDefinition `json:"AttributeDefinitions"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
}

// TableDescription is the core of the CreateTable response.
type TableDescription struct {
	TableName             string                `json:"TableName"`
	TableStatus           string                `json:"TableStatus"`
	AttributeDefinitions  []AttributeDefinition `json:"AttributeDefinitions"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
	CreationDateTime      float64               `json:"CreationDateTime"` // Represented as Unix epoch time
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
	TableSizeBytes        int64                 `json:"TableSizeBytes"`
	ItemCount             int64                 `json:"ItemCount"`
}

// CreateTableResponse mirrors the JSON response for a successful CreateTable action.
type CreateTableResponse struct {
	TableDescription TableDescription `json:"TableDescription"`
}

// ErrorResponse is a generic structure for sending DynamoDB-compatible errors.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"Message"`
}

// DeleteTableRequest mirrors the JSON request body for the DeleteTable action.
type DeleteTableRequest struct {
	TableName string `json:"TableName"`
}

// DeleteTableResponse mirrors the JSON response for a successful DeleteTable action.
type DeleteTableResponse struct {
	TableDescription TableDescription `json:"TableDescription"`
}
