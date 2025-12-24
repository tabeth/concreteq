package models

import "time"

// APIError is a custom error type that holds DynamoDB-compatible error info.
// By placing it in its own package, we avoid import cycles.
type APIError struct {
	Type    string
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

func New(typ, msg string) *APIError {
	return &APIError{Type: typ, Message: msg}
}

// TableStatus represents the lifecycle status of a table.
type TableStatus string

const (
	StatusCreating TableStatus = "CREATING"
	StatusActive   TableStatus = "ACTIVE"
	// For now, "DELETING" won't be used since we will block on deletion when you call DeleteTable. Once things are queued this will be used.
	StatusDeleting TableStatus = "DELETING"
)

// Table is the canonical internal representation of a table's metadata.
// This is the struct that will be persisted in FoundationDB.
type Table struct {
	TableName             string
	Status                TableStatus
	KeySchema             []KeySchemaElement
	AttributeDefinitions  []AttributeDefinition
	ProvisionedThroughput ProvisionedThroughput
	CreationDateTime      time.Time
}

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

// ListTablesRequest mirrors the JSON request body for the ListTables action.
type ListTablesRequest struct {
	ExclusiveStartTableName string `json:"ExclusiveStartTableName"`
	Limit                   int    `json:"Limit"`
}

// ListTablesResponse mirrors the JSON response for a successful ListTables action.
type ListTablesResponse struct {
	TableNames             []string `json:"TableNames"`
	LastEvaluatedTableName string   `json:"LastEvaluatedTableName,omitempty"`
}

// DescribeTableRequest mirrors the JSON request body for the DescribeTable action.
type DescribeTableRequest struct {
	TableName string `json:"TableName"`
}

// DescribeTableResponse mirrors the JSON response for a successful DescribeTable action.
type DescribeTableResponse struct {
	Table TableDescription `json:"Table"`
}

// AttributeValue represents the data for an attribute.
// It is a union type, where only one field should be set.
type AttributeValue struct {
	// S represents a string attribute type.
	// Example: "Bird"
	S *string `json:"S,omitempty"`
	// N represents a number attribute type. Numbers are sent as strings to handle arbitrary precision.
	// Example: "123.45"
	N *string `json:"N,omitempty"`
	// B represents a binary attribute type. Base64 encoded string.
	// Example: "dGhpcyBpcyBhIHRlc3Q="
	B *string `json:"B,omitempty"`
	// SS represents a string set attribute type.
	// Example: ["Giraffe", "Hippo"]
	SS []string `json:"SS,omitempty"`
	// NS represents a number set attribute type.
	// Example: ["42.2", "-19"]
	NS []string `json:"NS,omitempty"`
	// BS represents a binary set attribute type.
	// Example: ["U3Vubnk=", "UmFpbnk="]
	BS []string `json:"BS,omitempty"`
	// M represents a map attribute type.
	// Example: {"Name": {"S": "Joe"}, "Age": {"N": "35"}}
	M map[string]AttributeValue `json:"M,omitempty"`
	// L represents a list attribute type.
	// Example: [{"S": "Cookies"}, {"S": "Coffee"}, {"N": "3.14159"}]
	L []AttributeValue `json:"L,omitempty"`
	// NULL represents a null attribute type.
	// Example: true
	NULL *bool `json:"NULL,omitempty"`
	// BOOL represents a boolean attribute type.
	// Example: true
	BOOL *bool `json:"BOOL,omitempty"`
}

// PutItemRequest mirrors the JSON request body for the PutItem action.
type PutItemRequest struct {
	TableName string                    `json:"TableName"`
	Item      map[string]AttributeValue `json:"Item"`
}

// PutItemResponse mirrors the JSON response for the PutItem action.
// For MVP, this is empty as we don't support ReturnValues yet.
type PutItemResponse struct{}

// GetItemRequest mirrors the JSON request body for the GetItem action.
type GetItemRequest struct {
	TableName string                    `json:"TableName"`
	Key       map[string]AttributeValue `json:"Key"`
}

// GetItemResponse mirrors the JSON response for the GetItem action.
type GetItemResponse struct {
	Item map[string]AttributeValue `json:"Item,omitempty"`
}

// DeleteItemRequest mirrors the JSON request body for the DeleteItem action.
type DeleteItemRequest struct {
	TableName string                    `json:"TableName"`
	Key       map[string]AttributeValue `json:"Key"`
}

// DeleteItemResponse mirrors the JSON response for the DeleteItem action.
type DeleteItemResponse struct{}
