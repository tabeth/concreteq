package models

import "time"

// TableStatus represents the lifecycle status of a table.
type TableStatus string

const (
	StatusCreating TableStatus = "CREATING"
	StatusActive   TableStatus = "ACTIVE"
	// For now, "DELETING" won't be used since we will block on deletion when you call DeleteTable. Once things are queued this will be used.
	StatusDeleting TableStatus = "DELETING"
)

// These types are now defined locally within the db package,
// removing the dependency on the 'api' package.
type AttributeDefinition struct {
	AttributeName string
	AttributeType string
}

type KeySchemaElement struct {
	AttributeName string
	KeyType       string
}

type ProvisionedThroughput struct {
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

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
