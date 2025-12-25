package models

import "time"

// APIError is a custom error type that holds DynamoDB-compatible error info.
// By placing it in its own package, we avoid import cycles.
type APIError struct {
	Type    string
	Message string
}

func (e *APIError) Error() string {
	if e.Type != "" {
		return e.Type + ": " + e.Message
	}
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
	TableName              string
	Status                 TableStatus
	KeySchema              []KeySchemaElement
	AttributeDefinitions   []AttributeDefinition
	GlobalSecondaryIndexes []GlobalSecondaryIndex
	LocalSecondaryIndexes  []LocalSecondaryIndex
	ProvisionedThroughput  ProvisionedThroughput
	StreamSpecification    *StreamSpecification
	CreationDateTime       time.Time
	LatestStreamLabel      string
	LatestStreamArn        string
	Replicas               []ReplicaDescription
	TimeToLiveDescription  *TimeToLiveDescription
}

type TimeToLiveDescription struct {
	AttributeName    string `json:"AttributeName,omitempty"`
	TimeToLiveStatus string `json:"TimeToLiveStatus,omitempty"` // ENABLING, DISABLING, ENABLED, DISABLED
}

type TimeToLiveSpecification struct {
	Enabled       bool   `json:"Enabled"`
	AttributeName string `json:"AttributeName,omitempty"`
}

type ReplicaDescription struct {
	RegionName                    string                                   `json:"RegionName"`
	ReplicaStatus                 string                                   `json:"ReplicaStatus"` // CREATING, ACTIVE, UPDATING, DELETING, CREATION_FAILED, UPDATING_FAILED, DELETING_FAILED
	ReplicaStatusDescription      string                                   `json:"ReplicaStatusDescription,omitempty"`
	ReplicaStatusPercentProgress  string                                   `json:"ReplicaStatusPercentProgress,omitempty"`
	KMSMasterKeyId                string                                   `json:"KMSMasterKeyId,omitempty"`
	ProvisionedThroughputOverride *ProvisionedThroughputOverride           `json:"ProvisionedThroughputOverride,omitempty"`
	GlobalSecondaryIndexes        []ReplicaGlobalSecondaryIndexDescription `json:"GlobalSecondaryIndexes,omitempty"`
}

type ReplicaGlobalSecondaryIndexDescription struct {
	IndexName                     string                         `json:"IndexName"`
	ProvisionedThroughputOverride *ProvisionedThroughputOverride `json:"ProvisionedThroughputOverride,omitempty"`
}

type ProvisionedThroughputOverride struct {
	ReadCapacityUnits int64 `json:"ReadCapacityUnits,omitempty"`
}

// Projection represents attributes that are copied (projected) from the table into an index.
type Projection struct {
	ProjectionType   string   `json:"ProjectionType"` // KEYS_ONLY, INCLUDE, ALL
	NonKeyAttributes []string `json:"NonKeyAttributes,omitempty"`
}

// GlobalSecondaryIndex Represents the properties of a global secondary index.
type GlobalSecondaryIndex struct {
	IndexName             string                `json:"IndexName"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
	Projection            Projection            `json:"Projection"`
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
	IndexStatus           string                `json:"IndexStatus,omitempty"` // CREATING, ACTIVE, DELETING, UPDATING
	IndexSizeBytes        int64                 `json:"IndexSizeBytes,omitempty"`
	ItemCount             int64                 `json:"ItemCount,omitempty"`
}

// LocalSecondaryIndex Represents the properties of a local secondary index.
type LocalSecondaryIndex struct {
	IndexName  string             `json:"IndexName"`
	KeySchema  []KeySchemaElement `json:"KeySchema"`
	Projection Projection         `json:"Projection"`
	// LSI shares throughput with the table, so no ProvisionedThroughput field.
	IndexSizeBytes int64 `json:"IndexSizeBytes,omitempty"`
	ItemCount      int64 `json:"ItemCount,omitempty"`
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
	TableName              string                 `json:"TableName"`
	AttributeDefinitions   []AttributeDefinition  `json:"AttributeDefinitions"`
	KeySchema              []KeySchemaElement     `json:"KeySchema"`
	GlobalSecondaryIndexes []GlobalSecondaryIndex `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []LocalSecondaryIndex  `json:"LocalSecondaryIndexes,omitempty"`
	ProvisionedThroughput  ProvisionedThroughput  `json:"ProvisionedThroughput"`
	StreamSpecification    *StreamSpecification   `json:"StreamSpecification,omitempty"`
}

// TableDescription is the core of the CreateTable response.
type TableDescription struct {
	TableName              string                 `json:"TableName"`
	TableStatus            string                 `json:"TableStatus"`
	AttributeDefinitions   []AttributeDefinition  `json:"AttributeDefinitions"`
	KeySchema              []KeySchemaElement     `json:"KeySchema"`
	CreationDateTime       float64                `json:"CreationDateTime"` // Represented as Unix epoch time
	ProvisionedThroughput  ProvisionedThroughput  `json:"ProvisionedThroughput"`
	TableSizeBytes         int64                  `json:"TableSizeBytes"`
	ItemCount              int64                  `json:"ItemCount"`
	GlobalSecondaryIndexes []GlobalSecondaryIndex `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []LocalSecondaryIndex  `json:"LocalSecondaryIndexes,omitempty"`
	StreamSpecification    *StreamSpecification   `json:"StreamSpecification,omitempty"`
	LatestStreamLabel      string                 `json:"LatestStreamLabel,omitempty"`
	LatestStreamArn        string                 `json:"LatestStreamArn,omitempty"`
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

type UpdateTableRequest struct {
	TableName                   string                       `json:"TableName"`
	AttributeDefinitions        []AttributeDefinition        `json:"AttributeDefinitions,omitempty"`
	GlobalSecondaryIndexUpdates []GlobalSecondaryIndexUpdate `json:"GlobalSecondaryIndexUpdates,omitempty"`
	ProvisionedThroughput       *ProvisionedThroughput       `json:"ProvisionedThroughput,omitempty"`
	StreamSpecification         *StreamSpecification         `json:"StreamSpecification,omitempty"`
	ReplicaUpdates              []ReplicaUpdate              `json:"ReplicaUpdates,omitempty"`
}

type UpdateTableResponse struct {
	TableDescription TableDescription `json:"TableDescription"`
}

type GlobalSecondaryIndexUpdate struct {
	Update *UpdateGlobalSecondaryIndexAction `json:"Update,omitempty"`
	Create *CreateGlobalSecondaryIndexAction `json:"Create,omitempty"`
	Delete *DeleteGlobalSecondaryIndexAction `json:"Delete,omitempty"`
}

type UpdateGlobalSecondaryIndexAction struct {
	IndexName             string                `json:"IndexName"`
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
}

type CreateGlobalSecondaryIndexAction struct {
	IndexName             string                `json:"IndexName"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
	Projection            Projection            `json:"Projection"`
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
}

type DeleteGlobalSecondaryIndexAction struct {
	IndexName string `json:"IndexName"`
}

type ReplicaUpdate struct {
	Create *CreateReplicaAction `json:"Create,omitempty"`
	Delete *DeleteReplicaAction `json:"Delete,omitempty"`
}

type CreateReplicaAction struct {
	RegionName string `json:"RegionName"`
}

type DeleteReplicaAction struct {
	RegionName string `json:"RegionName"`
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

// Capacity represents the capacity units consumed by an operation.
type Capacity struct {
	ReadCapacityUnits  float64 `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits float64 `json:"WriteCapacityUnits,omitempty"`
	CapacityUnits      float64 `json:"CapacityUnits,omitempty"`
}

// ConsumedCapacity represents the capacity consumed by an operation on a table and its indexes.
type ConsumedCapacity struct {
	TableName              string              `json:"TableName,omitempty"`
	CapacityUnits          float64             `json:"CapacityUnits,omitempty"`
	ReadCapacityUnits      float64             `json:"ReadCapacityUnits,omitempty"`
	WriteCapacityUnits     float64             `json:"WriteCapacityUnits,omitempty"`
	Table                  *Capacity           `json:"Table,omitempty"`
	LocalSecondaryIndexes  map[string]Capacity `json:"LocalSecondaryIndexes,omitempty"`
	GlobalSecondaryIndexes map[string]Capacity `json:"GlobalSecondaryIndexes,omitempty"`
}

// ItemCollectionMetrics represents metrics for item collections.
type ItemCollectionMetrics struct {
	ItemCollectionKey   map[string]AttributeValue `json:"ItemCollectionKey,omitempty"`
	SizeEstimateRangeGB []float64                 `json:"SizeEstimateRangeGB,omitempty"`
}

// PutItemRequest mirrors the JSON request body for the PutItem action.
type PutItemRequest struct {
	TableName                   string                    `json:"TableName"`
	Item                        map[string]AttributeValue `json:"Item"`
	ConditionExpression         string                    `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames    map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues   map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues                string                    `json:"ReturnValues,omitempty"` // NONE, ALL_OLD
	ReturnConsumedCapacity      string                    `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                    `json:"ReturnItemCollectionMetrics,omitempty"`
}

// PutItemResponse mirrors the JSON response for the PutItem action.
type PutItemResponse struct {
	Attributes            map[string]AttributeValue `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity         `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics    `json:"ItemCollectionMetrics,omitempty"`
}

// GetItemRequest mirrors the JSON request body for the GetItem action.
type GetItemRequest struct {
	TableName                string                    `json:"TableName"`
	Key                      map[string]AttributeValue `json:"Key"`
	ConsistentRead           bool                      `json:"ConsistentRead,omitempty"`
	ProjectionExpression     string                    `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ReturnConsumedCapacity   string                    `json:"ReturnConsumedCapacity,omitempty"`
}

// GetItemResponse mirrors the JSON response for the GetItem action.
type GetItemResponse struct {
	Item             map[string]AttributeValue `json:"Item,omitempty"`
	ConsumedCapacity *ConsumedCapacity         `json:"ConsumedCapacity,omitempty"`
}

// DeleteItemRequest mirrors the JSON request body for the DeleteItem action.
type DeleteItemRequest struct {
	TableName                   string                    `json:"TableName"`
	Key                         map[string]AttributeValue `json:"Key"`
	ConditionExpression         string                    `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames    map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues   map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues                string                    `json:"ReturnValues,omitempty"` // NONE, ALL_OLD
	ReturnConsumedCapacity      string                    `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                    `json:"ReturnItemCollectionMetrics,omitempty"`
}

// DeleteItemResponse mirrors the JSON response for the DeleteItem action.
type DeleteItemResponse struct {
	Attributes            map[string]AttributeValue `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity         `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics    `json:"ItemCollectionMetrics,omitempty"`
}

// UpdateItemRequest mirrors the JSON request body for the UpdateItem action.
type UpdateItemRequest struct {
	TableName                   string                    `json:"TableName"`
	Key                         map[string]AttributeValue `json:"Key"`
	UpdateExpression            string                    `json:"UpdateExpression"`
	ConditionExpression         string                    `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames    map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues   map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues                string                    `json:"ReturnValues,omitempty"` // NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW
	ReturnConsumedCapacity      string                    `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                    `json:"ReturnItemCollectionMetrics,omitempty"`
}

// UpdateItemResponse mirrors the JSON response for the UpdateItem action.
type UpdateItemResponse struct {
	Attributes            map[string]AttributeValue `json:"Attributes,omitempty"`
	ConsumedCapacity      *ConsumedCapacity         `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics *ItemCollectionMetrics    `json:"ItemCollectionMetrics,omitempty"`
}

// ScanRequest mirrors the JSON request body for the Scan action.
type ScanRequest struct {
	TableName                 string                    `json:"TableName"`
	Limit                     int32                     `json:"Limit,omitempty"`
	ExclusiveStartKey         map[string]AttributeValue `json:"ExclusiveStartKey,omitempty"`
	FilterExpression          string                    `json:"FilterExpression,omitempty"`
	ProjectionExpression      string                    `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	ConsistentRead            bool                      `json:"ConsistentRead,omitempty"`
	ReturnConsumedCapacity    string                    `json:"ReturnConsumedCapacity,omitempty"`
}

// ScanResponse mirrors the JSON response for the Scan action.
type ScanResponse struct {
	Items            []map[string]AttributeValue `json:"Items"`
	Count            int32                       `json:"Count"`
	ScannedCount     int32                       `json:"ScannedCount"`
	LastEvaluatedKey map[string]AttributeValue   `json:"LastEvaluatedKey,omitempty"`
	ConsumedCapacity *ConsumedCapacity           `json:"ConsumedCapacity,omitempty"`
}

// QueryRequest mirrors the JSON request body for the Query action.
type QueryRequest struct {
	TableName                 string                    `json:"TableName"`
	IndexName                 string                    `json:"IndexName,omitempty"`
	KeyConditionExpression    string                    `json:"KeyConditionExpression"`
	FilterExpression          string                    `json:"FilterExpression,omitempty"`
	ProjectionExpression      string                    `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames  map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	Limit                     int32                     `json:"Limit,omitempty"`
	ExclusiveStartKey         map[string]AttributeValue `json:"ExclusiveStartKey,omitempty"`
	ScanIndexForward          *bool                     `json:"ScanIndexForward,omitempty"`
	ConsistentRead            bool                      `json:"ConsistentRead,omitempty"`
	ReturnConsumedCapacity    string                    `json:"ReturnConsumedCapacity,omitempty"`
}

// QueryResponse mirrors the JSON response for the Query action.
type QueryResponse struct {
	Items            []map[string]AttributeValue `json:"Items"`
	Count            int32                       `json:"Count"`
	ScannedCount     int32                       `json:"ScannedCount"`
	LastEvaluatedKey map[string]AttributeValue   `json:"LastEvaluatedKey,omitempty"`
	ConsumedCapacity *ConsumedCapacity           `json:"ConsumedCapacity,omitempty"`
}

// KeysAndAttributes represents the keys and attributes to get for a table in BatchGetItem.
type KeysAndAttributes struct {
	Keys                     []map[string]AttributeValue `json:"Keys"`
	AttributesToGet          []string                    `json:"AttributesToGet,omitempty"` // Legacy, but supported
	ConsistentRead           bool                        `json:"ConsistentRead,omitempty"`
	ProjectionExpression     string                      `json:"ProjectionExpression,omitempty"`
	ExpressionAttributeNames map[string]string           `json:"ExpressionAttributeNames,omitempty"`
}

// BatchGetItemRequest mirrors the JSON request body for the BatchGetItem action.
type BatchGetItemRequest struct {
	RequestItems           map[string]KeysAndAttributes `json:"RequestItems"`
	ReturnConsumedCapacity string                       `json:"ReturnConsumedCapacity,omitempty"`
}

// BatchGetItemResponse mirrors the JSON response for the BatchGetItem action.
type BatchGetItemResponse struct {
	Responses        map[string][]map[string]AttributeValue `json:"Responses,omitempty"`
	UnprocessedKeys  map[string]KeysAndAttributes           `json:"UnprocessedKeys,omitempty"`
	ConsumedCapacity []ConsumedCapacity                     `json:"ConsumedCapacity,omitempty"`
}

// WriteRequest represents a single write request (Put or Delete) in BatchWriteItem.
// Only one of PutRequest or DeleteRequest should be set.
type WriteRequest struct {
	PutRequest    *PutRequest    `json:"PutRequest,omitempty"`
	DeleteRequest *DeleteRequest `json:"DeleteRequest,omitempty"`
}

type PutRequest struct {
	Item map[string]AttributeValue `json:"Item"`
}

type DeleteRequest struct {
	Key map[string]AttributeValue `json:"Key"`
}

// BatchWriteItemRequest mirrors the JSON request body for the BatchWriteItem action.
type BatchWriteItemRequest struct {
	RequestItems                map[string][]WriteRequest `json:"RequestItems"`
	ReturnConsumedCapacity      string                    `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                    `json:"ReturnItemCollectionMetrics,omitempty"`
}

// BatchWriteItemResponse mirrors the JSON response for the BatchWriteItem action.
type BatchWriteItemResponse struct {
	UnprocessedItems      map[string][]WriteRequest          `json:"UnprocessedItems,omitempty"`
	ItemCollectionMetrics map[string][]ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
	ConsumedCapacity      []ConsumedCapacity                 `json:"ConsumedCapacity,omitempty"`
}

// TransactGetItem represents a single get item request within a transaction.
type TransactGetItem struct {
	Get GetItemRequest `json:"Get"`
}

// TransactGetItemsRequest mirrors the JSON request body for the TransactGetItems action.
type TransactGetItemsRequest struct {
	TransactItems          []TransactGetItem `json:"TransactItems"`
	ReturnConsumedCapacity string            `json:"ReturnConsumedCapacity,omitempty"`
}

// ItemResponse represents an individual item response wrapper for TransactGetItems.
type ItemResponse struct {
	Item map[string]AttributeValue `json:"Item"`
}

// TransactGetItemsResponse mirrors the JSON response for the TransactGetItems action.
type TransactGetItemsResponse struct {
	Responses        []ItemResponse     `json:"Responses,omitempty"`
	ConsumedCapacity []ConsumedCapacity `json:"ConsumedCapacity,omitempty"`
}

// ConditionCheck represents a check that must pass for the transaction to succeed.
type ConditionCheck struct {
	TableName                           string                    `json:"TableName"`
	Key                                 map[string]AttributeValue `json:"Key"`
	ConditionExpression                 string                    `json:"ConditionExpression"`
	ExpressionAttributeNames            map[string]string         `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues           map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	ReturnValuesOnConditionCheckFailure string                    `json:"ReturnValuesOnConditionCheckFailure,omitempty"`
}

// TransactWriteItem represents a single write action (ConditionCheck, Put, Delete, Update) within a transaction.
// Only one of the fields should be set.
type TransactWriteItem struct {
	ConditionCheck *ConditionCheck    `json:"ConditionCheck,omitempty"`
	Put            *PutItemRequest    `json:"Put,omitempty"`
	Delete         *DeleteItemRequest `json:"Delete,omitempty"`
	Update         *UpdateItemRequest `json:"Update,omitempty"`
}

// TransactWriteItemsRequest mirrors the JSON request body for the TransactWriteItems action.
type TransactWriteItemsRequest struct {
	TransactItems               []TransactWriteItem `json:"TransactItems"`
	ReturnConsumedCapacity      string              `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string              `json:"ReturnItemCollectionMetrics,omitempty"`
	ClientRequestToken          string              `json:"ClientRequestToken,omitempty"`
}

// TransactWriteItemsResponse mirrors the JSON response for the TransactWriteItems action.
type TransactWriteItemsResponse struct {
	ConsumedCapacity      []ConsumedCapacity                 `json:"ConsumedCapacity,omitempty"`
	ItemCollectionMetrics map[string][]ItemCollectionMetrics `json:"ItemCollectionMetrics,omitempty"`
}

// Global Tables

type CreateGlobalTableRequest struct {
	GlobalTableName  string    `json:"GlobalTableName"`
	ReplicationGroup []Replica `json:"ReplicationGroup"`
}

type Replica struct {
	RegionName string `json:"RegionName"`
}

type CreateGlobalTableResponse struct {
	GlobalTableDescription GlobalTableDescription `json:"GlobalTableDescription"`
}

type GlobalTableDescription struct {
	ReplicationGroup  []ReplicaDescription `json:"ReplicationGroup"`
	GlobalTableArn    string               `json:"GlobalTableArn"`
	CreationDateTime  float64              `json:"CreationDateTime"`
	GlobalTableStatus string               `json:"GlobalTableStatus"`
	GlobalTableName   string               `json:"GlobalTableName"`
}

type UpdateGlobalTableRequest struct {
	GlobalTableName string          `json:"GlobalTableName"`
	ReplicaUpdates  []ReplicaUpdate `json:"ReplicaUpdates"`
}

type UpdateGlobalTableResponse struct {
	GlobalTableDescription GlobalTableDescription `json:"GlobalTableDescription"`
}

type DescribeGlobalTableRequest struct {
	GlobalTableName string `json:"GlobalTableName"`
}

type DescribeGlobalTableResponse struct {
	GlobalTableDescription GlobalTableDescription `json:"GlobalTableDescription"`
}

type ListGlobalTablesRequest struct {
	ExclusiveStartGlobalTableName string `json:"ExclusiveStartGlobalTableName,omitempty"`
	Limit                         int    `json:"Limit,omitempty"`
	RegionName                    string `json:"RegionName,omitempty"`
}

type ListGlobalTablesResponse struct {
	GlobalTables                 []GlobalTable `json:"GlobalTables"`
	LastEvaluatedGlobalTableName string        `json:"LastEvaluatedGlobalTableName,omitempty"`
}

type GlobalTable struct {
	GlobalTableName  string    `json:"GlobalTableName"`
	ReplicationGroup []Replica `json:"ReplicationGroup"`
}

// TTL Operations

type UpdateTimeToLiveRequest struct {
	TableName               string                  `json:"TableName"`
	TimeToLiveSpecification TimeToLiveSpecification `json:"TimeToLiveSpecification"`
}

type UpdateTimeToLiveResponse struct {
	TimeToLiveSpecification TimeToLiveSpecification `json:"TimeToLiveSpecification"`
}

type DescribeTimeToLiveRequest struct {
	TableName string `json:"TableName"`
}

type DescribeTimeToLiveResponse struct {
	TimeToLiveDescription TimeToLiveDescription `json:"TimeToLiveDescription"`
}

// Backup Operations

type CreateBackupRequest struct {
	TableName  string `json:"TableName"`
	BackupName string `json:"BackupName"`
}

type CreateBackupResponse struct {
	BackupDetails BackupDetails `json:"BackupDetails"`
}

type DescribeBackupRequest struct {
	BackupArn string `json:"BackupArn"`
}

type DescribeBackupResponse struct {
	BackupDescription BackupDescription `json:"BackupDescription"`
}

type ListBackupsRequest struct {
	TableName               string  `json:"TableName,omitempty"`
	Limit                   int     `json:"Limit,omitempty"`
	TimeRangeLowerBound     float64 `json:"TimeRangeLowerBound,omitempty"`
	TimeRangeUpperBound     float64 `json:"TimeRangeUpperBound,omitempty"`
	ExclusiveStartBackupArn string  `json:"ExclusiveStartBackupArn,omitempty"`
	BackupType              string  `json:"BackupType,omitempty"` // USER or SYSTEM
}

type ListBackupsResponse struct {
	BackupSummaries        []BackupSummary `json:"BackupSummaries"`
	LastEvaluatedBackupArn string          `json:"LastEvaluatedBackupArn,omitempty"`
}

type DeleteBackupRequest struct {
	BackupArn string `json:"BackupArn"`
}

type DeleteBackupResponse struct {
	BackupDescription BackupDescription `json:"BackupDescription"`
}

type RestoreTableFromBackupRequest struct {
	TargetTableName string `json:"TargetTableName"`
	BackupArn       string `json:"BackupArn"`
}

type RestoreTableFromBackupResponse struct {
	TableDescription TableDescription `json:"TableDescription"`
}

type BackupDescription struct {
	BackupDetails             BackupDetails              `json:"BackupDetails"`
	SourceTableDetails        *SourceTableDetails        `json:"SourceTableDetails,omitempty"`
	SourceTableFeatureDetails *SourceTableFeatureDetails `json:"SourceTableFeatureDetails,omitempty"`
}

type BackupDetails struct {
	BackupArn              string  `json:"BackupArn"`
	BackupName             string  `json:"BackupName"`
	BackupSizeBytes        int64   `json:"BackupSizeBytes,omitempty"`
	BackupStatus           string  `json:"BackupStatus"` // CREATING, DELETED, AVAILABLE
	BackupType             string  `json:"BackupType"`   // USER, SYSTEM
	BackupCreationDateTime float64 `json:"BackupCreationDateTime"`
	BackupExpiryDateTime   float64 `json:"BackupExpiryDateTime,omitempty"`
}

type BackupSummary struct {
	BackupArn              string  `json:"BackupArn"`
	BackupName             string  `json:"BackupName"`
	BackupCreationDateTime float64 `json:"BackupCreationDateTime"`
	BackupExpiryDateTime   float64 `json:"BackupExpiryDateTime,omitempty"`
	BackupStatus           string  `json:"BackupStatus"`
	BackupType             string  `json:"BackupType"`
	BackupSizeBytes        int64   `json:"BackupSizeBytes,omitempty"`
	TableArn               string  `json:"TableArn,omitempty"`
	TableId                string  `json:"TableId,omitempty"`
	TableName              string  `json:"TableName,omitempty"`
}

type SourceTableDetails struct {
	TableName             string                `json:"TableName"`
	TableId               string                `json:"TableId"`
	TableArn              string                `json:"TableArn"`
	TableSizeBytes        int64                 `json:"TableSizeBytes"`
	KeySchema             []KeySchemaElement    `json:"KeySchema"`
	TableCreationDateTime float64               `json:"TableCreationDateTime"`
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
	ItemCount             int64                 `json:"ItemCount"`
}

type SourceTableFeatureDetails struct {
	GlobalSecondaryIndexes []GlobalSecondaryIndex `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []LocalSecondaryIndex  `json:"LocalSecondaryIndexes,omitempty"`
	StreamDescription      *StreamSpecification   `json:"StreamDescription,omitempty"`
	TimeToLiveDescription  *TimeToLiveDescription `json:"TimeToLiveDescription,omitempty"`
}
