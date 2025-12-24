package models

// StreamSpecification represents the stream configuration for a table.
type StreamSpecification struct {
	StreamEnabled  bool   `json:"StreamEnabled"`
	StreamViewType string `json:"StreamViewType,omitempty"` // KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES
}

// StreamDescription represents the details of a stream.
type StreamDescription struct {
	CreationRequestDateTime float64            `json:"CreationRequestDateTime"`
	KeySchema               []KeySchemaElement `json:"KeySchema"`
	Shards                  []Shard            `json:"Shards"`
	StreamArn               string             `json:"StreamArn"`
	StreamLabel             string             `json:"StreamLabel"`
	StreamStatus            string             `json:"StreamStatus"` // ENABLING, ENABLED, DISABLING, DISABLED
	StreamViewType          string             `json:"StreamViewType"`
	TableName               string             `json:"TableName"`
}

// Shard represents a shard within a stream.
type Shard struct {
	ShardId             string               `json:"ShardId"`
	SequenceNumberRange *SequenceNumberRange `json:"SequenceNumberRange"`
	ParentShardId       string               `json:"ParentShardId,omitempty"`
}

// SequenceNumberRange represents the range of sequence numbers in a shard.
type SequenceNumberRange struct {
	StartingSequenceNumber string `json:"StartingSequenceNumber"`
	EndingSequenceNumber   string `json:"EndingSequenceNumber,omitempty"`
}

// StreamRecord represents a single modification record in a stream.
type StreamRecord struct {
	ApproximateCreationDateTime float64                   `json:"ApproximateCreationDateTime"`
	Keys                        map[string]AttributeValue `json:"Keys"`
	NewImage                    map[string]AttributeValue `json:"NewImage,omitempty"`
	OldImage                    map[string]AttributeValue `json:"OldImage,omitempty"`
	SequenceNumber              string                    `json:"SequenceNumber"`
	SizeBytes                   int64                     `json:"SizeBytes"`
	StreamViewType              string                    `json:"StreamViewType"`
}

// Record represents a complete record in the GetRecords response.
type Record struct {
	AwsRegion    string        `json:"awsRegion"`
	Dynamodb     StreamRecord  `json:"dynamodb"`
	EventID      string        `json:"eventID"`
	EventName    string        `json:"eventName"` // INSERT, MODIFY, REMOVE
	EventSource  string        `json:"eventSource"`
	EventVersion string        `json:"eventVersion"`
	UserIdentity *UserIdentity `json:"userIdentity,omitempty"`
}

type UserIdentity struct {
	PrincipalId string `json:"PrincipalId"`
	Type        string `json:"Type"`
}

// ListStreamsRequest mirrors the JSON request body for the ListStreams action.
type ListStreamsRequest struct {
	ExclusiveStartStreamArn string `json:"ExclusiveStartStreamArn,omitempty"`
	Limit                   int32  `json:"Limit,omitempty"`
	TableName               string `json:"TableName,omitempty"`
}

// ListStreamsResponse mirrors the JSON response for the ListStreams action.
type ListStreamsResponse struct {
	Streams                []StreamSummary `json:"Streams"`
	LastEvaluatedStreamArn string          `json:"LastEvaluatedStreamArn,omitempty"`
}

type StreamSummary struct {
	StreamArn   string `json:"StreamArn"`
	StreamLabel string `json:"StreamLabel"`
	TableName   string `json:"TableName"`
}

// DescribeStreamRequest mirrors the JSON request body for the DescribeStream action.
type DescribeStreamRequest struct {
	StreamArn             string `json:"StreamArn"`
	Limit                 int32  `json:"Limit,omitempty"`
	ExclusiveStartShardId string `json:"ExclusiveStartShardId,omitempty"`
}

// DescribeStreamResponse mirrors the JSON response for the DescribeStream action.
type DescribeStreamResponse struct {
	StreamDescription StreamDescription `json:"StreamDescription"`
}

// GetShardIteratorRequest mirrors the JSON request body for the GetShardIterator action.
type GetShardIteratorRequest struct {
	StreamArn         string `json:"StreamArn"`
	ShardId           string `json:"ShardId"`
	ShardIteratorType string `json:"ShardIteratorType"` // TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER
	SequenceNumber    string `json:"SequenceNumber,omitempty"`
}

// GetShardIteratorResponse mirrors the JSON response for the GetShardIterator action.
type GetShardIteratorResponse struct {
	ShardIterator string `json:"ShardIterator"`
}

// GetRecordsRequest mirrors the JSON request body for the GetRecords action.
type GetRecordsRequest struct {
	ShardIterator string `json:"ShardIterator"`
	Limit         int32  `json:"Limit,omitempty"`
}

// GetRecordsResponse mirrors the JSON response for the GetRecords action.
type GetRecordsResponse struct {
	Records           []Record `json:"Records"`
	NextShardIterator string   `json:"NextShardIterator,omitempty"`
}
