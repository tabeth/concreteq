package main_test

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Streams(t *testing.T) {
	if os.Getenv("USE_EXISTING_FDB") != "true" && os.Getenv("CI") != "true" {
		t.Skip("Skipping integration test; requires running FDB/Server or USE_EXISTING_FDB=true")
	}
	// Assuming server is running on localhost:8080 (handled by TestMain in main_test.go if setup correctly)
	// We use the same TestMain setup as ttl_integration_test.go

	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("local"),
		Endpoint:    aws.String("http://localhost:8080"),
		Credentials: credentials.NewStaticCredentials("dummy", "dummy", ""),
	}))
	svc := dynamodb.New(sess)
	tableName := "StreamsIntegrationTest"

	// Cleanup
	_, _ = svc.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	// Wait a bit for delete
	time.Sleep(1 * time.Second)

	// 1. Create Table with Streams
	_, err := svc.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: aws.String("S")},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: aws.String("HASH")},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String("NEW_AND_OLD_IMAGES"),
		},
	})
	require.NoError(t, err)

	// Wait for Active (instant in ConcreteDB, but good practice)
	desc, err := svc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", *desc.Table.TableStatus)
	require.NotNil(t, desc.Table.LatestStreamArn)
	streamArn := *desc.Table.LatestStreamArn
	t.Logf("Stream ARN: %s", streamArn)

	// 2. Put Item (INSERT)
	_, err = svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]*dynamodb.AttributeValue{
			"PK":  {S: aws.String("item1")},
			"Val": {S: aws.String("value1")},
		},
	})
	require.NoError(t, err)

	// 3. Update Item (MODIFY)
	_, err = svc.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String("item1")},
		},
		UpdateExpression: aws.String("SET Val = :v"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {S: aws.String("value2")},
		},
	})
	require.NoError(t, err)

	// 4. Delete Item (REMOVE)
	_, err = svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"PK": {S: aws.String("item1")},
		},
	})
	require.NoError(t, err)

	// 5. Describe Stream to get Shard ID
	// Note: DynamoDB Streams API is separate client in real AWS SDK
	streamsSvc := dynamodbstreams.New(sess)

	// List Streams to verify it's there
	listResp, err := streamsSvc.ListStreams(&dynamodbstreams.ListStreamsInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, listResp.Streams)
	assert.Equal(t, streamArn, *listResp.Streams[0].StreamArn)

	// Describe Stream
	streamDesc, err := streamsSvc.DescribeStream(&dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(streamArn),
	})
	require.NoError(t, err)
	require.NotEmpty(t, streamDesc.StreamDescription.Shards)
	shardId := *streamDesc.StreamDescription.Shards[0].ShardId

	// 6. Get Shard Iterator (TRIM_HORIZON to see all)
	iterResp, err := streamsSvc.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamArn),
		ShardId:           aws.String(shardId),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
	})
	require.NoError(t, err)
	iterator := *iterResp.ShardIterator

	// 7. Get Records
	recResp, err := streamsSvc.GetRecords(&dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(iterator),
	})
	require.NoError(t, err)

	// We expect 3 records: INSERT, MODIFY, REMOVE
	require.Len(t, recResp.Records, 3)

	assert.Equal(t, "INSERT", *recResp.Records[0].EventName)
	assert.Equal(t, "MODIFY", *recResp.Records[1].EventName)
	assert.Equal(t, "REMOVE", *recResp.Records[2].EventName)

	// Check content of INSERT
	assert.Equal(t, "item1", *recResp.Records[0].Dynamodb.NewImage["PK"].S)
}
