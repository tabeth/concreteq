package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_Tagging_Comprehensive(t *testing.T) {
	tableName := fmt.Sprintf("test-tags-%d", time.Now().UnixNano())
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	})

	fakeARN := fmt.Sprintf("arn:aws:dynamodb:us-east-1:123456789012:table/%s", tableName)

	// 2. TagResouce
	tags := []models.Tag{
		{Key: "Environment", Value: "Production"},
		{Key: "Owner", Value: "TeamA"},
	}
	err := store.TagResource(ctx, fakeARN, tags)
	assert.NoError(t, err)

	// 3. ListTagsOfResource
	listedTags, _, err := store.ListTagsOfResource(ctx, fakeARN, "")
	assert.NoError(t, err)
	assert.Len(t, listedTags, 2)
	assert.Contains(t, listedTags, tags[0])
	assert.Contains(t, listedTags, tags[1])

	// 4. UntagResource
	err = store.UntagResource(ctx, fakeARN, []string{"Environment"})
	assert.NoError(t, err)

	// 5. Verify Removal
	listedTagsAfter, _, err := store.ListTagsOfResource(ctx, fakeARN, "")
	assert.NoError(t, err)
	assert.Len(t, listedTagsAfter, 1)
	assert.Equal(t, "Owner", listedTagsAfter[0].Key)

	// 6. Error: Non-existent resource
	missingARN := "arn:aws:dynamodb:us-east-1:123456789012:table/MissingTable"
	err = store.TagResource(ctx, missingARN, tags)
	assert.Error(t, err)
	assert.Equal(t, ErrTableNotFound, err)

	_, _, err = store.ListTagsOfResource(ctx, missingARN, "")
	assert.Error(t, err)
	assert.Equal(t, ErrTableNotFound, err)

	// 7. Error: Invalid ARN
	badARN := "invalid-arn"
	err = store.TagResource(ctx, badARN, tags)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid ARN")
}
