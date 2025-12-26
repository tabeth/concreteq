package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_UpdateItem_Advanced(t *testing.T) {
	tableName := "test-update-advanced"
	store := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table
	store.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
	})

	id := "item1"
	key := map[string]models.AttributeValue{"id": {S: &id}}

	// 2. Arithmetic (SET a = a + :v)
	// Initial put
	store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"id":    {S: &id},
		"count": {N: strPtr("10")},
	}, "", nil, nil, "")

	// Increment
	_, err := store.UpdateItem(ctx, tableName, key, "SET count = count + :v", "", nil, map[string]models.AttributeValue{
		":v": {N: strPtr("5")},
	}, "")
	assert.NoError(t, err)

	// Verify
	item, _ := store.GetItem(ctx, tableName, key, "", nil, true)
	assert.Equal(t, "15", *item["count"].N)

	// Decrement
	_, err = store.UpdateItem(ctx, tableName, key, "SET count = count - :v", "", nil, map[string]models.AttributeValue{
		":v": {N: strPtr("3")},
	}, "")
	assert.NoError(t, err)
	item, _ = store.GetItem(ctx, tableName, key, "", nil, true)
	assert.Equal(t, "12", *item["count"].N)

	// 3. list_append
	store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"id":   {S: &id},
		"list": {L: []models.AttributeValue{{S: strPtr("a")}}},
	}, "", nil, nil, "")

	_, err = store.UpdateItem(ctx, tableName, key, "SET list = list_append(list, :vals)", "", nil, map[string]models.AttributeValue{
		":vals": {L: []models.AttributeValue{{S: strPtr("b")}}},
	}, "")
	assert.NoError(t, err)
	item, _ = store.GetItem(ctx, tableName, key, "", nil, true)
	assert.Len(t, item["list"].L, 2)
	assert.Equal(t, "b", *item["list"].L[1].S)

	// 4. if_not_exists
	// Update non-existing attribute
	_, err = store.UpdateItem(ctx, tableName, key, "SET newAttr = if_not_exists(newAttr, :def)", "", nil, map[string]models.AttributeValue{
		":def": {S: strPtr("default")},
	}, "")
	assert.NoError(t, err)
	item, _ = store.GetItem(ctx, tableName, key, "", nil, true)
	assert.Equal(t, "default", *item["newAttr"].S)

	// Update existing attribute (should not change)
	_, err = store.UpdateItem(ctx, tableName, key, "SET newAttr = if_not_exists(newAttr, :new)", "", nil, map[string]models.AttributeValue{
		":new": {S: strPtr("changed")},
	}, "")
	assert.NoError(t, err)
	item, _ = store.GetItem(ctx, tableName, key, "", nil, true)
	assert.Equal(t, "default", *item["newAttr"].S)

	// 5. REMOVE
	_, err = store.UpdateItem(ctx, tableName, key, "REMOVE count", "", nil, nil, "")
	assert.NoError(t, err)
	item, _ = store.GetItem(ctx, tableName, key, "", nil, true)
	_, ok := item["count"]
	assert.False(t, ok)

	// 6. ADD (Set)
	store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"id":   {S: &id},
		"tags": {SS: []string{"red"}},
	}, "", nil, nil, "")

	_, err = store.UpdateItem(ctx, tableName, key, "ADD tags :t", "", nil, map[string]models.AttributeValue{
		":t": {SS: []string{"blue", "red"}},
	}, "")
	assert.NoError(t, err)
	item, _ = store.GetItem(ctx, tableName, key, "", nil, true)
	assert.Len(t, item["tags"].SS, 2) // red, blue

	// 7. DELETE (Set)
	_, err = store.UpdateItem(ctx, tableName, key, "DELETE tags :t", "", nil, map[string]models.AttributeValue{
		":t": {SS: []string{"red"}},
	}, "")
	assert.NoError(t, err)
	item, _ = store.GetItem(ctx, tableName, key, "", nil, true)
	assert.Len(t, item["tags"].SS, 1)
	assert.Equal(t, "blue", item["tags"].SS[0])

	// 8. Nested SET (Map)
	store.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"id": {S: &id},
		"info": {M: map[string]models.AttributeValue{
			"address": {M: map[string]models.AttributeValue{
				"city": {S: strPtr("OldCity")},
			}},
		}},
	}, "", nil, nil, "")

	_, err = store.UpdateItem(ctx, tableName, key, "SET info.address.city = :c", "", nil, map[string]models.AttributeValue{
		":c": {S: strPtr("NewCity")},
	}, "")
	assert.NoError(t, err)
	item, _ = store.GetItem(ctx, tableName, key, "", nil, true)
	assert.Equal(t, "NewCity", *item["info"].M["address"].M["city"].S)
}
