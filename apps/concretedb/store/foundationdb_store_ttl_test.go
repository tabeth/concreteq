package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_TimeToLive(t *testing.T) {
	ctx := context.Background()
	tableName := "test-ttl-table"
	store := setupTestStore(t, tableName)

	// 1. Create Table
	err := store.CreateTable(ctx, &models.Table{
		TableName: tableName,
	})
	assert.NoError(t, err)

	// 2. Describe TTL (Default: DISABLED)
	desc, err := store.DescribeTimeToLive(ctx, tableName)
	assert.NoError(t, err)
	assert.Equal(t, "DISABLED", desc.TimeToLiveStatus)

	// 3. Update TTL (ENABLE)
	spec := &models.TimeToLiveSpecification{
		Enabled:       true,
		AttributeName: "ttl_field",
	}
	res, err := store.UpdateTimeToLive(ctx, &models.UpdateTimeToLiveRequest{
		TableName:               tableName,
		TimeToLiveSpecification: *spec,
	})
	assert.NoError(t, err)
	assert.Equal(t, spec, res)

	// 4. Verify TTL Status
	desc2, err := store.DescribeTimeToLive(ctx, tableName)
	assert.NoError(t, err)
	assert.Equal(t, "ENABLED", desc2.TimeToLiveStatus)
	assert.Equal(t, "ttl_field", desc2.AttributeName)

	// 5. Update TTL (DISABLE)
	specDisable := &models.TimeToLiveSpecification{
		Enabled: false,
	}
	_, err = store.UpdateTimeToLive(ctx, &models.UpdateTimeToLiveRequest{
		TableName:               tableName,
		TimeToLiveSpecification: *specDisable,
	})
	assert.NoError(t, err)

	// 6. Verify TTL Status
	desc3, err := store.DescribeTimeToLive(ctx, tableName)
	assert.NoError(t, err)
	assert.Equal(t, "DISABLED", desc3.TimeToLiveStatus)

	// 7. Error: Table Not Found
	_, err = store.DescribeTimeToLive(ctx, "non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrTableNotFound, err)

	_, err = store.UpdateTimeToLive(ctx, &models.UpdateTimeToLiveRequest{
		TableName:               "non-existent",
		TimeToLiveSpecification: *spec,
	})
	assert.Error(t, err)
	assert.Equal(t, ErrTableNotFound, err)
}
