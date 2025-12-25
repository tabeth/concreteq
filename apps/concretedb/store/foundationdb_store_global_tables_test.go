package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestFoundationDBStore_GlobalTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tableName := "GlobalTable_" + time.Now().Format("20060102150405")
	s := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Global Table
	createReq := &models.CreateGlobalTableRequest{
		GlobalTableName: tableName,
		ReplicationGroup: []models.Replica{
			{RegionName: "us-east-1"},
			{RegionName: "us-west-2"},
		},
	}
	desc, err := s.CreateGlobalTable(ctx, createReq)
	assert.NoError(t, err)
	assert.Equal(t, tableName, desc.GlobalTableName)
	assert.Equal(t, "CREATING", desc.GlobalTableStatus)
	assert.Len(t, desc.ReplicationGroup, 2)

	// 2. Describe Global Table
	desc2, err := s.DescribeGlobalTable(ctx, tableName)
	assert.NoError(t, err)
	assert.Equal(t, tableName, desc2.GlobalTableName)
	assert.Len(t, desc2.ReplicationGroup, 2)

	// 3. List Global Tables
	listReq := &models.ListGlobalTablesRequest{Limit: 100}
	listResp, _, err := s.ListGlobalTables(ctx, listReq.Limit, listReq.ExclusiveStartGlobalTableName)
	assert.NoError(t, err)
	found := false
	for _, gt := range listResp {
		if gt.GlobalTableName == tableName {
			found = true
			break
		}
	}
	assert.True(t, found, "Global table should be listed")

	// 4. Update Global Table (Add Replica)
	updateReq := &models.UpdateGlobalTableRequest{
		GlobalTableName: tableName,
		ReplicaUpdates: []models.ReplicaUpdate{
			{
				Create: &models.CreateReplicaAction{RegionName: "eu-west-1"},
			},
		},
	}
	desc3, err := s.UpdateGlobalTable(ctx, updateReq)
	assert.NoError(t, err)
	assert.Len(t, desc3.ReplicationGroup, 3) // 2 original + 1 new

	// 5. Delete Replica (via Update)
	deleteReq := &models.UpdateGlobalTableRequest{
		GlobalTableName: tableName,
		ReplicaUpdates: []models.ReplicaUpdate{
			{
				Delete: &models.DeleteReplicaAction{RegionName: "us-west-2"},
			},
		},
	}
	deleteRes, err := s.UpdateGlobalTable(ctx, deleteReq)
	assert.NoError(t, err)
	assert.Len(t, deleteRes.ReplicationGroup, 2)

	// 6. Error Paths
	_, err = s.DescribeGlobalTable(ctx, "non-existent")
	assert.Error(t, err)

	_, err = s.UpdateGlobalTable(ctx, &models.UpdateGlobalTableRequest{
		GlobalTableName: "non-existent",
	})
	assert.Error(t, err)

	// 7. List with ExclusiveStartGlobalTableName
	_, _, err = s.ListGlobalTables(ctx, 1, tableName)
	assert.NoError(t, err)
}

func TestFoundationDBStore_UpdateTable_GSI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	tableName := "UpdateTableGSI_" + time.Now().Format("20060102150405")
	s := setupTestStore(t, tableName)
	ctx := context.Background()

	// 1. Create Table (Simple)
	err := s.CreateTable(ctx, &models.Table{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "gsi_pk", AttributeType: "S"},
		},
	})
	assert.NoError(t, err)

	// 2. Put Data
	s.PutItem(ctx, tableName, map[string]models.AttributeValue{
		"pk":     {S: strPtr("1")},
		"gsi_pk": {S: strPtr("g1")},
	}, "", nil, nil, "")

	// 3. Add GSI via UpdateTable
	updateReq := &models.UpdateTableRequest{
		TableName: tableName,
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "gsi_pk", AttributeType: "S"}, // Needed?
		},
		GlobalSecondaryIndexUpdates: []models.GlobalSecondaryIndexUpdate{
			{
				Create: &models.CreateGlobalSecondaryIndexAction{
					IndexName:  "NewGSI",
					KeySchema:  []models.KeySchemaElement{{AttributeName: "gsi_pk", KeyType: "HASH"}},
					Projection: models.Projection{ProjectionType: "ALL"},
				},
			},
		},
	}
	updated, err := s.UpdateTable(ctx, updateReq)
	assert.NoError(t, err)
	assert.Len(t, updated.GlobalSecondaryIndexes, 1)
	assert.Equal(t, "NewGSI", updated.GlobalSecondaryIndexes[0].IndexName)
	// Status might be CREATING or ACTIVE depending on implementation (sync backfill -> ACTIVE probably)
	assert.Equal(t, "ACTIVE", updated.GlobalSecondaryIndexes[0].IndexStatus)

	// 4. Verify GSI Data (if we had Query on it, or just trust backfill ran without error)
	// For now just success is enough to prove integration.
}
