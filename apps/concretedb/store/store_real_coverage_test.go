package store

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
	sharedfdb "github.com/tabeth/kiroku-core/libs/fdb"
	"github.com/tabeth/kiroku-core/libs/fdb/fdbtest"
)

func TestRealStore_Coverage_Gaps(t *testing.T) {
	fdbtest.SkipIfFDBUnavailable(t)

	fdbDB, err := sharedfdb.OpenDB(710)
	if err != nil {
		t.Fatalf("Failed to open FDB: %v", err)
	}

	store := NewFoundationDBStore(fdbDB)
	ctx := context.Background()

	t.Run("CreateTable_DeleteTable_Cycle", func(t *testing.T) {
		tableName := fmt.Sprintf("RealCovTable_%d", time.Now().UnixNano())
		// 1. Create Table
		req := &models.Table{
			TableName: tableName,
			KeySchema: []models.KeySchemaElement{
				{AttributeName: "pk", KeyType: "HASH"},
			},
			AttributeDefinitions: []models.AttributeDefinition{
				{AttributeName: "pk", AttributeType: "S"},
			},
		}
		err := store.CreateTable(ctx, req)
		assert.NoError(t, err)

		// 2. Create Table (Already Exists)
		err = store.CreateTable(ctx, req)
		assert.Error(t, err)

		// 3. List Tables
		list, _, err := store.ListTables(ctx, 10, "")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(list), 1)

		_, _, err = store.ListTables(ctx, 1, tableName)
		assert.NoError(t, err)

		// 4. Delete Table
		_, err = store.DeleteTable(ctx, tableName)
		assert.NoError(t, err)

		// 5. Delete Table Again (Not Found)
		_, err = store.DeleteTable(ctx, tableName)
		assert.Error(t, err)
		assert.Equal(t, ErrTableNotFound, err)
	})

	t.Run("CRUD_TableNotFound", func(t *testing.T) {
		tableName := "NonExistentTable_" + fmt.Sprint(time.Now().UnixNano())

		_, err := store.GetItem(ctx, tableName, nil, "", nil, false)
		assert.Error(t, err)

		_, err = store.DeleteItem(ctx, tableName, nil, "", nil, nil, "")
		assert.Error(t, err)

		_, err = store.PutItem(ctx, tableName, nil, "", nil, nil, "")
		assert.Error(t, err)

		_, err = store.UpdateItem(ctx, tableName, nil, "", "", nil, nil, "")
		assert.Error(t, err)

		_, _, err = store.Scan(ctx, tableName, "", "", nil, nil, 10, nil, false)
		assert.Error(t, err)

		_, _, err = store.Query(ctx, tableName, "", "", "", "", nil, nil, 10, nil, false)
		assert.Error(t, err)
	})

	t.Run("GlobalTables_Real", func(t *testing.T) {
		gtName := fmt.Sprintf("RealGT_%d", time.Now().UnixNano())

		// 1. Create Global Table
		_, err := store.CreateGlobalTable(ctx, &models.CreateGlobalTableRequest{
			GlobalTableName: gtName,
			ReplicationGroup: []models.Replica{
				{RegionName: "us-east-1"},
			},
		})
		assert.NoError(t, err)

		// 2. Describe Global Table
		resp, err := store.DescribeGlobalTable(ctx, gtName)
		assert.NoError(t, err)
		assert.Equal(t, gtName, resp.GlobalTableName)

		// 3. List Global Tables
		list, _, err := store.ListGlobalTables(ctx, 10, "")
		assert.NoError(t, err)
		found := false
		for _, gt := range list {
			if gt.GlobalTableName == gtName {
				found = true
				break
			}
		}
		assert.True(t, found)

		// List with start key and skip
		list, _, err = store.ListGlobalTables(ctx, 1, gtName)
		assert.NoError(t, err)

		// 4. Update Global Table
		_, err = store.UpdateGlobalTable(ctx, &models.UpdateGlobalTableRequest{
			GlobalTableName: gtName,
			ReplicaUpdates: []models.ReplicaUpdate{
				{Create: &models.CreateReplicaAction{RegionName: "us-west-2"}},
			},
		})
		assert.NoError(t, err)

		// 5. Describe Non-Existent
		_, err = store.DescribeGlobalTable(ctx, "NonExistentGT")
		assert.Error(t, err)

		// 6. Update Non-Existent
		_, err = store.UpdateGlobalTable(ctx, &models.UpdateGlobalTableRequest{GlobalTableName: "NonExistentGT"})
		assert.Error(t, err)
	})

	t.Run("Backup_Lifecycle_Real", func(t *testing.T) {
		tableName2 := fmt.Sprintf("BackupRealTable_%d", time.Now().UnixNano())
		store.CreateTable(ctx, &models.Table{
			TableName: tableName2,
			KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		})

		// 1. Create Backup
		resp, err := store.CreateBackup(ctx, &models.CreateBackupRequest{
			TableName:  tableName2,
			BackupName: "RealBackup",
		})
		assert.NoError(t, err)
		arn := resp.BackupArn

		// 2. List Backups
		backups, _, err := store.ListBackups(ctx, &models.ListBackupsRequest{TableName: tableName2})
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(backups), 1)

		// 3. Describe Backup
		_, err = store.DescribeBackup(ctx, arn)
		assert.NoError(t, err)

		// Sleep to let async backup (status update) finish if any
		time.Sleep(100 * time.Millisecond)

		// 4. Delete Backup
		_, err = store.DeleteBackup(ctx, arn)
		assert.NoError(t, err)

		// 5. Delete Non-Existent
		_, err = store.DeleteBackup(ctx, "non-existent-arn")
		assert.Error(t, err)

		// 6. Describe Non-Existent
		_, err = store.DescribeBackup(ctx, "non-existent-arn")
		assert.Error(t, err)
	})

	t.Run("PITR_Lifecycle_Real", func(t *testing.T) {
		tableName3 := fmt.Sprintf("PITRRealTable_%d", time.Now().UnixNano())
		store.CreateTable(ctx, &models.Table{
			TableName: tableName3,
			KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		})

		// 1. Enable PITR
		_, err := store.UpdateContinuousBackups(ctx, &models.UpdateContinuousBackupsRequest{
			TableName: tableName3,
			PointInTimeRecoverySpecification: models.PointInTimeRecoverySpecification{
				PointInTimeRecoveryEnabled: true,
			},
		})
		assert.NoError(t, err)

		// Sleep 1.1s to ensure earliest time is surpassed
		time.Sleep(1100 * time.Millisecond)

		// 2. Describe PITR
		resp, err := store.DescribeContinuousBackups(ctx, tableName3)
		assert.NoError(t, err)
		assert.Equal(t, "ENABLED", resp.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus)

		// 4. Perform operations after PITR enabled
		pk := "p1"
		item := map[string]models.AttributeValue{"pk": {S: &pk}}
		_, err = store.PutItem(ctx, tableName3, item, "", nil, nil, "")
		assert.NoError(t, err)

		_, err = store.DeleteItem(ctx, tableName3, item, "", nil, nil, "")
		assert.NoError(t, err)

		// 5. Restore to PITR
		targetName := tableName3 + "_restored"
		_, err = store.RestoreTableToPointInTime(ctx, &models.RestoreTableToPointInTimeRequest{
			SourceTableName: tableName3,
			TargetTableName: targetName,
			RestoreDateTime: float64(time.Now().Unix()),
		})
		assert.NoError(t, err)

		// Restore to existing table (Error)
		_, err = store.RestoreTableToPointInTime(ctx, &models.RestoreTableToPointInTimeRequest{
			SourceTableName: tableName3,
			TargetTableName: tableName3,
			RestoreDateTime: float64(time.Now().Unix()),
		})
		assert.Error(t, err)

		// Restore non-existent table (Error)
		_, err = store.RestoreTableToPointInTime(ctx, &models.RestoreTableToPointInTimeRequest{
			SourceTableName: "NonExistentSource",
			TargetTableName: "SomeTarget",
			RestoreDateTime: float64(time.Now().Unix()),
		})
		assert.Error(t, err)

		// 6. UpdateContinuousBackups non-existent (Error)
		_, err = store.UpdateContinuousBackups(ctx, &models.UpdateContinuousBackupsRequest{
			TableName: "NonExistentTable",
		})
		assert.Error(t, err)

		// Sleep to let async restore run
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("TTL_Real", func(t *testing.T) {
		tableName4 := fmt.Sprintf("TTLRealTable_%d", time.Now().UnixNano())
		store.CreateTable(ctx, &models.Table{
			TableName: tableName4,
			KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		})

		// 1. Update TTL
		_, err := store.UpdateTimeToLive(ctx, &models.UpdateTimeToLiveRequest{
			TableName: tableName4,
			TimeToLiveSpecification: models.TimeToLiveSpecification{
				AttributeName: "ttl",
				Enabled:       true,
			},
		})
		assert.NoError(t, err)

		// 2. Describe TTL
		resp, err := store.DescribeTimeToLive(ctx, tableName4)
		assert.NoError(t, err)
		assert.Equal(t, "ENABLED", resp.TimeToLiveStatus)

		// 3. TTL Non-existent (Error)
		_, err = store.UpdateTimeToLive(ctx, &models.UpdateTimeToLiveRequest{TableName: "NonExistent"})
		assert.Error(t, err)

		_, err = store.DescribeTimeToLive(ctx, "NonExistent")
		assert.Error(t, err)
	})

	t.Run("Advanced_Indexes_And_Streams_Real", func(t *testing.T) {
		tableName5 := fmt.Sprintf("IdxRealTable_%d", time.Now().UnixNano())
		err := store.CreateTable(ctx, &models.Table{
			TableName: tableName5,
			KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
			AttributeDefinitions: []models.AttributeDefinition{
				{AttributeName: "pk", AttributeType: "S"},
				{AttributeName: "sk", AttributeType: "S"},
				{AttributeName: "gsi_pk", AttributeType: "S"},
			},
			GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
				{
					IndexName:  "GSI1",
					KeySchema:  []models.KeySchemaElement{{AttributeName: "gsi_pk", KeyType: "HASH"}},
					Projection: models.Projection{ProjectionType: "ALL"},
				},
			},
			LocalSecondaryIndexes: []models.LocalSecondaryIndex{
				{
					IndexName: "LSI1",
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "pk", KeyType: "HASH"},
						{AttributeName: "sk", KeyType: "RANGE"},
					},
					Projection: models.Projection{ProjectionType: "KEYS_ONLY"},
				},
			},
			StreamSpecification: &models.StreamSpecification{
				StreamEnabled:  true,
				StreamViewType: "NEW_AND_OLD_IMAGES",
			},
		})
		assert.NoError(t, err)

		// 1. Put item with missing index keys (Triggers ErrSkipIndex)
		pk := "p1"
		item := map[string]models.AttributeValue{
			"pk": {S: &pk},
		}
		_, err = store.PutItem(ctx, tableName5, item, "", nil, nil, "")
		assert.NoError(t, err)

		// 2. Delete item missing index keys
		_, err = store.DeleteItem(ctx, tableName5, map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, nil, "")
		assert.NoError(t, err)

		// 3. Put item WITH index keys
		sk := "s1"
		gsi_pk := "g1"
		item2 := map[string]models.AttributeValue{
			"pk":     {S: &pk},
			"sk":     {S: &sk},
			"gsi_pk": {S: &gsi_pk},
		}
		_, err = store.PutItem(ctx, tableName5, item2, "", nil, nil, "")
		assert.NoError(t, err)

		// 4. Query on index with condition
		exprValues := map[string]models.AttributeValue{
			":g": {S: &gsi_pk},
		}
		_, _, err = store.Query(ctx, tableName5, "GSI1", "gsi_pk = :g", "", "", nil, exprValues, 10, nil, false)
		assert.NoError(t, err)

		// --- Query Error Paths ---
		_, _, err = store.Query(ctx, tableName5, "NonExistentIndex", "pk = :v", "", "", nil, exprValues, 10, nil, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "index not found")

		// 5. Perform UpdateItem to hit more stream paths
		updateExpr := "SET other = :v"
		exprValuesUpdate := map[string]models.AttributeValue{":v": {S: &pk}}
		_, err = store.UpdateItem(ctx, tableName5, map[string]models.AttributeValue{"pk": {S: &pk}}, updateExpr, "", nil, exprValuesUpdate, "")
		assert.NoError(t, err)

		// 5b. UpdateItem with ALL_OLD, ALL_NEW, UPDATED_OLD, UPDATED_NEW
		_, err = store.UpdateItem(ctx, tableName5, map[string]models.AttributeValue{"pk": {S: &pk}}, "SET more = :v", "", nil, exprValuesUpdate, "ALL_OLD")
		assert.NoError(t, err)
		_, err = store.UpdateItem(ctx, tableName5, map[string]models.AttributeValue{"pk": {S: &pk}}, "SET more = :v", "", nil, exprValuesUpdate, "ALL_NEW")
		assert.NoError(t, err)
		_, err = store.UpdateItem(ctx, tableName5, map[string]models.AttributeValue{"pk": {S: &pk}}, "SET more = :v", "", nil, exprValuesUpdate, "UPDATED_OLD")
		assert.NoError(t, err)
		_, err = store.UpdateItem(ctx, tableName5, map[string]models.AttributeValue{"pk": {S: &pk}}, "SET more = :v", "", nil, exprValuesUpdate, "UPDATED_NEW")
		assert.NoError(t, err)

		// 5c. UpdateItem with condition failure
		condExpr := "attribute_not_exists(pk)"
		_, err = store.UpdateItem(ctx, tableName5, map[string]models.AttributeValue{"pk": {S: &pk}}, updateExpr, condExpr, nil, exprValuesUpdate, "")
		assert.Error(t, err)

		// 6. Delete item with index keys
		_, err = store.DeleteItem(ctx, tableName5, map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, nil, "")
		assert.NoError(t, err)

		// 7. INCLUDE Projection Test
		tableName6 := fmt.Sprintf("IncludeIdxTable_%d", time.Now().UnixNano())
		err = store.CreateTable(ctx, &models.Table{
			TableName: tableName6,
			KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
			GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
				{
					IndexName: "GSI_Include",
					KeySchema: []models.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}},
					Projection: models.Projection{
						ProjectionType:   "INCLUDE",
						NonKeyAttributes: []string{"extra"},
					},
				},
			},
		})
		assert.NoError(t, err)

		gVal := "g1"
		extraVal := "e1"
		otherVal := "o1"
		item6 := map[string]models.AttributeValue{
			"pk":    {S: &pk},
			"g":     {S: &gVal},
			"extra": {S: &extraVal},
			"other": {S: &otherVal},
		}
		_, err = store.PutItem(ctx, tableName6, item6, "", nil, nil, "")
		assert.NoError(t, err)

		// 8. DeleteItem with ALL_OLD
		resp, err := store.DeleteItem(ctx, tableName6, map[string]models.AttributeValue{"pk": {S: &pk}}, "", nil, nil, "ALL_OLD")
		assert.NoError(t, err)
		assert.NotNil(t, resp)

		// 9. UpdateTable Stream Toggle
		_, err = store.UpdateTable(ctx, &models.UpdateTableRequest{
			TableName: tableName6,
			StreamSpecification: &models.StreamSpecification{
				StreamEnabled:  false,
				StreamViewType: "KEYS_ONLY",
			},
		})
		assert.NoError(t, err)

		_, err = store.UpdateTable(ctx, &models.UpdateTableRequest{
			TableName: tableName6,
			StreamSpecification: &models.StreamSpecification{
				StreamEnabled:  true,
				StreamViewType: "KEYS_ONLY",
			},
		})
		assert.NoError(t, err)

		// 10. PutItem with ALL_OLD
		// Ensure it exists first so we get ALL_OLD
		_, err = store.PutItem(ctx, tableName6, item6, "", nil, nil, "")
		assert.NoError(t, err)

		resp, err = store.PutItem(ctx, tableName6, item6, "", nil, nil, "ALL_OLD")
		assert.NoError(t, err)
		assert.NotNil(t, resp)

		// 11. Binary Index Test (toFDBElement B path)
		tableName7 := fmt.Sprintf("BinIdxTable_%d", time.Now().UnixNano())
		bVal := base64.StdEncoding.EncodeToString([]byte("bin-val"))
		err = store.CreateTable(ctx, &models.Table{
			TableName: tableName7,
			KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
			GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
				{
					IndexName:  "GSI_Bin",
					KeySchema:  []models.KeySchemaElement{{AttributeName: "g", KeyType: "HASH"}},
					Projection: models.Projection{ProjectionType: "KEYS_ONLY"},
				},
			},
		})
		assert.NoError(t, err)

		item7 := map[string]models.AttributeValue{
			"pk": {S: &pk},
			"g":  {B: &bVal},
		}
		_, err = store.PutItem(ctx, tableName7, item7, "", nil, nil, "")
		assert.NoError(t, err)
	})

	t.Run("Transactions_And_Batch_Real", func(t *testing.T) {
		t1 := fmt.Sprintf("TransTable1_%d", time.Now().UnixNano())
		t2 := fmt.Sprintf("TransTable2_%d", time.Now().UnixNano())

		for _, tn := range []string{t1, t2} {
			store.CreateTable(ctx, &models.Table{
				TableName: tn,
				KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
			})
		}

		pk := "p1"
		pk1 := "p1"
		v1 := "v1"
		pk2 := "p2"
		v2 := "v2"
		// TransactWrite
		err := store.TransactWriteItems(ctx, []models.TransactWriteItem{
			{Put: &models.PutItemRequest{TableName: t1, Item: map[string]models.AttributeValue{"pk": {S: &pk1}, "v": {S: &v1}}}},
			{Update: &models.UpdateItemRequest{TableName: t2, Key: map[string]models.AttributeValue{"pk": {S: &pk2}}, UpdateExpression: "SET v = :v", ExpressionAttributeValues: map[string]models.AttributeValue{":v": {S: &v2}}}},
		}, "")
		assert.NoError(t, err)

		// 3. TransactWrite with ConditionCheck (Failing)
		err = store.TransactWriteItems(ctx, []models.TransactWriteItem{
			{ConditionCheck: &models.ConditionCheck{
				TableName:           t1,
				Key:                 map[string]models.AttributeValue{"pk": {S: &pk1}},
				ConditionExpression: "v = :wrong",
				ExpressionAttributeValues: map[string]models.AttributeValue{
					":wrong": {S: stringPtr("wrong")},
				},
			}},
		}, "")
		assert.Error(t, err)

		// TransactGet
		items, err := store.TransactGetItems(ctx, []models.TransactGetItem{
			{Get: models.GetItemRequest{TableName: t1, Key: map[string]models.AttributeValue{"pk": {S: &pk1}}}},
			{Get: models.GetItemRequest{TableName: t2, Key: map[string]models.AttributeValue{"pk": {S: &pk2}}}},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(items))

		// BatchWrite (Mixed Put/Delete)
		_, err = store.BatchWriteItem(ctx, map[string][]models.WriteRequest{
			t1: {
				{PutRequest: &models.PutRequest{Item: map[string]models.AttributeValue{"pk": {S: &pk2}}}},
				{DeleteRequest: &models.DeleteRequest{Key: map[string]models.AttributeValue{"pk": {S: &pk}}}},
			},
		})
		assert.NoError(t, err)

		// BatchGet
		batchResp, _, err := store.BatchGetItem(ctx, map[string]models.KeysAndAttributes{
			t1: {Keys: []map[string]models.AttributeValue{{"pk": {S: &pk2}}}},
			t2: {Keys: []map[string]models.AttributeValue{{"pk": {S: &pk2}}}},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(batchResp))

		// --- Error Paths (Non-existent tables) ---

		// BatchGet non-existent
		_, _, err = store.BatchGetItem(ctx, map[string]models.KeysAndAttributes{
			"NonExistent": {Keys: []map[string]models.AttributeValue{{"pk": {S: &pk}}}},
		})
		assert.Error(t, err)

		// BatchWrite non-existent
		_, err = store.BatchWriteItem(ctx, map[string][]models.WriteRequest{
			"NonExistent": {{PutRequest: &models.PutRequest{Item: map[string]models.AttributeValue{"pk": {S: &pk}}}}},
		})
		assert.Error(t, err)

		// TransactGet non-existent
		_, err = store.TransactGetItems(ctx, []models.TransactGetItem{
			{Get: models.GetItemRequest{TableName: "NonExistent", Key: map[string]models.AttributeValue{"pk": {S: &pk}}}},
		})
		assert.Error(t, err)

		// TransactWrite non-existent
		err = store.TransactWriteItems(ctx, []models.TransactWriteItem{
			{Put: &models.PutItemRequest{TableName: "NonExistent", Item: map[string]models.AttributeValue{"pk": {S: &pk}}}},
		}, "")
		assert.Error(t, err)
	})
}
