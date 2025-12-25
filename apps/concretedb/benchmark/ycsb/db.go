package main

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/service"
	"github.com/tabeth/concretedb/store"
)

type concreteDB struct {
	svc service.TableServicer
}

func (db *concreteDB) Close() error {
	return nil
}

func (db *concreteDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *concreteDB) CleanupThread(ctx context.Context) {
}

func (db *concreteDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	req := &models.GetItemRequest{
		TableName: table,
		Key: map[string]models.AttributeValue{
			"ycsb_key": {S: &key},
		},
	}
	// TODO: Handle fields projection if needed, checking existing impl ignores it effectively or fetches all
	// If fields are specified, we could set ProjectionExpression, but populating ExpressionAttributeNames is annoying.
	// We'll fetch all.

	resp, err := db.svc.GetItem(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Item == nil {
		return nil, nil // Not found
	}

	result := make(map[string][]byte)
	for k, v := range resp.Item {
		if v.S != nil {
			result[k] = []byte(*v.S)
		} else if v.B != nil {
			// If we used B, we would need to decode base64.
			// But we assume S for now as per plan.
			result[k] = []byte(*v.B)
		}
	}
	return result, nil
}

func (db *concreteDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	limit := int32(count)
	req := &models.ScanRequest{
		TableName: table,
		Limit:     limit,
		ExclusiveStartKey: map[string]models.AttributeValue{
			"ycsb_key": {S: &startKey},
		},
	}

	resp, err := db.svc.Scan(ctx, req)
	if err != nil {
		return nil, err
	}

	results := make([]map[string][]byte, 0, len(resp.Items))
	for _, item := range resp.Items {
		row := make(map[string][]byte)
		for k, v := range item {
			if v.S != nil {
				row[k] = []byte(*v.S)
			}
		}
		results = append(results, row)
	}
	return results, nil
}

func (db *concreteDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Construct UpdateExpression
	// SET #k1 = :v1, #k2 = :v2
	exprParts := ""
	exprAttrNames := make(map[string]string)
	exprAttrValues := make(map[string]models.AttributeValue)

	i := 0
	for k, v := range values {
		accKey := fmt.Sprintf("#k%d", i)
		valKey := fmt.Sprintf(":v%d", i)
		if i > 0 {
			exprParts += ", "
		}
		exprParts += fmt.Sprintf("%s = %s", accKey, valKey)

		exprAttrNames[accKey] = k
		valStr := string(v)
		exprAttrValues[valKey] = models.AttributeValue{S: &valStr}
		i++
	}

	req := &models.UpdateItemRequest{
		TableName: table,
		Key: map[string]models.AttributeValue{
			"ycsb_key": {S: &key},
		},
		UpdateExpression:          "SET " + exprParts,
		ExpressionAttributeNames:  exprAttrNames,
		ExpressionAttributeValues: exprAttrValues,
	}

	_, err := db.svc.UpdateItem(ctx, req)
	return err
}

func (db *concreteDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	item := make(map[string]models.AttributeValue)
	item["ycsb_key"] = models.AttributeValue{S: &key}

	for k, v := range values {
		valStr := string(v)
		item[k] = models.AttributeValue{S: &valStr}
	}

	req := &models.PutItemRequest{
		TableName: table,
		Item:      item,
	}

	_, err := db.svc.PutItem(ctx, req)
	return err
}

func (db *concreteDB) Delete(ctx context.Context, table string, key string) error {
	req := &models.DeleteItemRequest{
		TableName: table,
		Key: map[string]models.AttributeValue{
			"ycsb_key": {S: &key},
		},
	}
	_, err := db.svc.DeleteItem(ctx, req)
	return err
}

type concreteDBCreator struct{}

func (c concreteDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	// Initialize FDB
	fdb.MustAPIVersion(710)
	// We need to wait for connection or just OpenDefault?
	// OpenDefault blocks or returns error? It usually returns DB structure immediately but connection might be established lazily.
	// But concretedb main has a goroutine to wait.

	dbObj, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	fdbStore := store.NewFoundationDBStore(dbObj)
	tableService := service.NewTableService(fdbStore)

	// Ensure table exists
	ctx := context.Background()
	_, err = tableService.GetTable(ctx, "usertable")
	if err != nil {
		// Possibly TableNotFound error or other
		// concretedb/models doesn't export ErrTableNotFound easily reachable?
		// check error string or just try creating
	}

	// Try creating it. If exists, it might fail or we ignore.
	// concretedb CreateTable returns error if exists.
	// We want to swallow "TableExists".

	table := &models.Table{
		TableName: "usertable",
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "ycsb_key", KeyType: "HASH"},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "ycsb_key", AttributeType: "S"},
		},
		Status: models.StatusActive,
	}

	_, err = tableService.CreateTable(ctx, table)
	if err != nil {
		// If error is "Table already exists", we are fine.
		// Since we can't easily check error type without importing internal or dependency,
		// we will assume if GetTable failed earlier maybe it wasn't there.
		// Actually, let's just log and proceed.
		// fmt.Printf("CreateTable: %v\n", err)
	}

	return &concreteDB{svc: tableService}, nil
}

func init() {
	ycsb.RegisterDBCreator("concretedb", concreteDBCreator{})
}
