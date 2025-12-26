package ttl

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/concretedb/service"
)

type TTLWorker struct {
	service  service.TableServicer
	interval time.Duration
	stopCh   chan struct{}
}

func NewTTLWorker(service service.TableServicer, interval time.Duration) *TTLWorker {
	return &TTLWorker{
		service:  service,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

func (w *TTLWorker) Start() {
	ticker := time.NewTicker(w.interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				w.scanAndCleanup()
			case <-w.stopCh:
				ticker.Stop()
				return
			}
		}
	}()
}

func (w *TTLWorker) Stop() {
	close(w.stopCh)
}

func (w *TTLWorker) scanAndCleanup() {
	ctx := context.Background()
	// List Tables
	var exclusiveStartTableName string
	for {
		tableNames, lastEval, err := w.service.ListTables(ctx, 100, exclusiveStartTableName)
		if err != nil {
			log.Printf("Error listing tables for TTL cleanup: %v", err)
			return
		}

		for _, tableName := range tableNames {
			w.processTable(ctx, tableName)
		}

		if lastEval == "" {
			break
		}
		exclusiveStartTableName = lastEval
	}
}

func (w *TTLWorker) processTable(ctx context.Context, tableName string) {
	// Check TTL status
	desc, err := w.service.DescribeTimeToLive(ctx, &models.DescribeTimeToLiveRequest{TableName: tableName})
	if err != nil {
		log.Printf("Error describing TTL for table %s: %v", tableName, err)
		return
	}

	if desc.TimeToLiveDescription.TimeToLiveStatus != "ENABLED" {
		return
	}

	ttlAttr := desc.TimeToLiveDescription.AttributeName
	table, err := w.service.GetTable(ctx, tableName)
	if err != nil {
		log.Printf("Error getting table definition for %s: %v", tableName, err)
		return
	}

	// Scan items with filter
	now := time.Now().Unix()
	nowStr := strconv.FormatInt(now, 10)

	var exclusiveStartKey map[string]models.AttributeValue

	for {
		// FilterExpression: #t < :now
		scanInput := &models.ScanRequest{
			TableName:        tableName,
			FilterExpression: "#t < :now",
			ExpressionAttributeNames: map[string]string{
				"#t": ttlAttr,
			},
			ExpressionAttributeValues: map[string]models.AttributeValue{
				":now": {N: &nowStr},
			},
			Limit:             100,
			ExclusiveStartKey: exclusiveStartKey,
		}

		scanResp, err := w.service.Scan(ctx, scanInput)
		if err != nil {
			log.Printf("Error scanning table %s for TTL: %v", tableName, err)
			return
		}

		for _, item := range scanResp.Items {
			if isExpired(item, ttlAttr, now) {
				key := extractKey(item, table.KeySchema)
				_, err = w.service.DeleteItem(ctx, &models.DeleteItemRequest{
					TableName: tableName,
					Key:       key,
				})
				if err != nil {
					log.Printf("Failed to delete expired item in table %s: %v", tableName, err)
				}
			}
		}

		if len(scanResp.LastEvaluatedKey) == 0 {
			break
		}
		exclusiveStartKey = scanResp.LastEvaluatedKey
	}
}

func isExpired(item map[string]models.AttributeValue, ttlAttr string, now int64) bool {
	val, ok := item[ttlAttr]
	if !ok || val.N == nil {
		return false
	}

	ts, err := strconv.ParseInt(*val.N, 10, 64)
	if err != nil {
		return false
	}

	return ts < now
}

func extractKey(item map[string]models.AttributeValue, keySchema []models.KeySchemaElement) map[string]models.AttributeValue {
	key := make(map[string]models.AttributeValue)
	for _, ke := range keySchema {
		if v, ok := item[ke.AttributeName]; ok {
			key[ke.AttributeName] = v
		}
	}
	return key
}
