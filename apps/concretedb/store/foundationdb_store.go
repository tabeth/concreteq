package store

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/tabeth/concretedb/expression"
	"github.com/tabeth/concretedb/models"
	"github.com/tabeth/kiroku-core/libs/fdb/directory"
)

// FoundationDBStore implements the Store interface using FoundationDB.
type FoundationDBStore struct {
	db        fdb.Database
	dir       directory.Directory
	evaluator *expression.Evaluator
}

// NewFoundationDBStore creates a new store connected to FoundationDB.
func NewFoundationDBStore(db fdb.Database) *FoundationDBStore {
	fmt.Println("Creating FDB store.")
	return &FoundationDBStore{
		db:        db,
		dir:       directory.NewDirectoryLayer(subspace.Sub(tuple.Tuple{"concretedb"}), subspace.Sub(tuple.Tuple{"content"}), true),
		evaluator: expression.NewEvaluator(),
	}
}

// ... (retain other methods) ...

// Scan scans the table.
func (s *FoundationDBStore) Scan(ctx context.Context, tableName string, filterExpression string, projectionExpression string, expressionAttributeNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
	// First check table metadata existence (consistency check)
	table, err := s.GetTable(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}
	if table == nil {
		return nil, nil, ErrTableNotFound
	}

	res, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		// Open the directory for this table
		tableDir, err := s.dir.Open(rtr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}

		// Determine Range
		var r fdb.Range
		if len(exclusiveStartKey) > 0 {
			keyTuple, err := s.buildKeyTuple(table, exclusiveStartKey)
			if err != nil {
				return nil, err
			}
			startFDBKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
			// Start reading *after* the exclusive start key
			// Note: We need the end of the data range for this table.
			dataPrefix := tableDir.Pack(tuple.Tuple{"data"})
			pr, _ := fdb.PrefixRange(dataPrefix)

			r = fdb.SelectorRange{
				Begin: fdb.FirstGreaterThan(startFDBKey),
				End:   fdb.FirstGreaterOrEqual(pr.End),
			}
		} else {
			// Scan the entire "data" subspace for the table
			dataPrefix := tableDir.Pack(tuple.Tuple{"data"})
			r, _ = fdb.PrefixRange(dataPrefix)
		}

		// Options
		opts := fdb.RangeOptions{}
		if limit > 0 {
			opts.Limit = int(limit)
		}

		// Perform Range Read
		iter := rtr.GetRange(r, opts).Iterator()

		var items []map[string]models.AttributeValue
		var lastProcessedItem map[string]models.AttributeValue
		var itemsRead int

		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}

			// Deserialize value
			var item map[string]models.AttributeValue
			if len(kv.Value) > 0 {
				err = json.Unmarshal(kv.Value, &item)
				if err != nil {
					return nil, fmt.Errorf("failed to unmarshal item: %v", err)
				}

				lastProcessedItem = item
				itemsRead++

				// Evaluate Filter
				match, err := s.evaluator.EvaluateFilter(item, filterExpression, expressionAttributeNames, expressionAttributeValues)
				if err != nil {
					return nil, err
				}
				if match {
					item = s.evaluator.ProjectItem(item, projectionExpression, expressionAttributeNames)
					items = append(items, item)
				}
			}
		}

		// Determine LastEvaluatedKey
		// If we read 'limit' items, we return LastEvaluatedKey of the last read item.
		// Note: FDB GetRange honors limit. So if iterator exhausted, we likely didn't hit limit unless exact match.
		// Wait, FDB GetRange stops AT limit.
		// So if itemsRead == limit, we have a LEK.
		var lastEvaluatedKey map[string]models.AttributeValue
		if limit > 0 && itemsRead == int(limit) && lastProcessedItem != nil {
			lastEvaluatedKey = make(map[string]models.AttributeValue)
			for _, ks := range table.KeySchema {
				if val, ok := lastProcessedItem[ks.AttributeName]; ok {
					lastEvaluatedKey[ks.AttributeName] = val
				}
			}
		}

		return struct {
			Items   []map[string]models.AttributeValue
			LastKey map[string]models.AttributeValue
		}{items, lastEvaluatedKey}, nil
	})

	if err != nil {
		return nil, nil, err
	}

	result := res.(struct {
		Items   []map[string]models.AttributeValue
		LastKey map[string]models.AttributeValue
	})
	return result.Items, result.LastKey, nil
}

// Query queries the table.
func (s *FoundationDBStore) Query(ctx context.Context, tableName string, indexName string, keyConditionExpression string, filterExpression string, projectionExpression string, expressionAttributeNames map[string]string, expressionAttributeValues map[string]models.AttributeValue, limit int32, exclusiveStartKey map[string]models.AttributeValue, consistentRead bool) ([]map[string]models.AttributeValue, map[string]models.AttributeValue, error) {
	// 1. Get Table Metadata
	table, err := s.GetTable(ctx, tableName)
	if err != nil {
		return nil, nil, err
	}
	if table == nil {
		return nil, nil, ErrTableNotFound
	}

	// 2. Determine Key Schema (Table or Index)
	targetKeySchema := table.KeySchema
	if indexName != "" {
		found := false
		// check GSI
		for _, gsi := range table.GlobalSecondaryIndexes {
			if gsi.IndexName == indexName {
				targetKeySchema = gsi.KeySchema
				found = true
				break
			}
		}
		// check LSI
		if !found {
			for _, lsi := range table.LocalSecondaryIndexes {
				if lsi.IndexName == indexName {
					targetKeySchema = lsi.KeySchema
					found = true
					break
				}
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("index not found: %s", indexName)
		}
	}

	// 3. Parse Key Condition
	var pkHashName, pkRangeName string
	for _, k := range targetKeySchema {
		if k.KeyType == "HASH" {
			pkHashName = k.AttributeName
		} else if k.KeyType == "RANGE" {
			pkRangeName = k.AttributeName
		}
	}
	if pkHashName == "" {
		return nil, nil, fmt.Errorf("invalid schema: no HASH key")
	}

	// Simple Parser Logic (reused)
	rawParts := strings.Split(keyConditionExpression, " AND ")
	var parts []string
	for i := 0; i < len(rawParts); i++ {
		p := rawParts[i]
		if strings.Contains(p, "BETWEEN") {
			if i+1 < len(rawParts) {
				p = p + " AND " + rawParts[i+1]
				i++
			}
		}
		parts = append(parts, p)
	}

	var pkValue *models.AttributeValue
	var skOp string
	var skVals []*models.AttributeValue

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.Contains(part, pkHashName) && strings.Contains(part, "=") {
			subParts := strings.SplitN(part, "=", 2)
			if len(subParts) == 2 {
				valPlaceholder := strings.TrimSpace(subParts[1])
				if v, ok := expressionAttributeValues[valPlaceholder]; ok {
					val := v
					pkValue = &val
				}
			}
		} else if pkRangeName != "" && strings.Contains(part, pkRangeName) {
			// SK Conditions
			if strings.HasPrefix(part, "begins_with") {
				skOp = "begins_with"
				start := strings.Index(part, ",")
				end := strings.Index(part, ")")
				if start > 0 && end > start {
					valPlaceholder := strings.TrimSpace(part[start+1 : end])
					if v, ok := expressionAttributeValues[valPlaceholder]; ok {
						val := v
						skVals = append(skVals, &val)
					}
				}
			} else {
				ops := []string{"<=", ">=", "<", ">", "=", "BETWEEN", "IN"}
				foundOp := ""
				for _, op := range ops {
					if strings.Contains(part, op) {
						foundOp = op
						break
					}
				}
				// Robust search
				if strings.Contains(part, "<=") {
					foundOp = "<="
				} else if strings.Contains(part, ">=") {
					foundOp = ">="
				} else if strings.Contains(part, "<") {
					foundOp = "<"
				} else if strings.Contains(part, ">") {
					foundOp = ">"
				} else if strings.Contains(part, "=") {
					foundOp = "="
				} else if strings.Contains(part, "BETWEEN") {
					foundOp = "BETWEEN"
				}

				if foundOp != "" {
					skOp = foundOp
					sub := strings.SplitN(part, foundOp, 2)
					if len(sub) == 2 {
						rhs := strings.TrimSpace(sub[1])
						if foundOp == "BETWEEN" {
							betweens := strings.Split(rhs, " AND ")
							if len(betweens) == 2 {
								v1 := strings.TrimSpace(betweens[0])
								v2 := strings.TrimSpace(betweens[1])
								if val1, ok := expressionAttributeValues[v1]; ok {
									val := val1
									skVals = append(skVals, &val)
								}
								if val2, ok := expressionAttributeValues[v2]; ok {
									val := val2
									skVals = append(skVals, &val)
								}
							}
						} else {
							if v, ok := expressionAttributeValues[rhs]; ok {
								val := v
								skVals = append(skVals, &val)
							}
						}
					}
				}
			}
		}
	}

	if pkValue == nil {
		if len(expressionAttributeValues) == 1 {
			for _, v := range expressionAttributeValues {
				val := v
				pkValue = &val
				break
			}
		}
	}
	if pkValue == nil {
		return nil, nil, fmt.Errorf("could not resolve Partition Key value")
	}

	res, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		var subspace subspace.Subspace
		var err error
		if indexName != "" {
			// Index Query: Key is (indexPK, ..., indexSK, ...)
			subspace, err = s.dir.Open(rtr, []string{"tables", tableName, "index", indexName}, nil)
		} else {
			// Base Table Query: Key is ("data", pk, sk)
			// We access the table directory directly, not a subspace via Sub("data"), to align with PutItem logic
			var td directory.DirectorySubspace
			td, err = s.dir.Open(rtr, []string{"tables", tableName}, nil)
			if err != nil {
				return nil, err
			}
			// Simulate subspace behavior by manually pre-pending "data" to query logic below
			// The original code used td.Sub("data"). Since Sub returns a subspace, and Pack appends, it should have worked.
			// However, to be absolutely safe and consistent with PutItem which uses tableDir.Pack(append(tuple.Tuple{"data"}, ...)),
			// we will use the tableDir directly if possible, or verify subspace.
			// Actually, let's stick to the current structure but verify the tuple composition.
			subspace = td
		}
		if err != nil {
			return nil, err
		}

		pkTupleElem, err := toTupleElement(*pkValue)
		if err != nil {
			return nil, err
		}
		var prefixKey fdb.Key
		if indexName != "" {
			prefixKey = subspace.Pack(tuple.Tuple{pkTupleElem})
		} else {
			prefixKey = subspace.Pack(tuple.Tuple{"data", pkTupleElem})
		}
		pr, err := fdb.PrefixRange(prefixKey)
		if err != nil {
			return nil, err
		}

		r := fdb.SelectorRange{
			Begin: fdb.FirstGreaterOrEqual(pr.Begin),
			End:   fdb.FirstGreaterOrEqual(pr.End),
		}

		if skOp != "" && len(skVals) > 0 {
			getSKKey := func(val *models.AttributeValue) (fdb.Key, error) {
				tElem, err := toTupleElement(*val)
				if err != nil {
					return nil, err
				}
				if indexName != "" {
					return subspace.Pack(tuple.Tuple{pkTupleElem, tElem}), nil
				}
				return subspace.Pack(tuple.Tuple{"data", pkTupleElem, tElem}), nil
			}
			switch skOp {
			case "=":
				k, _ := getSKKey(skVals[0])
				pr, _ := fdb.PrefixRange(k)
				r.Begin = fdb.FirstGreaterOrEqual(pr.Begin)
				r.End = fdb.FirstGreaterOrEqual(pr.End)
			case "<":
				k, _ := getSKKey(skVals[0])
				// All items with index_sk < target. Since keys have suffixes, pack(target) is before any k+suffix.
				r.End = fdb.FirstGreaterOrEqual(k)
			case "<=":
				k, _ := getSKKey(skVals[0])
				pr, _ := fdb.PrefixRange(k)
				r.End = fdb.FirstGreaterOrEqual(pr.End)
			case ">":
				k, _ := getSKKey(skVals[0])
				pr, _ := fdb.PrefixRange(k)
				// All items with index_sk > target. Start after all items with target prefix.
				r.Begin = fdb.FirstGreaterOrEqual(pr.End)
			case ">=":
				k, _ := getSKKey(skVals[0])
				r.Begin = fdb.FirstGreaterOrEqual(k)
			case "BETWEEN":
				if len(skVals) >= 2 {
					k1, _ := getSKKey(skVals[0])
					k2, _ := getSKKey(skVals[1])
					pr2, _ := fdb.PrefixRange(k2)
					r.Begin = fdb.FirstGreaterOrEqual(k1)
					r.End = fdb.FirstGreaterOrEqual(pr2.End)
				}
			case "begins_with":
				skVal := skVals[0]
				if skVal.S != nil {
					k1, _ := getSKKey(skVal)
					str := *skVal.S
					strNext := str + "\x00"
					for i := len(str) - 1; i >= 0; i-- {
						if str[i] < 0xff {
							strNext = str[:i] + string(str[i]+1)
							break
						}
					}
					avNext := models.AttributeValue{S: &strNext}
					k2, _ := getSKKey(&avNext)
					r.Begin = fdb.FirstGreaterOrEqual(k1)
					r.End = fdb.FirstGreaterOrEqual(k2)
				} else {
					return nil, fmt.Errorf("begins_with SK must be string")
				}
			default:
				return nil, fmt.Errorf("unsupported SK operator: %s", skOp)
			}
		}

		if len(exclusiveStartKey) > 0 {
			var keyTuple []tuple.TupleElement
			var err error
			if indexName != "" {
				// Use helper from foundationdb_store_indexes.go
				kt, err := s.buildIndexKeyTuple(table, targetKeySchema, exclusiveStartKey)
				if err != nil {
					return nil, err
				}
				keyTuple = kt
			} else {
				keyTuple, err = s.buildKeyTuple(table, exclusiveStartKey)
				if err != nil {
					return nil, err
				}
			}

			if indexName != "" {
				startFDBKey := subspace.Pack(keyTuple)
				r.Begin = fdb.FirstGreaterThan(startFDBKey)
			} else {
				startFDBKey := subspace.Pack(append(tuple.Tuple{"data"}, keyTuple...))
				r.Begin = fdb.FirstGreaterThan(startFDBKey)
			}
		}

		opts := fdb.RangeOptions{}
		if limit > 0 {
			opts.Limit = int(limit)
		}
		iter := rtr.GetRange(r, opts).Iterator()

		var items []map[string]models.AttributeValue
		var lastProcessedItem map[string]models.AttributeValue
		var itemsRead int

		for iter.Advance() {
			kv, err := iter.Get()
			if err != nil {
				return nil, err
			}
			var item map[string]models.AttributeValue
			if len(kv.Value) > 0 {
				_ = json.Unmarshal(kv.Value, &item)

				lastProcessedItem = item
				itemsRead++

				// Evaluate Filter
				match, err := s.evaluator.EvaluateFilter(item, filterExpression, expressionAttributeNames, expressionAttributeValues)
				if err != nil {
					return nil, err
				}
				if match {
					item = s.evaluator.ProjectItem(item, projectionExpression, expressionAttributeNames)
					items = append(items, item)
				}
			}
		}

		var lastEvaluatedKey map[string]models.AttributeValue
		if limit > 0 && itemsRead == int(limit) && lastProcessedItem != nil {
			lastEvaluatedKey = make(map[string]models.AttributeValue)
			for _, ks := range table.KeySchema {
				if val, ok := lastProcessedItem[ks.AttributeName]; ok {
					lastEvaluatedKey[ks.AttributeName] = val
				}
			}
		}

		return struct {
			Items   []map[string]models.AttributeValue
			LastKey map[string]models.AttributeValue
		}{items, lastEvaluatedKey}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	result := res.(struct {
		Items   []map[string]models.AttributeValue
		LastKey map[string]models.AttributeValue
	})
	return result.Items, result.LastKey, nil
}

// CreateTable persists a new table's metadata within a FoundationDB transaction.
func (s *FoundationDBStore) CreateTable(ctx context.Context, table *models.Table) error {
	if table.TableName == "" {
		return models.New("ValidationException", "TableName cannot be empty")
	}
	fmt.Println("Creating table :", table.TableName)
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Simulating instant provisioning for ConcreteDB
		table.Status = models.StatusActive

		if table.StreamSpecification != nil && table.StreamSpecification.StreamEnabled {
			now := time.Now().UTC()
			label := now.Format("2006-01-02T15:04:05.000")
			table.LatestStreamLabel = label
			table.LatestStreamArn = fmt.Sprintf("arn:aws:dynamodb:local:000000000000:table/%s/stream/%s", table.TableName, label)
		}

		// Marshal the table metadata to JSON
		tableBytes, err := json.Marshal(table)
		if err != nil {
			return nil, err
		}

		// Create or Open the directory for this table
		// Path: ["tables", tableName]
		// This allocates a unique prefix for the table's data
		tableDir, err := s.dir.CreateOrOpen(tr, []string{"tables", table.TableName}, nil)
		if err != nil {
			return nil, err
		}

		// Use a specific key regarding metadata within that directory
		metaKey := tableDir.Pack(tuple.Tuple{"metadata"})

		// Check if it already has metadata (implies existence)
		// Note: CreateOrOpen is idempotent, effectively "GetOrConnect".
		// We explicitly check if metadata exists to enforce "ErrTableExists"
		existingVal, err := tr.Get(metaKey).Get()
		if err != nil {
			return nil, err
		}
		if existingVal != nil {
			return nil, ErrTableExists
		}

		// Save metadata
		tr.Set(metaKey, tableBytes)

		// Create directories for GSIs
		for _, gsi := range table.GlobalSecondaryIndexes {
			_, err := s.dir.CreateOrOpen(tr, []string{"tables", table.TableName, "index", gsi.IndexName}, nil)
			if err != nil {
				return nil, err
			}
		}

		// Create directories for LSIs
		for _, lsi := range table.LocalSecondaryIndexes {
			_, err := s.dir.CreateOrOpen(tr, []string{"tables", table.TableName, "index", lsi.IndexName}, nil)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})
	return err
}

// GetTable retrieves a table's metadata by name.
func (s *FoundationDBStore) GetTable(ctx context.Context, tableName string) (*models.Table, error) {
	val, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		// Check if directory exists first
		exists, err := s.dir.Exists(rtr, []string{"tables", tableName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}

		// Open the directory (read-only safe if exists)
		tableDir, err := s.dir.Open(rtr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}

		return rtr.Get(tableDir.Pack(tuple.Tuple{"metadata"})).Get()
	})
	if err != nil {
		return nil, err
	}

	tableBytes, ok := val.([]byte)
	if !ok || len(tableBytes) == 0 {
		return nil, nil
	}

	var table models.Table
	if err := json.Unmarshal(tableBytes, &table); err != nil {
		return nil, err
	}

	return &table, nil
}

// UpdateTable updates a table's metadata (e.g. enabling streams).
func (s *FoundationDBStore) UpdateTable(ctx context.Context, tableName string, streamSpec *models.StreamSpecification) (*models.Table, error) {
	val, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Get Table Metadata
		table, err := s.getTableInternal(tr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}

		// 2. Update Stream Spec if provided
		if streamSpec != nil {
			table.StreamSpecification = streamSpec
			if streamSpec.StreamEnabled {
				now := time.Now().UTC()
				label := now.Format("2006-01-02T15:04:05.000")
				table.LatestStreamLabel = label
				table.LatestStreamArn = fmt.Sprintf("arn:aws:dynamodb:local:000000000000:table/%s/stream/%s", table.TableName, label)
			}
		}

		// 3. Save Metadata
		tableBytes, err := json.Marshal(table)
		if err != nil {
			return nil, err
		}
		tableDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		tr.Set(tableDir.Pack(tuple.Tuple{"metadata"}), tableBytes)

		return table, nil
	})
	if err != nil {
		return nil, err
	}
	return val.(*models.Table), nil
}

func (s *FoundationDBStore) DeleteTable(ctx context.Context, tableName string) (*models.Table, error) {
	val, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Check existence
		exists, err := s.dir.Exists(tr, []string{"tables", tableName})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrTableNotFound
		}

		tableDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		metaKey := tableDir.Pack(tuple.Tuple{"metadata"})

		existingVal, err := tr.Get(metaKey).Get()
		if err != nil {
			return nil, err
		}
		if existingVal == nil {
			// Should strictly not happen if dir exists, but possible if inconsistent
			return nil, ErrTableNotFound
		}

		var table models.Table
		if err := json.Unmarshal(existingVal, &table); err != nil {
			return nil, err
		}

		// Mark as deleting (optional if we really delete it immediately, but good for return value)
		table.Status = models.StatusDeleting

		// Actually delete the directory (metadata + data)
		// This removes the prefix mapping and all data under it.
		if _, err := s.dir.Remove(tr, []string{"tables", tableName}); err != nil {
			return nil, err
		}

		return &table, nil
	})

	if err != nil {
		return nil, err
	}
	return val.(*models.Table), nil
}

func (s *FoundationDBStore) ListTables(ctx context.Context, limit int, exclusiveStartTableName string) ([]string, string, error) {
	// We fetch one more than the limit to determine if there are more tables.
	// If limit is 0 (unspecified), we can pick a default, or just pass 0 if fdb supports it (usually means all).
	// DynamoDB default is usually larger, but let's stick to user request.
	// If the user didn't specify a limit, we might want to default to 100 or something.
	fetchLimit := limit
	if fetchLimit > 0 {
		fetchLimit++
	} else {
		fetchLimit = 101 // Default fetch size if none provided
	}

	res, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		options := directory.ListOptions{
			Limit: fetchLimit,
			After: exclusiveStartTableName,
		}
		return s.dir.List(rtr, []string{"tables"}, options)
	})
	if err != nil {
		return nil, "", err
	}

	tableNames := res.([]string)
	lastEvaluatedTableName := ""

	if limit > 0 && len(tableNames) > limit {
		// We have more results than requested
		lastEvaluatedTableName = tableNames[limit-1]
		tableNames = tableNames[:limit]
	} else if len(tableNames) > 0 {
		// If we didn't hit the limit (or no limit was set but we fetched default),
		// we check if we need pagination.
		// If fetchLimit was set to 101 (default) and we got 101, then we have a LEK.
		if limit == 0 && len(tableNames) == 101 {
			lastEvaluatedTableName = tableNames[100-1] // The 100th item is the last one we return
			tableNames = tableNames[:100]
		}
	}

	return tableNames, lastEvaluatedTableName, nil
}

// PutItem writes an item to the table.
func (s *FoundationDBStore) PutItem(ctx context.Context, tableName string, item map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	val, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Get Table Metadata
		table, err := s.getTableInternal(tr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}

		return s.putItemInternal(tr, table, item, conditionExpression, exprAttrNames, exprAttrValues, returnValues)
	})
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	return val.(map[string]models.AttributeValue), nil
}

func (s *FoundationDBStore) putItemInternal(tr fdb.Transaction, table *models.Table, item map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	// 1. Extract Key Fields
	keyTuple, err := s.buildKeyTuple(table, item)
	if err != nil {
		return nil, err
	}

	// 2. Always fetch old item for index maintenance AND conditional checks
	oldItem, err := s.getItemInternal(tr, table, keyTuple)
	if err != nil {
		return nil, err
	}

	// 3. Conditional Check
	if conditionExpression != "" {
		// Ensure oldItem is not nil for evaluator
		evalItem := oldItem
		if evalItem == nil {
			evalItem = make(map[string]models.AttributeValue)
		}
		match, err := s.evaluator.EvaluateFilter(evalItem, conditionExpression, exprAttrNames, exprAttrValues)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, models.New("ConditionalCheckFailedException", "The conditional request failed")
		}
	}

	// 4. Serialize Item
	itemBytes, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}

	// 5. Store
	tableDir, err := s.dir.Open(tr, []string{"tables", table.TableName}, nil)
	if err != nil {
		return nil, err
	}
	itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
	tr.Set(itemKey, itemBytes)

	// 6. Update Indexes
	if err := s.updateIndexes(tr, table, oldItem, item); err != nil {
		return nil, err
	}

	// 7. Stream Record
	eventName := "MODIFY"
	if len(oldItem) == 0 {
		eventName = "INSERT" // Or INSERT if oldItem was nil/empty
	}
	if err := s.writeStreamRecord(tr, table, eventName, oldItem, item); err != nil {
		return nil, err
	}

	if returnValues == "ALL_OLD" {
		return oldItem, nil
	}
	return nil, nil
}

// GetItem retrieves an item by key.
func (s *FoundationDBStore) GetItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, projectionExpression string, expressionAttributeNames map[string]string, consistentRead bool) (map[string]models.AttributeValue, error) {
	val, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		// 1. Get Table Metadata
		table, err := s.getTableInternal(rtr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}

		// 2. Extract Key Fields from the *request key map*
		keyTuple, err := s.buildKeyTuple(table, key)
		if err != nil {
			return nil, err
		}

		// 3. Fetch
		tableDir, err := s.dir.Open(rtr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
		return rtr.Get(itemKey).Get()
	})
	if err != nil {
		return nil, err
	}

	valBytes, ok := val.([]byte)
	if !ok || len(valBytes) == 0 {
		return nil, nil
	}

	// 4. Deserialize
	var item map[string]models.AttributeValue
	if err := json.Unmarshal(valBytes, &item); err != nil {
		return nil, err
	}

	// 5. Apply Projection
	if projectionExpression != "" {
		item = s.evaluator.ProjectItem(item, projectionExpression, expressionAttributeNames)
	}

	return item, nil
}

// DeleteItem deletes an item by key.
func (s *FoundationDBStore) DeleteItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	val, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// 1. Get Table Metadata
		table, err := s.getTableInternal(tr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}

		return s.deleteItemInternal(tr, table, key, conditionExpression, exprAttrNames, exprAttrValues, returnValues)
	})
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	return val.(map[string]models.AttributeValue), nil
}

func (s *FoundationDBStore) deleteItemInternal(tr fdb.Transaction, table *models.Table, key map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	// 1. Extract Key Fields
	keyTuple, err := s.buildKeyTuple(table, key)
	if err != nil {
		return nil, err
	}

	// 2. Always fetch old item for index maintenance AND conditional checks
	oldItem, err := s.getItemInternal(tr, table, keyTuple)
	if err != nil {
		return nil, err
	}

	// 3. Conditional Check
	if conditionExpression != "" {
		evalItem := oldItem
		if evalItem == nil {
			evalItem = make(map[string]models.AttributeValue)
		}
		match, err := s.evaluator.EvaluateFilter(evalItem, conditionExpression, exprAttrNames, exprAttrValues)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, models.New("ConditionalCheckFailedException", "The conditional request failed")
		}
	}

	// 4. Delete
	tableDir, err := s.dir.Open(tr, []string{"tables", table.TableName}, nil)
	if err != nil {
		return nil, err
	}
	itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
	tr.Clear(itemKey)

	// 5. Update Indexes
	if err := s.updateIndexes(tr, table, oldItem, nil); err != nil {
		return nil, err
	}

	// 6. Stream Record
	if oldItem != nil { // Only stream if something was deleted
		if err := s.writeStreamRecord(tr, table, "REMOVE", oldItem, nil); err != nil {
			return nil, err
		}
	}

	if returnValues == "ALL_OLD" {
		return oldItem, nil
	}
	return nil, nil
}

// Helpers

func (s *FoundationDBStore) getTableInternal(rtr fdb.ReadTransaction, tableName string) (*models.Table, error) {
	exists, err := s.dir.Exists(rtr, []string{"tables", tableName})
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	tableDir, err := s.dir.Open(rtr, []string{"tables", tableName}, nil)
	if err != nil {
		return nil, err
	}
	val, err := rtr.Get(tableDir.Pack(tuple.Tuple{"metadata"})).Get()
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	var table models.Table
	if err := json.Unmarshal(val, &table); err != nil {
		return nil, err
	}
	return &table, nil
}

func (s *FoundationDBStore) getItemInternal(rt fdb.ReadTransaction, table *models.Table, keyTuple []tuple.TupleElement) (map[string]models.AttributeValue, error) {
	tableDir, err := s.dir.Open(rt, []string{"tables", table.TableName}, nil)
	if err != nil {
		return nil, err
	}
	itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
	valBytes, err := rt.Get(itemKey).Get()
	if err != nil {
		return nil, err
	}
	if len(valBytes) == 0 {
		return nil, nil
	}

	var item map[string]models.AttributeValue
	if err := json.Unmarshal(valBytes, &item); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *FoundationDBStore) buildKeyTuple(table *models.Table, item map[string]models.AttributeValue) ([]tuple.TupleElement, error) {
	return s.buildKeyTupleFromSchema(table.KeySchema, item)
}

func (s *FoundationDBStore) buildKeyTupleFromSchema(keySchema []models.KeySchemaElement, item map[string]models.AttributeValue) ([]tuple.TupleElement, error) {
	var pkHashName, pkRangeName string
	for _, k := range keySchema {
		if k.KeyType == "HASH" {
			pkHashName = k.AttributeName
		} else if k.KeyType == "RANGE" {
			pkRangeName = k.AttributeName
		}
	}

	if pkHashName == "" {
		return nil, fmt.Errorf("schema has no HASH key")
	}

	hashVal, ok := item[pkHashName]
	if !ok {
		return nil, fmt.Errorf("missing HASH key: %s", pkHashName)
	}
	hashTupleElem, err := toTupleElement(hashVal)
	if err != nil {
		return nil, err
	}

	elems := []tuple.TupleElement{hashTupleElem}

	if pkRangeName != "" {
		rangeVal, ok := item[pkRangeName]
		if !ok {
			return nil, fmt.Errorf("missing RANGE key: %s", pkRangeName)
		}
		rangeTupleElem, err := toTupleElement(rangeVal)
		if err != nil {
			return nil, err
		}
		elems = append(elems, rangeTupleElem)
	}

	return elems, nil
}

func toTupleElement(av models.AttributeValue) (tuple.TupleElement, error) {
	if av.S != nil {
		return *av.S, nil
	}
	if av.N != nil {
		// Store numbers as strings/bytes or convert?
		// FDB Tuple layer supports integers and floats. DynamoDB numbers are arbitrary precision strings.
		// For MVP simplicity and sorting correctness for common numbers, maybe store as string?
		// Actually, DynamoDB strings sort differently than numbers.
		// If we store as string in tuple, "10" comes before "2". That's wrong for numbers.
		// We should try to parse as float64 or int64 if possible for ordering.
		// But for exact equality matching (Hash key), string is fine.
		// Range keys need proper ordering.
		// Let's store as string for MVP to ensure round-trip accuracy, realizing range queries might be lexicographical on the string rep.
		return *av.N, nil
	}
	if av.B != nil {
		return []byte(*av.B), nil
	}
	// ... other types ...
	// For PKs, usually only S, N, B are allowed.
	return nil, fmt.Errorf("unsupported key type or null")
}

// UpdateItem updates an item.
func (s *FoundationDBStore) UpdateItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, updateExpression string, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	val, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		table, err := s.getTableInternal(tr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}
		return s.updateItemInternal(tr, table, key, updateExpression, conditionExpression, exprAttrNames, exprAttrValues, returnValues)
	})

	if err != nil {
		return nil, err
	}
	if v, ok := val.(map[string]models.AttributeValue); ok {
		return v, nil
	}
	return nil, nil
}

func (s *FoundationDBStore) updateItemInternal(tr fdb.Transaction, table *models.Table, key map[string]models.AttributeValue, updateExpression string, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue, returnValues string) (map[string]models.AttributeValue, error) {
	// 1. Extract Key Fields
	keyTuple, err := s.buildKeyTuple(table, key)
	if err != nil {
		return nil, err
	}

	// 2. Open Directory
	tableDir, err := s.dir.Open(tr, []string{"tables", table.TableName}, nil)
	if err != nil {
		return nil, err
	}
	itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))

	// 3. Read Existing Item
	existingBytes, err := tr.Get(itemKey).Get()
	if err != nil {
		return nil, err
	}

	var item map[string]models.AttributeValue
	if existingBytes != nil {
		if err := json.Unmarshal(existingBytes, &item); err != nil {
			return nil, err
		}
	} else {
		// Item doesn't exist. Create new one initialized with Key.
		item = make(map[string]models.AttributeValue)
		for k, v := range key {
			item[k] = v
		}
	}

	// Capture old item for return values, index maintenance, and conditions
	var oldItem map[string]models.AttributeValue
	if existingBytes != nil {
		oldItem = make(map[string]models.AttributeValue)
		for k, v := range item {
			oldItem[k] = v
		}
	}

	// 4. Conditional Check
	if conditionExpression != "" {
		evalItem := oldItem
		if evalItem == nil {
			evalItem = make(map[string]models.AttributeValue)
		}
		match, err := s.evaluator.EvaluateFilter(evalItem, conditionExpression, exprAttrNames, exprAttrValues)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, models.New("ConditionalCheckFailedException", "The conditional request failed")
		}
	}

	// 5. Apply Update Expression
	changed := make(map[string]bool)
	if updateExpression != "" {
		c, err := applyUpdateExpression(item, updateExpression, exprAttrNames, exprAttrValues)
		if err != nil {
			return nil, err
		}
		changed = c
	}

	// 5a. Validate PK not changed
	for _, k := range table.KeySchema {
		if changed[k.AttributeName] {
			return nil, fmt.Errorf("cannot update attribute %s. This is part of the key", k.AttributeName)
		}
	}

	// 6. Serialize and Write
	itemBytes, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	tr.Set(itemKey, itemBytes)

	// 7. Update Indexes
	if err := s.updateIndexes(tr, table, oldItem, item); err != nil {
		return nil, err
	}

	// 8. Stream Record
	eventName := "MODIFY"
	if oldItem == nil {
		eventName = "INSERT" // It was a new item (Upsert)
	}
	if err := s.writeStreamRecord(tr, table, eventName, oldItem, item); err != nil {
		return nil, err
	}

	// 9. Handle ReturnValues
	if returnValues == "ALL_OLD" {
		return oldItem, nil
	}
	if returnValues == "ALL_NEW" {
		return item, nil
	}
	if returnValues == "UPDATED_OLD" {
		res := make(map[string]models.AttributeValue)
		for k := range changed {
			if v, ok := oldItem[k]; ok {
				res[k] = v
			}
		}
		return res, nil
	}
	if returnValues == "UPDATED_NEW" {
		res := make(map[string]models.AttributeValue)
		for k := range changed {
			if v, ok := item[k]; ok {
				res[k] = v
			}
		}
		return res, nil
	}
	return nil, nil // NONE
}

func (s *FoundationDBStore) conditionCheckInternal(tr fdb.Transaction, table *models.Table, key map[string]models.AttributeValue, conditionExpression string, exprAttrNames map[string]string, exprAttrValues map[string]models.AttributeValue) error {
	keyTuple, err := s.buildKeyTuple(table, key)
	if err != nil {
		return err
	}

	tableDir, err := s.dir.Open(tr, []string{"tables", table.TableName}, nil)
	if err != nil {
		return err
	}
	itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))

	// Read Existing
	existingBytes, err := tr.Get(itemKey).Get()
	if err != nil {
		return err
	}

	var item map[string]models.AttributeValue
	if existingBytes != nil {
		if err := json.Unmarshal(existingBytes, &item); err != nil {
			return err
		}
	} else {
		item = make(map[string]models.AttributeValue)
	}

	// Evaluate
	match, err := s.evaluator.EvaluateFilter(item, conditionExpression, exprAttrNames, exprAttrValues)
	if err != nil {
		return err
	}
	if !match {
		return models.New("ConditionalCheckFailedException", "The conditional request failed")
	}
	return nil
}

func (s *FoundationDBStore) TransactGetItems(ctx context.Context, transactItems []models.TransactGetItem) ([]models.ItemResponse, error) {
	// Constraints: Max 25 items
	if len(transactItems) > 25 {
		return nil, models.New("ValidationException", "Too many items in transaction")
	}

	res, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		responses := make([]models.ItemResponse, len(transactItems))

		for i, req := range transactItems {
			// 1. Get Table
			table, err := s.getTableInternal(rtr, req.Get.TableName)
			if err != nil {
				return nil, err
			}
			if table == nil {
				return nil, models.New("ResourceNotFoundException", "Requested resource not found")
			}

			// 2. Build Key
			keyTuple, err := s.buildKeyTuple(table, req.Get.Key)
			if err != nil {
				return nil, err
			}

			// 3. Get Item
			item, err := s.getItemInternal(rtr, table, keyTuple)
			if err != nil {
				return nil, err
			}
			if req.Get.ProjectionExpression != "" {
				item = s.evaluator.ProjectItem(item, req.Get.ProjectionExpression, req.Get.ExpressionAttributeNames)
			}
			responses[i] = models.ItemResponse{Item: item}
		}
		return responses, nil
	})

	if err != nil {
		return nil, err
	}
	return res.([]models.ItemResponse), nil
}

func (s *FoundationDBStore) TransactWriteItems(ctx context.Context, transactItems []models.TransactWriteItem, clientRequestToken string) error {
	// Constraints
	if len(transactItems) > 25 {
		return models.New("ValidationException", "Too many items in transaction")
	}

	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for _, item := range transactItems {
			// Determine action
			var tableName string
			if item.ConditionCheck != nil {
				tableName = item.ConditionCheck.TableName
			} else if item.Put != nil {
				tableName = item.Put.TableName
			} else if item.Delete != nil {
				tableName = item.Delete.TableName
			} else if item.Update != nil {
				tableName = item.Update.TableName
			} else {
				return nil, models.New("ValidationException", "TransactWriteItem must have one action set")
			}

			table, err := s.getTableInternal(tr, tableName)
			if err != nil {
				return nil, err
			}
			if table == nil {
				return nil, models.New("ResourceNotFoundException", "Requested resource not found: "+tableName)
			}

			// Dispatch
			if item.ConditionCheck != nil {
				if err := s.conditionCheckInternal(tr, table, item.ConditionCheck.Key, item.ConditionCheck.ConditionExpression, item.ConditionCheck.ExpressionAttributeNames, item.ConditionCheck.ExpressionAttributeValues); err != nil {
					return nil, err
				}
			} else if item.Put != nil {
				if _, err := s.putItemInternal(tr, table, item.Put.Item, item.Put.ConditionExpression, item.Put.ExpressionAttributeNames, item.Put.ExpressionAttributeValues, "NONE"); err != nil {
					return nil, err
				}
			} else if item.Delete != nil {
				if _, err := s.deleteItemInternal(tr, table, item.Delete.Key, item.Delete.ConditionExpression, item.Delete.ExpressionAttributeNames, item.Delete.ExpressionAttributeValues, "NONE"); err != nil {
					return nil, err
				}
			} else if item.Update != nil {
				if _, err := s.updateItemInternal(tr, table, item.Update.Key, item.Update.UpdateExpression, item.Update.ConditionExpression, item.Update.ExpressionAttributeNames, item.Update.ExpressionAttributeValues, "NONE"); err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	})

	return err
}

// Simple expression parser for MVP supporting SET and REMOVE
func applyUpdateExpression(item map[string]models.AttributeValue, expr string, names map[string]string, values map[string]models.AttributeValue) (map[string]bool, error) {
	changed := make(map[string]bool)
	expr = strings.TrimSpace(expr)

	// Split by major sections (SET, REMOVE)
	sections := make(map[string]string)
	currentSection := ""
	var sectionWords []string

	words := strings.Fields(expr)
	for _, word := range words {
		upper := strings.ToUpper(word)
		if upper == "SET" || upper == "REMOVE" || upper == "ADD" || upper == "DELETE" {
			if currentSection != "" {
				sections[currentSection] = strings.Join(sectionWords, " ")
			}
			currentSection = upper
			sectionWords = nil
		} else {
			sectionWords = append(sectionWords, word)
		}
	}
	if currentSection != "" {
		sections[currentSection] = strings.Join(sectionWords, " ")
	}

	// If no section keyword, assume SET for backward compatibility with very simple expressions
	if currentSection == "" && expr != "" {
		sections["SET"] = expr
	}

	// Helpers
	resolve := func(name string) (string, error) {
		if strings.HasPrefix(name, "#") {
			if n, ok := names[name]; ok {
				return n, nil
			}
			return "", fmt.Errorf("missing expression attribute name: %s", name)
		}
		return name, nil
	}

	// Process SET
	if body, ok := sections["SET"]; ok {
		assignments := strings.Split(body, ",")
		for _, assignment := range assignments {
			assignment = strings.TrimSpace(assignment)
			if assignment == "" {
				continue
			}
			parts := strings.Split(assignment, "=")
			if len(parts) != 2 {
				return nil, fmt.Errorf("invalid assignment: %s", assignment)
			}

			lhs := strings.TrimSpace(parts[0])
			rhs := strings.TrimSpace(parts[1])

			realName, err := resolve(lhs)
			if err != nil {
				return nil, err
			}

			// Resolve RHS (Value)
			var val models.AttributeValue
			if strings.HasPrefix(rhs, ":") {
				if v, ok := values[rhs]; ok {
					val = v
				} else {
					return nil, fmt.Errorf("missing expression attribute value: %s", rhs)
				}
			} else {
				return nil, fmt.Errorf("only literal values with ':' prefix supported in MVP")
			}

			item[realName] = val
			changed[realName] = true
		}
	}

	// Process REMOVE
	if body, ok := sections["REMOVE"]; ok {
		attrs := strings.Split(body, ",")
		for _, a := range attrs {
			name := strings.TrimSpace(a)
			if name == "" {
				continue
			}
			realName, err := resolve(name)
			if err != nil {
				return nil, err
			}
			delete(item, realName)
			changed[realName] = true
		}
	}

	if _, ok := sections["ADD"]; ok {
		return nil, fmt.Errorf("ADD expression not yet supported in MVP")
	}
	if _, ok := sections["DELETE"]; ok {
		return nil, fmt.Errorf("DELETE expression not yet supported in MVP")
	}

	return changed, nil
}

// BatchGetItem retrieves multiple items from multiple tables.
func (s *FoundationDBStore) BatchGetItem(ctx context.Context, requestItems map[string]models.KeysAndAttributes) (map[string][]map[string]models.AttributeValue, map[string]models.KeysAndAttributes, error) {
	responses := make(map[string][]map[string]models.AttributeValue)
	unprocessed := make(map[string]models.KeysAndAttributes)

	// Real implementation with Futures
	res, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		type futureItem struct {
			tableName string
			future    fdb.FutureByteSlice
		}
		var futures []futureItem

		// 1. Launch reads
		for tableName, ka := range requestItems {
			table, err := s.getTableInternal(rtr, tableName)
			if err != nil {
				return nil, err
			}
			if table == nil {
				return nil, models.New("ResourceNotFoundException", "Requested resource not found")
			}
			tableDir, err := s.dir.Open(rtr, []string{"tables", tableName}, nil)
			if err != nil {
				return nil, err
			}

			for _, key := range ka.Keys {
				keyTuple, err := s.buildKeyTuple(table, key)
				if err != nil {
					return nil, err
				}
				itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))
				futures = append(futures, futureItem{
					tableName: tableName,
					future:    rtr.Get(itemKey),
				})
			}
		}

		// 2. Await results
		output := make(map[string][]map[string]models.AttributeValue)
		for _, f := range futures {
			valBytes, err := f.future.Get()
			if err != nil {
				return nil, err
			}
			if len(valBytes) > 0 {
				var item map[string]models.AttributeValue
				if err := json.Unmarshal(valBytes, &item); err != nil {
					return nil, err
				}
				if ka, ok := requestItems[f.tableName]; ok && ka.ProjectionExpression != "" {
					item = s.evaluator.ProjectItem(item, ka.ProjectionExpression, ka.ExpressionAttributeNames)
				}
				output[f.tableName] = append(output[f.tableName], item)
			}
		}
		return output, nil
	})

	if err != nil {
		return nil, nil, err
	}
	responses = res.(map[string][]map[string]models.AttributeValue)
	return responses, unprocessed, nil // UnprocessedKeys unused in happy path MVP
}

// BatchWriteItem puts or deletes multiple items in multiple tables.
func (s *FoundationDBStore) BatchWriteItem(ctx context.Context, requestItems map[string][]models.WriteRequest) (map[string][]models.WriteRequest, error) {
	unprocessed := make(map[string][]models.WriteRequest)

	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for tableName, requests := range requestItems {
			table, err := s.getTableInternal(tr, tableName)
			if err != nil {
				return nil, err
			}
			if table == nil {
				return nil, models.New("ResourceNotFoundException", "Requested resource not found: "+tableName)
			}

			for _, req := range requests {
				if req.PutRequest != nil {
					if _, err := s.putItemInternal(tr, table, req.PutRequest.Item, "", nil, nil, "NONE"); err != nil {
						return nil, err
					}
				} else if req.DeleteRequest != nil {
					if _, err := s.deleteItemInternal(tr, table, req.DeleteRequest.Key, "", nil, nil, "NONE"); err != nil {
						return nil, err
					}
				}
			}
		}
		return nil, nil
	})

	if err != nil {
		return nil, err
	}
	return unprocessed, nil
}

// writeStreamRecord writes a Stream record to the FDB if streaming is enabled.
func (s *FoundationDBStore) writeStreamRecord(tr fdb.Transaction, table *models.Table, eventName string, oldImage, newImage map[string]models.AttributeValue) error {
	if table.StreamSpecification == nil || !table.StreamSpecification.StreamEnabled {
		return nil
	}
	viewType := table.StreamSpecification.StreamViewType

	// Create inner StreamRecord
	streamRecord := models.StreamRecord{
		ApproximateCreationDateTime: float64(time.Now().Unix()),
		Keys:                        s.extractKeys(table, oldImage, newImage),
		StreamViewType:              viewType,
		// SequenceNumber will be set by the versionstamp efficiently if we could,
		// but since it's inside the value JSON, we can't easily versionstamp it dynamically
		// without a second read. For now, we leave it empty or use a placeholder.
		// Real DynamoDB puts the sequence number here.
		// We might need to rethink this if we strictly need it in the payload.
		SequenceNumber: "PENDING",
		SizeBytes:      0,
	}

	// Filter based on ViewType
	if viewType == "NEW_IMAGE" || viewType == "NEW_AND_OLD_IMAGES" {
		streamRecord.NewImage = newImage
	}
	if viewType == "OLD_IMAGE" || viewType == "NEW_AND_OLD_IMAGES" {
		streamRecord.OldImage = oldImage
	}

	// Create outer Record
	record := models.Record{
		AwsRegion:    "local",
		Dynamodb:     streamRecord,
		EventID:      uuid.New().String(),
		EventName:    eventName,
		EventSource:  "aws:dynamodb",
		EventVersion: "1.1",
	}

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Key Structure: ["tables", tableName, "stream", "shard-0000", <versionstamp>]
	shardId := "shard-0000"
	streamDir, err := s.dir.CreateOrOpen(tr, []string{"tables", table.TableName, "stream", shardId}, nil)
	if err != nil {
		return err
	}

	// Use SetVersionstampedKey
	t := tuple.Tuple{tuple.IncompleteVersionstamp(0)}
	key, err := t.PackWithVersionstamp(streamDir.Bytes())
	if err != nil {
		return err
	}
	tr.SetVersionstampedKey(fdb.Key(key), recordBytes)

	return nil
}

// extractKeys extracts the primary key (PK and SK) from attributes.
func (s *FoundationDBStore) extractKeys(table *models.Table, oldItem, newItem map[string]models.AttributeValue) map[string]models.AttributeValue {
	keys := make(map[string]models.AttributeValue)
	source := newItem
	if source == nil {
		source = oldItem
	}
	if source == nil {
		return keys
	}

	for _, ks := range table.KeySchema {
		if val, ok := source[ks.AttributeName]; ok {
			keys[ks.AttributeName] = val
		}
	}
	return keys
}

// Stream APIs

// ListStreams lists the streams.
// Note: This implementation scans tables to find active streams, which is suitable for MVP.
func (s *FoundationDBStore) ListStreams(ctx context.Context, tableName string, limit int, exclusiveStartStreamArn string) ([]models.StreamSummary, string, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 100 {
		limit = 100
	}

	// Calculate start table name from ARN if provided
	var startTableName string
	if exclusiveStartStreamArn != "" {
		// Format: arn:aws:dynamodb:local:000000000000:table/TableName/stream/Timestamp
		parts := strings.Split(exclusiveStartStreamArn, "/")
		if len(parts) >= 2 {
			// Find the table part. Usually it's in the part that contains ":table"
			for i, p := range parts {
				if (p == "table" || strings.Contains(p, ":table")) && i+1 < len(parts) {
					startTableName = parts[i+1]
					break
				}
			}
		}
	}
	// If tableName filter is provided, we just check that one table.
	// But ListStreams API is usually global unless specific filtering logic?
	// DynamoDB ListStreams expects "TableName" as optional filter.
	// If TableName provided, we just return that table's stream if any.

	summaries := []models.StreamSummary{}
	var lastEvaluatedStreamArn string

	if tableName != "" {
		// Specific table
		table, err := s.GetTable(ctx, tableName)
		if err != nil {
			return nil, "", err
		}
		if table != nil && table.StreamSpecification != nil && table.StreamSpecification.StreamEnabled {
			// Only return if it matches start ARN criteria (simplified: if start ARN provided, we might skip if it's same? logic is complex, assuming simple case)
			if exclusiveStartStreamArn != "" && table.LatestStreamArn == exclusiveStartStreamArn {
				// skip
			} else {
				summaries = append(summaries, models.StreamSummary{
					StreamArn:   table.LatestStreamArn,
					StreamLabel: table.LatestStreamLabel,
					TableName:   table.TableName,
				})
			}
		}
		return summaries, "", nil
	}

	// Scan tables
	// tables, lastTable, err := s.ListTables(ctx, limit, startTableName)
	// We do manual scan to populate stream info.
	// Getting one by one is N+1 but ok for MVP.

	// Issue: We might scan 100 tables and find 0 streams. Pagination becomes hard.
	// Better approach: Scan tables until we find 'limit' streams or hit end.
	// Since we reused ListTables which relies on FDB range scan of table names, we can do manual scan here.

	// Manual Scan of Table Metadata
	chunkSize := limit
	currentStart := startTableName

	for len(summaries) < limit {
		names, last, err := s.ListTables(ctx, chunkSize, currentStart)
		if err != nil {
			return nil, "", err
		}
		if len(names) == 0 {
			break
		}

		for _, name := range names {
			// Optimization: Should use ReadTransact for batch, but GetTable is clean.
			// Let's rely on GetTable
			t, err := s.GetTable(ctx, name)
			if err != nil {
				// skip or error? skip
				continue
			}
			if t != nil && t.StreamSpecification != nil && t.StreamSpecification.StreamEnabled {
				// Check strict ordering vs exclusiveStartStreamArn
				if exclusiveStartStreamArn != "" && t.LatestStreamArn == exclusiveStartStreamArn {
					continue
				}

				summaries = append(summaries, models.StreamSummary{
					StreamArn:   t.LatestStreamArn,
					StreamLabel: t.LatestStreamLabel,
					TableName:   t.TableName,
				})
			}
			if len(summaries) >= limit {
				lastEvaluatedStreamArn = summaries[len(summaries)-1].StreamArn
				break
			}
		}

		if last == "" {
			break
		}
		currentStart = last
	}

	return summaries, lastEvaluatedStreamArn, nil
}

// DescribeStream returns details about a stream.
func (s *FoundationDBStore) DescribeStream(ctx context.Context, streamArn string, limit int, exclusiveStartShardId string) (*models.StreamDescription, error) {
	// Parse TableName from ARN
	parts := strings.Split(streamArn, "/")
	if len(parts) < 4 {
		return nil, models.New("ValidationException", "Invalid Stream ARN")
	}
	tableName := parts[len(parts)-3] // table/NAME/stream/LABEL -> NAME at -3?
	// parts: [arn:aws:dynamodb:local:000000000000:table, TableName, stream, Label]
	// 0: prefix...table
	// 1: TableName
	// 2: stream
	// 3: Label
	// Split by "/" might not be enough if prefix has /? arn usually colons. resource part has /.
	// "arn:aws:dynamodb:local:000000000000:table/MyTable/stream/Label"
	// Split("/"):
	// 0: arn:aws:dynamodb:local:000000000000:table
	// 1: MyTable
	// 2: stream
	// 3: Label

	tableName = parts[1]

	table, err := s.GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, models.New("ResourceNotFoundException", "Table not found")
	}

	if table.LatestStreamArn != streamArn {
		return nil, models.New("ResourceNotFoundException", "Stream not found")
	}

	if table.Status != models.StatusActive {
		// Streams might still exist?
	}

	// Shards
	// For MVP, we have one shard: "shard-0000"
	shards := []models.Shard{}

	// Determine Sequence Numbers (approx)
	// We can't easily get strict start/end without querying.
	// We'll return open-ended shard.

	shards = append(shards, models.Shard{
		ShardId: "shard-0000",
		SequenceNumberRange: &models.SequenceNumberRange{
			StartingSequenceNumber: "00000000000000000000", // Start
		},
	})

	desc := &models.StreamDescription{
		StreamArn:               streamArn,
		StreamLabel:             table.LatestStreamLabel,
		StreamStatus:            "ENABLED",
		StreamViewType:          table.StreamSpecification.StreamViewType,
		CreationRequestDateTime: float64(table.CreationDateTime.Unix()), // Approx
		TableName:               tableName,
		KeySchema:               table.KeySchema,
		Shards:                  shards,
	}

	return desc, nil
}

// Iterator Structure
type ShardIteratorData struct {
	StreamArn    string
	ShardId      string
	IteratorType string
	SequenceNum  string // Hex string of 10-byte FDB versionstamp
}

// GetShardIterator returns a shard iterator.
func (s *FoundationDBStore) GetShardIterator(ctx context.Context, streamArn string, shardId string, shardIteratorType string, sequenceNumber string) (string, error) {
	data := ShardIteratorData{
		StreamArn:    streamArn,
		ShardId:      shardId,
		IteratorType: shardIteratorType,
		SequenceNum:  sequenceNumber,
	}

	js, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(js), nil
}

// GetRecords retrieves records from a shard using an iterator.
func (s *FoundationDBStore) GetRecords(ctx context.Context, shardIterator string, limit int) ([]models.Record, string, error) {
	if limit <= 0 {
		limit = 1000 // default
	}

	// Decode
	js, err := base64.StdEncoding.DecodeString(shardIterator)
	if err != nil {
		return nil, "", models.New("ValidationException", "Invalid ShardIterator")
	}
	var data ShardIteratorData
	if err := json.Unmarshal(js, &data); err != nil {
		return nil, "", models.New("ValidationException", "Invalid ShardIterator JSON")
	}

	// Parse TableName from ARN
	parts := strings.Split(data.StreamArn, "/")
	if len(parts) < 2 {
		return nil, "", models.New("ValidationException", "Invalid ARN in iterator")
	}
	tableName := parts[1]

	records := []models.Record{}
	var nextSeqNum string

	// Read from FDB
	_, err = s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		_, err := s.dir.Open(rtr, []string{"tables", tableName, "stream", data.ShardId}, nil)
		if err != nil {
			// Shard usually exists if iterator valid. If not, maybe expired.
			return nil, err
		}

		// var startKey fdb.Key
		// var endKey fdb.Key

		// Range logic
		// r := streamDir // Subspace

		// Logic based on IteratorType
		if data.IteratorType == "TRIM_HORIZON" || (data.IteratorType == "AFTER_SEQUENCE_NUMBER" && data.SequenceNum == "00000000000000000000") {
			// Start from beginning
			// startKey = r.FDBKey()
		} else if data.IteratorType == "LATEST" {
			// Start from end? LATEST means just after the last record.
			// Effectively returns empty and next iterator is future?
			// For MVP, LATEST logic is complex without a persistent "Last Sequence".
			// We'll treat LATEST as "End of Range".
			// startKey = r.FDBRangeKeys().End // No records
		} else if data.IteratorType == "AFTER_SEQUENCE_NUMBER" {
			// Decode SequenceNum (hex) -> Versionstamp -> Key
			// SequenceNum is just the Transaction version part or full stamp?
			// Models say SequenceNumber is string.
			// Internal: We stored Versionstamp in Key.
			// Key layout: [Subspace_Prefix, Versionstamp(12 bytes approx?)]
			// Versionstamp struct is 10 bytes TransactionVersion + 2 bytes UserVersion?
			// FDB Key has 10 bytes versionstamp at end if SetVersionstampedKey used correctly?
			// Actually FDB Versionstamp is 10 bytes. The tuple packs it.
			// tuple.Versionstamp struct has [10]byte TransactionVersion and uint16 UserVersion.

			// If we provided a hex string, decode it.
			// Assuming SequenceNum is the hex of the 10-byte TransactionVersion + 2-byte UserVersion?
			// Or just checking lexicographical order?

			// MVP: If SequenceNum is provided, we try to create a selector just after it.
			// But since we don't return SequenceNum in Record (it said "PENDING"), we have a problem.
			// We MUST return SequenceNum in the GetRecords response for each record!
			// So we need to populate SequenceNum when reading.
			// SequenceNum = hex(Key's Versionstamp component).

			// For AFTER_SEQUENCE_NUMBER with "seq":
			// We construct a key selector > "seq".
			// How to pass "seq" back to Key?
			// We need to construct the key tuple with that versionstamp.

			// Let's assume SequenceNumber is hex string of the *whole* packed versionstamp or just the 12 bytes?
			// The tuple packing adds overhead.
			// Correct approach: SequenceNumber = Hex(TransactionVersion|UserVersion).

			// ... Implementation details for AFTER_SEQUENCE_NUMBER ...
			// If we can't parse, default to start?
			// startKey = r.FDBKey() // Fallback
		}

		// Actually execute Range
		// We'll just read from startKey

		// To properly support AFTER_SEQUENCE_NUMBER, we need real parsing.
		// Let's implement refined loop below.
		return nil, nil
	})

	// Refined Read Block outside ReadTransact to avoid complexity inside? No, must be inside.

	// Let's rewrite the ReadTransact block to be more robust.
	res, err := s.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		streamDir, err := s.dir.Open(rtr, []string{"tables", tableName, "stream", data.ShardId}, nil)
		if err != nil {
			return nil, err
		}

		r := streamDir

		// Construct Range
		var beginSel fdb.Selectable
		beginSel = fdb.FirstGreaterOrEqual(fdb.Key(r.Bytes())) // Default start

		if data.IteratorType == "AFTER_SEQUENCE_NUMBER" && data.SequenceNum != "" {
			// Parse Hex
			seqBytes, err := hex.DecodeString(data.SequenceNum)
			if err == nil && len(seqBytes) == 12 {
				// Reconstruct versionstamp
				var tv [10]byte
				copy(tv[:], seqBytes[0:10])
				uv := uint16(seqBytes[10])<<8 | uint16(seqBytes[11])

				t := tuple.Tuple{tuple.Versionstamp{TransactionVersion: tv, UserVersion: uv}}
				// Pack using standard Pack, because it is Complete
				key := streamDir.Pack(t)
				beginSel = fdb.FirstGreaterThan(fdb.Key(key))
			}
		}

		// Use RangeOptions
		_, end := r.FDBRangeKeys()
		iter := rtr.GetRange(fdb.SelectorRange{Begin: beginSel.FDBKeySelector(), End: fdb.FirstGreaterOrEqual(end)}, fdb.RangeOptions{Limit: limit, Mode: fdb.StreamingModeWantAll})

		// If LATEST, we might want last key?
		// DynamoDB LATEST means "records written after this iterator is created".
		// So we return empty records and an iterator that points to "now".
		if data.IteratorType == "LATEST" {
			// For MVP, just return empty list + iterator pointing to "end"?
			// We need the key of the last item.
			lastKv, _ := rtr.GetRange(r, fdb.RangeOptions{Limit: 1, Reverse: true}).GetSliceWithError()
			if len(lastKv) > 0 {
				// Iterator -> AFTER_SEQUENCE_NUMBER of last item
				// But we need to extract versionstamp.
			}
			return []models.Record{}, nil
		}

		rows, err := iter.GetSliceWithError()
		if err != nil {
			return nil, err
		}

		params := []models.Record{}
		for _, row := range rows {
			var rec models.Record
			if err := json.Unmarshal(row.Value, &rec); err != nil {
				continue
			}

			// Extract Versionstamp from Key to set SequenceNumber
			// Key: [Prefix, Versionstamp]
			// Unpack key
			t, err := streamDir.Unpack(row.Key)
			if err == nil && len(t) > 0 {
				if vs, ok := t[0].(tuple.Versionstamp); ok {
					// Encode to Hex
					// TransactionVersion (10 bytes) + UserVersion (2 bytes)
					// BigEndian UserVersion
					uv := vs.UserVersion
					// Manually pack 12 bytes
					seqBytes := make([]byte, 12)
					copy(seqBytes[0:10], vs.TransactionVersion[:])
					seqBytes[10] = byte(uv >> 8)
					seqBytes[11] = byte(uv)
					rec.Dynamodb.SequenceNumber = fmt.Sprintf("%x", seqBytes)
					nextSeqNum = rec.Dynamodb.SequenceNumber
				}
			}

			params = append(params, rec)
		}
		return params, nil
	})

	if err != nil {
		return nil, "", err
	}

	records = res.([]models.Record)

	// Next Iterator
	// If we got records, next iterator is AFTER_SEQUENCE_NUMBER of last record.
	if len(records) > 0 {
		nextData := ShardIteratorData{
			StreamArn:    data.StreamArn,
			ShardId:      data.ShardId,
			IteratorType: "AFTER_SEQUENCE_NUMBER",
			SequenceNum:  nextSeqNum,
		}
		jb, _ := json.Marshal(nextData)
		return records, base64.StdEncoding.EncodeToString(jb), nil
	}

	// If no records, return same iterator? Or null?
	// DynamoDB returns next iterator to poll again.
	// If we are at end, we return same/updated iterator.
	return records, shardIterator, nil
}
