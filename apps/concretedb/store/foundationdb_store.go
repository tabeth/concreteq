package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
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
				r.Begin = fdb.FirstGreaterOrEqual(k)
				r.End = fdb.FirstGreaterThan(k)
			case "<":
				k, _ := getSKKey(skVals[0])
				r.End = fdb.FirstGreaterOrEqual(k)
			case "<=":
				k, _ := getSKKey(skVals[0])
				r.End = fdb.FirstGreaterThan(k)
			case ">":
				k, _ := getSKKey(skVals[0])
				r.Begin = fdb.FirstGreaterThan(k)
			case ">=":
				k, _ := getSKKey(skVals[0])
				r.Begin = fdb.FirstGreaterOrEqual(k)
			case "BETWEEN":
				if len(skVals) >= 2 {
					k1, _ := getSKKey(skVals[0])
					k2, _ := getSKKey(skVals[1])
					r.Begin = fdb.FirstGreaterOrEqual(k1)
					r.End = fdb.FirstGreaterThan(k2)
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
	fmt.Println("Creating table :", table.TableName)
	_, err := s.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// Simulating instant provisioning for ConcreteDB
		table.Status = models.StatusActive

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

	if returnValues == "ALL_OLD" {
		return oldItem, nil
	}
	return nil, nil
}

// GetItem retrieves an item by key.
func (s *FoundationDBStore) GetItem(ctx context.Context, tableName string, key map[string]models.AttributeValue, consistentRead bool) (map[string]models.AttributeValue, error) {
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
		// 1. Get Table Metadata
		table, err := s.getTableInternal(tr, tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, ErrTableNotFound
		}

		// 2. Extract Key Fields
		keyTuple, err := s.buildKeyTuple(table, key)
		if err != nil {
			return nil, err
		}

		// 3. Open Directory
		tableDir, err := s.dir.Open(tr, []string{"tables", tableName}, nil)
		if err != nil {
			return nil, err
		}
		itemKey := tableDir.Pack(append(tuple.Tuple{"data"}, keyTuple...))

		// 4. Read Existing Item
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

		// 5. Conditional Check
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

		// 6. Apply Update Expression (Simple implementation)
		if updateExpression != "" {
			// For MVP, simplistic parsing: "SET attr = :val"
			if err := applyUpdateExpression(item, updateExpression, exprAttrNames, exprAttrValues); err != nil {
				return nil, err
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

		// 8. Handle ReturnValues
		if returnValues == "ALL_OLD" {
			return oldItem, nil
		}
		if returnValues == "ALL_NEW" {
			return item, nil
		}
		// TODO: Implement UPDATED_OLD / UPDATED_NEW
		return nil, nil // NONE
	})

	if err != nil {
		return nil, err
	}
	if v, ok := val.(map[string]models.AttributeValue); ok {
		return v, nil
	}
	return nil, nil
}

// Simple expression parser for MVP
func applyUpdateExpression(item map[string]models.AttributeValue, expr string, names map[string]string, values map[string]models.AttributeValue) error {
	if len(expr) < 4 || expr[:4] != "SET " {
		return fmt.Errorf("only SET expressions supported in MVP")
	}

	body := expr[4:]
	assignments := strings.Split(body, ",")

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		parts := strings.Split(assignment, "=")
		if len(parts) != 2 {
			return fmt.Errorf("invalid assignment: %s", assignment)
		}

		lhs := strings.TrimSpace(parts[0])
		rhs := strings.TrimSpace(parts[1])

		// Resolve LHS (Attribute Name)
		realName := lhs
		if strings.HasPrefix(lhs, "#") {
			if name, ok := names[lhs]; ok {
				realName = name
			} else {
				return fmt.Errorf("missing expression attribute name: %s", lhs)
			}
		}

		// Resolve RHS (Value)
		var val models.AttributeValue
		if strings.HasPrefix(rhs, ":") {
			if v, ok := values[rhs]; ok {
				val = v
			} else {
				return fmt.Errorf("missing expression attribute value: %s", rhs)
			}
		} else {
			return fmt.Errorf("only literal values with ':' prefix supported in MVP")
		}

		item[realName] = val
	}

	return nil
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
