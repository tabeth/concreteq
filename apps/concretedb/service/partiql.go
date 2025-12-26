package service

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/tabeth/concretedb/models"
)

// PartiQLOperation represents a parsed PartiQL operation
type PartiQLOperation struct {
	Type          string // "SELECT", "INSERT", "UPDATE", "DELETE"
	TableName     string
	Key           map[string]models.AttributeValue // For Get/Delete/Update item
	Item          map[string]models.AttributeValue // For Put/Insert item
	UpdateExpr    string                           // For Update
	ConditionExpr string
	FilterExpr    string
	Projection    string
	Limit         int32
	Params        []models.AttributeValue
}

// ParsePartiQL parses a subset of SQL for DynamoDB compatibility.
// Supported:
// SELECT * FROM table WHERE pk=? AND sk=?
// INSERT INTO table VALUE {'pk': ?, 'attr': ?}
// UPDATE table SET attr=? WHERE pk=?
// DELETE FROM table WHERE pk=?
func ParsePartiQL(statement string, params []models.AttributeValue) (*PartiQLOperation, error) {
	stmt := strings.TrimSpace(statement)
	upperStmt := strings.ToUpper(stmt)

	if strings.HasPrefix(upperStmt, "SELECT") {
		return parseSelect(statement, params)
	} else if strings.HasPrefix(upperStmt, "INSERT") {
		return parseInsert(statement, params)
	} else if strings.HasPrefix(upperStmt, "UPDATE") {
		return parseUpdate(statement, params)
	} else if strings.HasPrefix(upperStmt, "DELETE") {
		return parseDelete(statement, params)
	}

	return nil, fmt.Errorf("unsupported statement type")
}

// Very basic regex-based parser. In a real implementation this would be a proper lexical parser.
var (
	selectRegex = regexp.MustCompile(`(?i)SELECT\s+(.+)\s+FROM\s+"?([a-zA-Z0-9_\-\.]+)"?(?:\s+WHERE\s+(.+))?`)
	insertRegex = regexp.MustCompile(`(?i)INSERT\s+INTO\s+"?([a-zA-Z0-9_\-\.]+)"?\s+VALUE\s+(.+)`)
	updateRegex = regexp.MustCompile(`(?i)UPDATE\s+"?([a-zA-Z0-9_\-\.]+)"?\s+SET\s+(.+?)\s+WHERE\s+(.+)`)
	deleteRegex = regexp.MustCompile(`(?i)DELETE\s+FROM\s+"?([a-zA-Z0-9_\-\.]+)"?\s+WHERE\s+(.+)`)
)

func parseSelect(stmt string, params []models.AttributeValue) (*PartiQLOperation, error) {
	matches := selectRegex.FindStringSubmatch(stmt)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid SELECT syntax")
	}

	projection := strings.TrimSpace(matches[1])
	tableName := strings.TrimSpace(matches[2])
	whereClause := ""
	if len(matches) > 3 {
		whereClause = strings.TrimSpace(matches[3])
	}

	op := &PartiQLOperation{
		Type:       "SELECT",
		TableName:  tableName,
		Params:     params,
		Projection: projection,
	}

	if whereClause != "" {
		// Just passing the raw where clause as KeyCondition for simplification in this mock parser.
		// A proper parser would break this down into keys vs filters.
		// For now, assume simple equivalence.
		op.Key = parseKeyCondition(whereClause, params)
	}

	return op, nil
}

func parseInsert(stmt string, params []models.AttributeValue) (*PartiQLOperation, error) {
	matches := insertRegex.FindStringSubmatch(stmt)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}

	tableName := strings.TrimSpace(matches[1])
	valueStr := strings.TrimSpace(matches[2])

	// Mocking JSON parsing of the VALUE object
	item, err := parseJSONItem(valueStr, params)
	if err != nil {
		return nil, err
	}

	return &PartiQLOperation{
		Type:      "INSERT",
		TableName: tableName,
		Item:      item,
		Params:    params,
	}, nil
}

func parseUpdate(stmt string, params []models.AttributeValue) (*PartiQLOperation, error) {
	matches := updateRegex.FindStringSubmatch(stmt)
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}

	tableName := strings.TrimSpace(matches[1])
	setClause := strings.TrimSpace(matches[2])
	whereClause := strings.TrimSpace(matches[3])

	key := parseKeyCondition(whereClause, params)

	return &PartiQLOperation{
		Type:       "UPDATE",
		TableName:  tableName,
		Key:        key,
		UpdateExpr: "SET " + setClause, // Simplified
		Params:     params,
	}, nil
}

func parseDelete(stmt string, params []models.AttributeValue) (*PartiQLOperation, error) {
	matches := deleteRegex.FindStringSubmatch(stmt)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid DELETE syntax")
	}

	tableName := strings.TrimSpace(matches[1])
	whereClause := strings.TrimSpace(matches[2])

	key := parseKeyCondition(whereClause, params)

	return &PartiQLOperation{
		Type:      "DELETE",
		TableName: tableName,
		Key:       key,
		Params:    params,
	}, nil
}

// Helpers

func parseKeyCondition(clause string, params []models.AttributeValue) map[string]models.AttributeValue {
	// Simple key parser: "pk = '123' AND sk = 456"
	key := make(map[string]models.AttributeValue)

	// Split by AND case-insensitive
	parts := regexp.MustCompile(`(?i)\s+AND\s+`).Split(clause, -1)
	paramIdx := 0

	for _, part := range parts {
		if idx := strings.Index(part, "="); idx != -1 {
			k := strings.TrimSpace(part[:idx])
			vStr := strings.TrimSpace(part[idx+1:])

			// Extract key name (remove quotes if any)
			k = strings.Trim(k, "\"'")

			var val models.AttributeValue
			if vStr == "?" {
				if paramIdx < len(params) {
					val = params[paramIdx]
					paramIdx++
				}
			} else {
				// Naive value parsing
				if strings.HasPrefix(vStr, "'") {
					s := strings.Trim(vStr, "'")
					val = models.AttributeValue{S: &s}
				} else {
					val = models.AttributeValue{N: &vStr}
				}
			}
			key[k] = val
		}
	}
	return key
}

func parseJSONItem(jsonStr string, params []models.AttributeValue) (map[string]models.AttributeValue, error) {
	// Extremely simplified parser that expects "{'key': 'value'}" format or replacement with params
	// A real implementation would use a proper JSON parser that handles PartiQL extended JSON.

	// If the entire value is a parameter ?
	if strings.TrimSpace(jsonStr) == "?" {
		if len(params) > 0 && params[0].M != nil {
			return params[0].M, nil
		}
		return nil, fmt.Errorf("expected Map parameter for INSERT")
	}

	return nil, fmt.Errorf("only parameter based INSERT supported in this mockup")
}
