package expression

import (
	"fmt"
	"strings"

	"github.com/tabeth/concretedb/models"
)

// Operator represents a comparison operator
type Operator string

const (
	OpEqual              Operator = "="
	OpNotEqual           Operator = "<>"
	OpLessThan           Operator = "<"
	OpLessThanOrEqual    Operator = "<="
	OpGreaterThan        Operator = ">"
	OpGreaterThanOrEqual Operator = ">="
	OpBeginsWith         Operator = "begins_with"
	OpBetween            Operator = "BETWEEN"
	OpAttributeExists    Operator = "attribute_exists"
	OpAttributeNotExists Operator = "attribute_not_exists"
	OpContains           Operator = "contains"
	OpIn                 Operator = "IN"
)

// Condition represents a single comparison condition
type Condition struct {
	AttributeName string
	Operator      Operator
	Values        []models.AttributeValue
}

// Evaluator provides methods to evaluate conditions against items
type Evaluator struct{}

// NewEvaluator creates a new Evaluator
func NewEvaluator() *Evaluator {
	return &Evaluator{}
}

// EvaluateFilter parses (simplistically) the filter expression and checks the item.
func (e *Evaluator) EvaluateFilter(item map[string]models.AttributeValue, filterExpr string, exprNames map[string]string, exprValues map[string]models.AttributeValue) (bool, error) {
	if filterExpr == "" {
		return true, nil
	}

	// MVP: Split by " AND " and ensure all pass
	parts := strings.Split(filterExpr, " AND ")
	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Parse Condition
		cond, err := parseCondition(part, exprNames, exprValues)
		if err != nil {
			return false, err
		}

		match, err := e.evaluateCondition(item, cond)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

func parseCondition(part string, names map[string]string, values map[string]models.AttributeValue) (Condition, error) {
	// Identify Operator priority (longer strings first to avoid sub-match issues)
	ops := []Operator{
		OpAttributeNotExists, OpAttributeExists, OpBeginsWith, OpContains,
		OpLessThanOrEqual, OpGreaterThanOrEqual, OpEqual, OpNotEqual,
		OpLessThan, OpGreaterThan, OpBetween, OpIn,
	}
	var op Operator
	for _, o := range ops {
		if strings.Contains(part, string(o)) {
			op = o
			break
		}
	}
	if op == "" {
		return Condition{}, fmt.Errorf("unknown operator in: %s", part)
	}

	var lhsName string
	var rhsValues []models.AttributeValue

	if op == OpBeginsWith || op == OpContains || op == OpAttributeExists || op == OpAttributeNotExists {
		// Function style: func(arg1, arg2) or func(arg1)
		start := strings.Index(part, "(")
		end := strings.LastIndex(part, ")")
		if start < 0 || end < 0 || end <= start {
			return Condition{}, fmt.Errorf("malformed function call: %s", part)
		}
		inner := part[start+1 : end]
		args := strings.Split(inner, ",")

		lhsRaw := strings.TrimSpace(args[0])
		lhsName = resolveName(lhsRaw, names)

		if op == OpAttributeExists || op == OpAttributeNotExists {
			if len(args) != 1 {
				return Condition{}, fmt.Errorf("%s requires 1 arg", op)
			}
		} else {
			// begins_with, contains require 2 args
			if len(args) != 2 {
				return Condition{}, fmt.Errorf("%s requires 2 args", op)
			}
			rhsRaw := strings.TrimSpace(args[1])
			if v, ok := values[rhsRaw]; ok {
				rhsValues = []models.AttributeValue{v}
			} else {
				// Special case: contains check against raw string literals?
				// DynamoDB usually requires :val for everything. Sticking to that.
				return Condition{}, fmt.Errorf("missing value for %s", rhsRaw)
			}
		}
	} else if op == OpBetween {
		// lhs BETWEEN v1 AND v2
		sub := strings.SplitN(part, string(op), 2)
		lhsRaw := strings.TrimSpace(sub[0])
		lhsName = resolveName(lhsRaw, names)

		// This splitter is brittle if values contain " AND ", but typical usage uses placeholders :v1 AND :v2
		rhsPart := strings.TrimSpace(sub[1])
		bounds := strings.Split(rhsPart, " AND ")
		if len(bounds) != 2 {
			return Condition{}, fmt.Errorf("BETWEEN requires 2 values separated by AND")
		}

		for _, b := range bounds {
			b = strings.TrimSpace(b)
			if v, ok := values[b]; ok {
				rhsValues = append(rhsValues, v)
			} else {
				return Condition{}, fmt.Errorf("missing value for %s", b)
			}
		}

	} else {
		// Binary Op: lhs op rhs
		sub := strings.SplitN(part, string(op), 2)
		lhsRaw := strings.TrimSpace(sub[0])
		rhsRaw := strings.TrimSpace(sub[1])

		lhsName = resolveName(lhsRaw, names)
		if v, ok := values[rhsRaw]; ok {
			rhsValues = []models.AttributeValue{v}
		} else {
			return Condition{}, fmt.Errorf("missing value for %s", rhsRaw)
		}
	}

	return Condition{
		AttributeName: lhsName,
		Operator:      op,
		Values:        rhsValues,
	}, nil
}

func resolveName(raw string, names map[string]string) string {
	if strings.HasPrefix(raw, "#") {
		if n, ok := names[raw]; ok {
			return n
		}
	}
	return raw
}

func (e *Evaluator) evaluateCondition(item map[string]models.AttributeValue, cond Condition) (bool, error) {
	val, exists := item[cond.AttributeName]

	if cond.Operator == OpAttributeExists {
		return exists, nil
	}
	if cond.Operator == OpAttributeNotExists {
		return !exists, nil
	}

	// For other operators, if attribute missing, usually false (except potentially NOT NULL checks, but those are OpAttributeExists)
	if !exists {
		if cond.Operator == OpNotEqual {
			// update: "NOT Equal" to something usually implies it should match if it doesn't exist?
			// DynamoDB:
			// "The attribute does not exist" -> condition is false?
			// "If the attribute you are comparing does not exist, the result is false." (except for NULL checks)
			// Wait, if I say "status <> :active" and status is missing, is it true or false?
			// DynamoDB docs: "Inequality operator: The condition is satisfied if the attribute is not equal to the value."
			// BUT "If key attribute does not exist... method throws error".
			// "If the attribute does not exist ... result is false" generally applies.
			return false, nil
		}
		return false, nil
	}

	target := cond.Values[0]

	// Determine type
	if val.S != nil && target.S != nil {
		return compareStrings(*val.S, *target.S, cond.Operator)
	}
	if val.N != nil && target.N != nil {
		return compareNumbers(*val.N, *target.N, cond.Operator)
	}
	// Bool
	if val.BOOL != nil && target.BOOL != nil {
		if cond.Operator == OpEqual {
			return *val.BOOL == *target.BOOL, nil
		}
		if cond.Operator == OpNotEqual {
			return *val.BOOL != *target.BOOL, nil
		}
	}

	// Sets for CONTAINS
	if cond.Operator == OpContains {
		if val.SS != nil && target.S != nil {
			for _, s := range val.SS {
				if s == *target.S {
					return true, nil
				}
			}
			return false, nil
		}
		// TODO: NS, BS
	}

	return false, nil
}

func compareStrings(a, b string, op Operator) (bool, error) {
	switch op {
	case OpEqual:
		return a == b, nil
	case OpNotEqual:
		return a != b, nil
	case OpLessThan:
		return a < b, nil
	case OpLessThanOrEqual:
		return a <= b, nil
	case OpGreaterThan:
		return a > b, nil
	case OpGreaterThanOrEqual:
		return a >= b, nil
	case OpBeginsWith:
		return strings.HasPrefix(a, b), nil
	case OpContains:
		return strings.Contains(a, b), nil
	default:
		return false, fmt.Errorf("unsupported string op: %s", op)
	}
}

// ProjectItem filters the item attributes based on projection expression.
// MVP: Only supports top-level comma-separated attributes.
func (e *Evaluator) ProjectItem(item map[string]models.AttributeValue, expr string, names map[string]string) map[string]models.AttributeValue {
	if expr == "" {
		return item
	}

	newItem := make(map[string]models.AttributeValue)
	parts := strings.Split(expr, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		path := resolveName(part, names)

		if val, ok := item[path]; ok {
			newItem[path] = val
		}
	}
	return newItem
}

func compareNumbers(aStr, bStr string, op Operator) (bool, error) {
	var a, b float64
	fmt.Sscan(aStr, &a)
	fmt.Sscan(bStr, &b)

	switch op {
	case OpEqual:
		return a == b, nil
	case OpNotEqual:
		return a != b, nil
	case OpLessThan:
		return a < b, nil
	case OpLessThanOrEqual:
		return a <= b, nil
	case OpGreaterThan:
		return a > b, nil
	case OpGreaterThanOrEqual:
		return a >= b, nil
	default:
		return false, fmt.Errorf("unsupported number op: %s", op)
	}
}
