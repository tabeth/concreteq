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
// MVP: Only supports single conditions like "age > :v" or "name begins_with :p".
// Complex logic (AND/OR) is NOT fully supported yet, but we can try to support simple AND.
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
			// For MVP, if we can't parse, we might log or fail.
			// Let's fail secure.
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
	// Identify Operator
	ops := []Operator{OpLessThanOrEqual, OpGreaterThanOrEqual, OpEqual, OpNotEqual, OpLessThan, OpGreaterThan, OpBeginsWith, OpBetween}
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

	// Parse LHS and RHS
	// "begins_with(lhs, rhs)" is special
	var lhsName string
	var rhsValues []models.AttributeValue

	if op == OpBeginsWith {
		// begins_with(path, substr)
		// Extract inside parens
		start := strings.Index(part, "(")
		end := strings.Index(part, ")")
		if start < 0 || end < 0 {
			return Condition{}, fmt.Errorf("malformed begins_with")
		}
		inner := part[start+1 : end]
		args := strings.Split(inner, ",")
		if len(args) != 2 {
			return Condition{}, fmt.Errorf("begins_with requires 2 args")
		}

		lhsRaw := strings.TrimSpace(args[0])
		rhsRaw := strings.TrimSpace(args[1])

		lhsName = resolveName(lhsRaw, names)
		if v, ok := values[rhsRaw]; ok {
			rhsValues = []models.AttributeValue{v}
		} else {
			return Condition{}, fmt.Errorf("missing value for %s", rhsRaw)
		}
	} else if op == OpBetween {
		// lhs BETWEEN v1 AND v2
		// This uses AND, so our top-level split might have broken it?
		// Assuming the caller handled the split logic or we reuse the logic.
		// For Scan/Filter, we probably have the same split issue.
		// Let's assume for now Filter doesn't use BETWEEN or we fix it later.
		// Or assume "v1 AND v2" are present.

		sub := strings.SplitN(part, string(op), 2)
		lhsRaw := strings.TrimSpace(sub[0])
		// rhsRaw := strings.TrimSpace(sub[1])

		lhsName = resolveName(lhsRaw, names)

		// Split RHS by " AND " ... risky if top level split ...
		// If top level split by " AND ", then `v1 AND v2` is broken.
		// We'll ignore BETWEEN for filters for this moment or implement same fix.
		return Condition{}, fmt.Errorf("BETWEEN not fully supported in Filter MVP")
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

	// Handle non-existent
	if !exists {
		// In DynamoDB, most comparisons with null/missing evaluate to false.
		// except attribute_not_exists (not implemented yet).
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
	// Bool?
	if val.BOOL != nil && target.BOOL != nil {
		if cond.Operator == OpEqual {
			return *val.BOOL == *target.BOOL, nil
		}
		if cond.Operator == OpNotEqual {
			return *val.BOOL != *target.BOOL, nil
		}
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
