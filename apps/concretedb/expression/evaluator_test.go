package expression

import (
	"testing"

	"github.com/tabeth/concretedb/models"
)

func TestEvaluator_EvaluateFilter(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"name":     {S: strPtr("Alice")},
		"age":      {N: strPtr("30")},
		"isActive": {BOOL: boolPtr(true)},
	}

	tests := []struct {
		name      string
		filter    string
		names     map[string]string
		values    map[string]models.AttributeValue
		wantMatch bool
		wantErr   bool
	}{
		{
			name:      "EmptyFilter",
			filter:    "",
			wantMatch: true,
		},
		{
			name:      "SimpleEqual",
			filter:    "name = :n",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Alice")}},
			wantMatch: true,
		},
		{
			name:      "SimpleNotEqual",
			filter:    "name <> :n",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Bob")}},
			wantMatch: true,
		},
		{
			name:      "LessThan",
			filter:    "age < :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("40")}},
			wantMatch: true,
		},
		{
			name:      "GreaterThan",
			filter:    "age > :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("20")}},
			wantMatch: true,
		},
		{
			name:      "BeginsWith",
			filter:    "begins_with(name, :p)",
			values:    map[string]models.AttributeValue{":p": {S: strPtr("Al")}},
			wantMatch: true,
		},
		{
			name:      "AndCondition",
			filter:    "name = :n AND age > :a",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Alice")}, ":a": {N: strPtr("25")}},
			wantMatch: true,
		},
		{
			name:      "AndConditionFail",
			filter:    "name = :n AND age > :a",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Alice")}, ":a": {N: strPtr("35")}},
			wantMatch: false,
		},
		{
			name:      "BoolEqual",
			filter:    "isActive = :v",
			values:    map[string]models.AttributeValue{":v": {BOOL: boolPtr(true)}},
			wantMatch: true,
		},
		{
			name:      "BoolNotEqual",
			filter:    "isActive <> :v",
			values:    map[string]models.AttributeValue{":v": {BOOL: boolPtr(false)}},
			wantMatch: true,
		},
		{
			name:      "Alias",
			filter:    "#n = :v",
			names:     map[string]string{"#n": "name"},
			values:    map[string]models.AttributeValue{":v": {S: strPtr("Alice")}},
			wantMatch: true,
		},
		{
			name:      "MissingAttribute",
			filter:    "foo = :v",
			values:    map[string]models.AttributeValue{":v": {S: strPtr("bar")}},
			wantMatch: false,
		},
		{
			name:      "LessThanOrEqual",
			filter:    "age <= :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("30")}},
			wantMatch: true,
		},
		{
			name:      "GreaterThanOrEqual",
			filter:    "age >= :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("30")}},
			wantMatch: true,
		},
		{
			name:      "StringLessThan",
			filter:    "name < :n",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Bob")}},
			wantMatch: true,
		},
		{
			name:      "StringGreaterThanOrEqual",
			filter:    "name >= :n",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Alice")}},
			wantMatch: true,
		},
		{
			name:      "NotEqualNumber",
			filter:    "age <> :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("20")}},
			wantMatch: true,
		},
		{
			name:      "NotEqualStringFail",
			filter:    "name <> :n",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Alice")}},
			wantMatch: false,
		},
		{
			name:      "NumberLessThanFail",
			filter:    "age < :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("20")}},
			wantMatch: false,
		},
		{
			name:    "BetweenUnsupported_1",
			filter:  "age BETWEEN :v1 AND :v2",
			wantErr: true,
		},
		{
			name:      "NumberGreaterEqual_True",
			filter:    "age >= :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("25")}},
			wantMatch: true,
		},
		{
			name:      "NumberGreaterEqual_Equal",
			filter:    "age >= :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("30")}},
			wantMatch: true,
		},
		{
			name:      "NumberGreaterEqual_False",
			filter:    "age >= :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("35")}},
			wantMatch: false,
		},
		{
			name:      "NumberLessEqual_True",
			filter:    "age <= :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("35")}},
			wantMatch: true,
		},
		{
			name:      "NumberLessEqual_Equal",
			filter:    "age <= :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("30")}},
			wantMatch: true,
		},
		{
			name:      "NumberLessEqual_False",
			filter:    "age <= :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("25")}},
			wantMatch: false,
		},
		{
			name:    "UnknownOperator",
			filter:  "name IN :v",
			wantErr: true,
		},
		{
			name:      "StringLessThanOrEqual",
			filter:    "name <= :n",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Alice")}},
			wantMatch: true,
		},
		{
			name:      "StringGreaterThan",
			filter:    "name > :n",
			values:    map[string]models.AttributeValue{":n": {S: strPtr("Al")}},
			wantMatch: true,
		},
		{
			name:      "NumberNotEqualFail",
			filter:    "age <> :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("30")}},
			wantMatch: false,
		},
		{
			name:      "NumberGreater_Success",
			filter:    "age > :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("25")}},
			wantMatch: true,
		},
		{
			name:      "NumberLess_Success",
			filter:    "age < :a",
			values:    map[string]models.AttributeValue{":a": {N: strPtr("35")}},
			wantMatch: true,
		},
		{
			name:      "StringOpOnNumbers",
			filter:    "begins_with(age, :p)",
			values:    map[string]models.AttributeValue{":p": {S: strPtr("3")}},
			wantMatch: false, // Mixed types
		},
		{
			name:      "BeginsWithIncorrectArgType",
			filter:    "begins_with(name, :p)",
			values:    map[string]models.AttributeValue{":p": {N: strPtr("3")}},
			wantMatch: false,
		},
		{
			name:    "UnknownValue",
			filter:  "name = :unknown",
			wantErr: true,
		},
		{
			name:    "BeginsWithUnknownValue",
			filter:  "begins_with(name, :unknown)",
			wantErr: true,
		},
		{
			name:    "UnknownOperator",
			filter:  "name IN :v",
			wantErr: true,
		},
		{
			name:    "BetweenUnsupported",
			filter:  "age BETWEEN :v1 AND :v2",
			wantErr: true,
		},
		{
			name:    "MalformedBeginsWith",
			filter:  "begins_with(name",
			wantErr: true,
		},
		{
			name:      "ResolveName_MissingAlias",
			filter:    "#unknown = :v",
			names:     map[string]string{"#other": "name"},
			values:    map[string]models.AttributeValue{":v": {S: strPtr("Alice")}},
			wantMatch: false, // resolveName returns #unknown, which doesn't exist in item
		},
		{
			name:      "MixedTypes_S_N",
			filter:    "name = :v",
			values:    map[string]models.AttributeValue{":v": {N: strPtr("30")}},
			wantMatch: false,
		},
		{
			name:      "MixedTypes_Bool_S",
			filter:    "isActive = :v",
			values:    map[string]models.AttributeValue{":v": {S: strPtr("true")}},
			wantMatch: false,
		},
		{
			name:      "Bool_UnsupportedOp",
			filter:    "isActive < :v",
			values:    map[string]models.AttributeValue{":v": {BOOL: boolPtr(true)}},
			wantMatch: false,
		},
		{
			name:    "Parse_MissingValueNormal",
			filter:  "name = :missing",
			wantErr: true,
		},
		{
			name:    "Parse_MissingValueBeginsWith",
			filter:  "begins_with(name, :missing)",
			wantErr: true,
		},
		{
			name:    "Parse_BeginsWithMismatchedArgs",
			filter:  "begins_with(name, :v1, :v2)",
			wantErr: true,
		},
		{
			name:    "BeginsWithOnNumbers_Error",
			filter:  "begins_with(age, :p)",
			values:  map[string]models.AttributeValue{":p": {N: strPtr("3")}},
			wantErr: true, // compareNumbers returns error for begins_with
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := eval.EvaluateFilter(item, tt.filter, tt.names, tt.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("EvaluateFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantMatch {
				t.Errorf("EvaluateFilter() = %v, want %v", got, tt.wantMatch)
			}
		})
	}
}

func TestEvaluator_ProjectItem(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"name": {S: strPtr("Alice")},
		"age":  {N: strPtr("30")},
		"role": {S: strPtr("admin")},
	}

	tests := []struct {
		name     string
		expr     string
		names    map[string]string
		wantItem map[string]models.AttributeValue
	}{
		{
			name:     "EmptyProjection",
			expr:     "",
			wantItem: item,
		},
		{
			name: "SingleField",
			expr: "name",
			wantItem: map[string]models.AttributeValue{
				"name": {S: strPtr("Alice")},
			},
		},
		{
			name: "MultipleFields",
			expr: "name, age",
			wantItem: map[string]models.AttributeValue{
				"name": {S: strPtr("Alice")},
				"age":  {N: strPtr("30")},
			},
		},
		{
			name:  "WithAlias",
			expr:  "#n, role",
			names: map[string]string{"#n": "name"},
			wantItem: map[string]models.AttributeValue{
				"name": {S: strPtr("Alice")},
				"role": {S: strPtr("admin")},
			},
		},
		{
			name: "NonExistentField",
			expr: "name, foo",
			wantItem: map[string]models.AttributeValue{
				"name": {S: strPtr("Alice")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := eval.ProjectItem(item, tt.expr, tt.names)
			if len(got) != len(tt.wantItem) {
				t.Errorf("ProjectItem() length = %v, want %v", len(got), len(tt.wantItem))
			}
			for k, v := range tt.wantItem {
				gv, ok := got[k]
				if !ok {
					t.Errorf("ProjectItem() missing key %s", k)
					continue
				}
				if v.S != nil {
					if gv.S == nil || *gv.S != *v.S {
						t.Errorf("ProjectItem() key %s value mismatch", k)
					}
				} else if v.N != nil {
					if gv.N == nil || *gv.N != *v.N {
						t.Errorf("ProjectItem() key %s value mismatch", k)
					}
				}
			}
		})
	}
}

func TestEvaluator_Internal(t *testing.T) {
	e := NewEvaluator()

	// Test compareStrings unsupported op
	_, err := compareStrings("a", "b", "INVALID")
	if err == nil {
		t.Error("expected error for invalid string op")
	}

	// Test compareNumbers unsupported op
	_, err = compareNumbers("1", "2", "INVALID")
	if err == nil {
		t.Error("expected error for invalid number op")
	}

	// Test evaluateCondition with unsupported op for bool
	item := map[string]models.AttributeValue{"b": {BOOL: boolPtr(true)}}
	cond := Condition{AttributeName: "b", Operator: "<", Values: []models.AttributeValue{{BOOL: boolPtr(false)}}}
	match, err := e.evaluateCondition(item, cond)
	if err != nil {
		t.Errorf("expected no error for bool <, got %v", err)
	}
	if match {
		t.Error("expected match false for bool <")
	}

	// Test evaluateCondition with mismatched Types
	itemS := map[string]models.AttributeValue{"s": {S: strPtr("val")}}
	condN := Condition{AttributeName: "s", Operator: "=", Values: []models.AttributeValue{{N: strPtr("123")}}}
	match, err = e.evaluateCondition(itemS, condN)
	if match || err != nil {
		t.Error("expected match false and no error for mismatched types")
	}
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }
