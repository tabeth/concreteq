package expression

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tabeth/concretedb/models"
)

func TestApplyUpdate_Set(t *testing.T) {
	eval := NewEvaluator()

	// 1. SET path = value
	item := map[string]models.AttributeValue{
		"a": {N: newStr("1")},
	}
	expr := "SET b = :v"
	vals := map[string]models.AttributeValue{":v": {S: newStr("foo")}}

	changed, err := eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	assert.True(t, changed["b"])
	assert.Equal(t, "foo", *item["b"].S)

	// 2. SET path = path + value
	expr = "SET a = a + :n"
	vals = map[string]models.AttributeValue{":n": {N: newStr("2")}}
	changed, err = eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	assert.Equal(t, "3", *item["a"].N) // 1+2=3

	// 3. SET path = path - value
	expr = "SET a = a - :n"
	vals = map[string]models.AttributeValue{":n": {N: newStr("1")}}
	changed, err = eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	assert.Equal(t, "2", *item["a"].N) // 3-1=2

	// 4. list_append
	item["l"] = models.AttributeValue{L: []models.AttributeValue{{N: newStr("1")}}}
	expr = "SET l = list_append(l, :list)"
	vals = map[string]models.AttributeValue{":list": {L: []models.AttributeValue{{N: newStr("2")}}}}
	changed, err = eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	if assert.Len(t, item["l"].L, 2) {
		assert.Equal(t, "2", *item["l"].L[1].N)
	}

	// 5. if_not_exists (exists)
	expr = "SET a = if_not_exists(a, :def)"
	vals = map[string]models.AttributeValue{":def": {N: newStr("100")}}
	changed, err = eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	assert.Equal(t, "2", *item["a"].N) // Not changed

	// 6. if_not_exists (not exists)
	expr = "SET newAttr = if_not_exists(newAttr, :def)"
	vals = map[string]models.AttributeValue{":def": {N: newStr("100")}}
	changed, err = eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	assert.Equal(t, "100", *item["newAttr"].N)
}

func TestApplyUpdate_Remove(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"a": {N: newStr("1")},
		"b": {S: newStr("foo")},
	}
	expr := "REMOVE a, b"
	changed, err := eval.ApplyUpdate(item, expr, nil, nil)
	assert.NoError(t, err)
	assert.True(t, changed["a"])
	assert.True(t, changed["b"])
	_, ok := item["a"]
	assert.False(t, ok)
}

func TestApplyUpdate_Add(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"n": {N: newStr("1")},
		"s": {SS: []string{"a"}},
	}

	// ADD number
	expr := "ADD n :v"
	vals := map[string]models.AttributeValue{":v": {N: newStr("5")}}
	_, err := eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	assert.Equal(t, "6", *item["n"].N)

	// ADD set
	expr = "ADD s :ss"
	vals = map[string]models.AttributeValue{":ss": {SS: []string{"b", "a"}}} // 'a' duplicate
	_, err = eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	assert.Len(t, item["s"].SS, 2) // a, b (order not guaranteed but 2 items)
}

func TestApplyUpdate_Delete(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"s": {SS: []string{"a", "b", "c"}},
	}
	expr := "DELETE s :ss"
	vals := map[string]models.AttributeValue{":ss": {SS: []string{"a", "c"}}}
	_, err := eval.ApplyUpdate(item, expr, nil, vals)
	assert.NoError(t, err)
	assert.Len(t, item["s"].SS, 1)
	assert.Equal(t, "b", item["s"].SS[0])
}

// Helpers
func newStr(s string) *string { return &s }

func TestUpdateParser_Errors(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}

	tests := []struct {
		name string
		expr string
		err  string
	}{
		{"EmptySET", "SET", "expected identifier"}, // EOF hit during path parse
		{"MissingEq", "SET a", "expected '='"},
		{"MissingValue", "SET a =", "expected identifier"},
		{"BadRemove", "REMOVE", "expected identifier"},
		{"BadAdd", "ADD a", "expected identifier"},
		{"BadDelete", "DELETE", "expected identifier"},
		{"Garbage", "GARBAGE", "unexpected token"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eval.ApplyUpdate(item, tc.expr, nil, nil)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
		})
	}
}

func TestUpdateCoverage_Stringify(t *testing.T) {
	// Cover AST String() methods
	set := &SetAction{Path: &PathNode{}, Value: &LiteralNode{Value: ":v"}}
	assert.NotEmpty(t, set.String())

	rem := &RemoveAction{Path: &PathNode{}}
	assert.NotEmpty(t, rem.String())

	add := &AddAction{Path: &PathNode{}, Value: &LiteralNode{}}
	assert.NotEmpty(t, add.String())

	del := &DeleteAction{Path: &PathNode{}, Value: &LiteralNode{}}
	assert.NotEmpty(t, del.String())

	update := &UpdateNode{SetActions: []*SetAction{set}}
	assert.NotEmpty(t, update.String())
}

func TestUpdateCoverage_EdgeCases(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{"a": {N: newStr("1")}}

	// Unsupported nested SET index
	// To hit "list indexing not supported", we need to traverse to the last part.
	// Current SetValue implementation requires intermediate parts to be Maps.
	itemMap := map[string]models.AttributeValue{"a": {M: map[string]models.AttributeValue{}}}
	_, err := eval.ApplyUpdate(itemMap, "SET a[1] = :v", nil, map[string]models.AttributeValue{":v": {N: newStr("1")}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "list indexing in SET target not supported")

	// Arithmetic type mismatch (via SET, not ADD)
	// ADD handles mismatch internally. arithmetic error comes from resolveUpdateValue (binary, math)
	item = map[string]models.AttributeValue{"a": {N: newStr("1")}}
	_, err = eval.ApplyUpdate(item, "SET a = a + :v", nil, map[string]models.AttributeValue{":v": {S: newStr("s")}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "arithmetic requires Number types")
}
