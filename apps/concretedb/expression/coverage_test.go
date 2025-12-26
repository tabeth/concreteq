package expression

import (
	"testing"

	"github.com/tabeth/concretedb/models"
)

// TestCoverage_Internals targets specific functions and branches that are hard to reach
// via the public API or represent defensive coding.
func TestCoverage_Internals(t *testing.T) {
	// 1. AST Node Types (Trivial getters)
	// Coverage for Type() methods in ast.go
	nodes := []Node{
		&BinaryNode{},
		&UnaryNode{},
		&FunctionNode{},
		&LiteralNode{},
		&PathNode{},
	}
	for _, n := range nodes {
		_ = n.Type()
		_ = n.String() // Cover String() formatting
	}

	// 2. Lexer String()
	// Coverage for token string representations
	tokens := []Token{
		{Type: TokenEOF, Literal: "EOF"},
		{Type: TokenError, Literal: "ERR"},
	}
	for _, tok := range tokens {
		_ = tok.String()
	}

	// 3. Evaluator Internals - compareStrings / compareNumbers
	// Trigger unsupported operators
	_, err := compareStrings("a", "b", TokenAND) // Invalid op for string
	if err == nil {
		t.Error("expected error for invalid string op")
	}

	_, err = compareNumbers("1", "2", TokenAND) // Invalid op for number
	if err == nil {
		t.Error("expected error for invalid number op")
	}

	// Trigger invalid number parsing in compareNumbers (if possible via internal call)
	_, err = compareNumbers("invalid", "2", TokenEq)
	// Sscan might not error but return 0?
	// fmt.Sscan matches. If it fails, checks err?
	// The implementation ignores Sscan error: "fmt.Sscan(aStr, &a)".
	// So "invalid" becomes 0.

	// 4. Resolving Values - Edge cases
	eval := NewEvaluator()
	// resolveValue with unknown node type (mock node)
	_, err = eval.resolveValue(&mockNode{}, nil, nil, nil)
	// resolveValue now returns error for unknown types
	if err == nil {
		t.Error("resolveValue with unknown node should error")
	}

	// 5. evalNode unknown type
	// The switch in evalNode has a default: return false, fmt.Errorf("unknown node type")
	_, err = eval.evalNode(&mockNode{}, nil, nil, nil)
	if err == nil {
		t.Error("evalNode should error on unknown node type")
	}

	// 6. checkBetween edge cases
	// Mixed types were covered in integ test, but ensure straight call works
	valN := &models.AttributeValue{N: strPtr("5")}
	lowN := &models.AttributeValue{N: strPtr("1")}
	highN := &models.AttributeValue{N: strPtr("10")}

	ok, _ := checkBetween(valN, lowN, highN)
	if !ok {
		t.Error("5 between 1, 10")
	}

	// Nil checks? Caller guarantees non-nil.
	// But mixed types should fail.
	valS := &models.AttributeValue{S: strPtr("foo")}
	ok, _ = checkBetween(valN, valS, highN)
	if ok {
		t.Error("Between with mixed types should fail")
	}

}

// Mock Node to trigger default cases
type mockNode struct{}

func (m *mockNode) Type() NodeType { return NodeType(-1) }
func (m *mockNode) String() string { return "mock" }

func TestCoverage_Lexer(t *testing.T) {
	input := "foo.bar[1] = 12.34 AND size(:v) <> :val"
	l := NewLexer(input)

	// Iterate tokens
	for _, tok := range l.tokens {
		if tok.Type == TokenError {
			t.Errorf("Unexpected error token: %s", tok.Literal)
		}
	}

	// Test lexNumber float path specifically
	l2 := NewLexer("3.14")
	if len(l2.tokens) > 0 && l2.tokens[0].Literal != "3.14" {
		t.Errorf("Expected 3.14, got %s", l2.tokens[0].Literal)
	}

	// Test lexIdentifier with digits
	l3 := NewLexer("v1")
	if len(l3.tokens) > 0 && l3.tokens[0].Literal != "v1" {
		t.Errorf("Expected v1, got %s", l3.tokens[0].Literal)
	}

	// Test lexValue with underscore
	l4 := NewLexer(":v_1")
	if len(l4.tokens) > 0 && l4.tokens[0].Literal != ":v_1" {
		t.Errorf("Expected :v_1, got %s", l4.tokens[0].Literal)
	}
}

func TestCoverage_Functions_EdgeCases(t *testing.T) {
	eval := NewEvaluator()
	// contains(NS, val) -> not supported yet -> returns false (coverage line 168)
	// We need to construct an item with NS
	// contains(NS, val) -> not supported yet -> returns false (coverage line 168)
	// We need to construct an item with NS
	// NS in models is []string

	// We can't easily trigger this via EvaluateFilter(expr) because resolving NS might be tricky
	// or valid. Let's try direct call if possible, or careful filter.
	// "contains(ns, :v)" where ns is NS.

	item := map[string]models.AttributeValue{
		"ns": {NS: []string{"1", "2"}},
		"n":  {N: strPtr("1")},
	}
	vals := map[string]models.AttributeValue{":v": {S: strPtr("1")}}

	// contains(ns, :v)
	match, err := eval.EvaluateFilter(item, "contains(ns, :v)", nil, vals)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if match {
		t.Error("NS contains not supported yet, should be false")
	}

	// begins_with(n, :v) -> n is number -> return false (line 134)
	match, err = eval.EvaluateFilter(item, "begins_with(n, :v)", nil, vals)
	// The implementation checks if v1.S == nil -> false.
	// 134: if err != nil || v1 == nil || v1.S == nil { return false, nil }
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if match {
		t.Error("begins_with number should return false")
	}
}

func TestCoverage_FinalPush(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"l": {L: []models.AttributeValue{{S: strPtr("x")}}}, // size 1
		"n": {N: strPtr("1")},
		"x": {N: strPtr("10")},
	}
	vals := map[string]models.AttributeValue{
		":v": {N: strPtr("1")},
	}

	// 1. Size as RHS operand
	// "n = size(l)"
	// parseCondition: n -> Path (Left). = -> Op. size(l) -> parseOperand calls size logic.
	match, err := eval.EvaluateFilter(item, "n = size(l)", nil, vals)
	if err != nil {
		t.Errorf("n=size(l) error: %v", err)
	}
	if !match {
		t.Error("n=size(l) should match")
	}

	// Verify parseOperand size error path: size(size(x)) -> "expected '(' after size" if malformed?
	// or size() empty?
	// "n = size)" -> parser error.
	_, err = eval.EvaluateFilter(item, "n = size", nil, vals) // "expected '(' after size"
	if err == nil {
		t.Error("expected error for malformed size")
	}

	// 2. IN with missing value
	// "start IN (:v, :missing)"
	// resolveValue(:missing) -> error.
	// evalFunction IN should continue?
	// The implementation:
	// 202: if err != nil { continue }
	// So it ignores errors and checks next?
	// If :v matches, true.
	match, err = eval.EvaluateFilter(item, "n IN (:missing, :v)", nil, vals)
	if err != nil {
		t.Errorf("IN with missing arg error: %v", err)
	}
	if !match {
		t.Error("IN should match :v even if :missing errors")
	}

	// 3. resolveName direct
	// We can't call it directly (unexported), but we can trigger it.
	// We covered #alias -> resolved.
	// We covered #missing -> raw.
	// We covered prop -> raw.
	// Maybe ensuring coverage counts?
	// EvaluateFilter with names map.
	names := map[string]string{"#a": "n"}
	match, _ = eval.EvaluateFilter(item, "#a = :v", names, vals)
	if !match {
		t.Error("Alias resolution failed")
	}
}

func TestCoverage_ManualAST(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}

	// 1. resolveValue with non-size function
	// "unknown" function as value
	fnNode := &FunctionNode{Name: "unknown", Arguments: []Node{}}
	_, err := eval.resolveValue(fnNode, item, nil, nil)
	if err == nil {
		t.Error("resolveValue should error for unknown function name")
	}

	// 2. resolveValue with empty PathNode (impossible via parser)
	pathNode := &PathNode{Parts: []PathPart{}}
	val, err := eval.resolveValue(pathNode, item, nil, nil)
	// resolveValue returns nil, nil for empty path? Or error?
	// Implementation: if len(parts)==0 return nil, nil
	if val != nil || err != nil {
		t.Error("resolveValue empty path should return nil, nil")
	}

	// 3. evalNode with LiteralNode (not a condition)
	litNode := &LiteralNode{Value: ":v"}
	_, err = eval.evalNode(litNode, item, nil, nil)
	// evalNode default case -> error
	if err == nil {
		t.Error("evalNode should error for LiteralNode")
	}

	// 4. evalNode with PathNode (path as condition, e.g. "foo")
	// If "foo" exists and is bool? DDB supports implicit truthiness?
	// Project says: "Could just be standalone boolean path".
	// Implementation in evalNode:
	// case *PathNode:
	// 	  val := resolveValue...
	//    if bool, return bool.

	// Test PathNode evaluating to true/false/error
	itemBool := map[string]models.AttributeValue{
		"isTrue":  {BOOL: boolPtr(true)},
		"isFalse": {BOOL: boolPtr(false)},
		"notBool": {S: strPtr("s")},
	}

	// True
	pTrue := &PathNode{Parts: []PathPart{{Name: "isTrue", Index: -1}}}
	match, err := eval.evalNode(pTrue, itemBool, nil, nil)
	if err != nil || !match {
		t.Error("evalNode path=true failed")
	}

	// False
	pFalse := &PathNode{Parts: []PathPart{{Name: "isFalse", Index: -1}}}
	match, err = eval.evalNode(pFalse, itemBool, nil, nil)
	if err != nil || match {
		t.Error("evalNode path=false failed")
	}

	// Not Bool
	pString := &PathNode{Parts: []PathPart{{Name: "notBool", Index: -1}}}
	_, err = eval.evalNode(pString, itemBool, nil, nil)
	if err == nil {
		t.Error("evalNode path=string should error")
	}

	// Missing
	pMiss := &PathNode{Parts: []PathPart{{Name: "missing", Index: -1}}}
	match, err = eval.evalNode(pMiss, itemBool, nil, nil)
	if err != nil || match {
		t.Error("evalNode path=missing should be false (falsy)? Or error?")
	}
	// Implementation: resolveValue returns nil -> false?
	// If resolveValue returns nil, evalNode path checks val == nil?
	// Let's assume implementation treats nil as false or error.

	// 5. BinaryNode with invalid operator (impossible via Parser)
	// Parser ensures Operator is TokenEq...
	// Construct BinaryNode with TokenAND (invalid for binary comparison node usually? No, AND is	// Invalid comparison op
	// BinaryNode with Op=999
	binNode := &BinaryNode{
		Left:     &LiteralNode{Value: ":a"},
		Right:    &LiteralNode{Value: ":b"},
		Operator: Token{Type: 999, Literal: "???"},
	}
	// Needs values
	vals := map[string]models.AttributeValue{
		":a": {S: strPtr("a")},
		":b": {S: strPtr("b")},
	}
	_, err = eval.evalNode(binNode, item, nil, vals)
	// compareStrings error
	if err == nil {
		t.Error("Invalid comparison op should error")
	}
}

func TestCoverage_Parser_DirectEmpty(t *testing.T) {
	// calling Parse with empty tokens or EOF
	p := NewParser([]Token{})
	n, err := p.Parse()
	if err != nil || n != nil {
		t.Error("Empty parser should return nil, nil")
	}

	p = NewParser([]Token{{Type: TokenEOF}})
	n, err = p.Parse()
	if err != nil || n != nil {
		t.Error("EOF parser should return nil, nil")
	}
}

func TestCoverage_Filter_Whitespace(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}
	// "   " -> Lexer emits only EOF. Parse returns nil. EvaluateFilter returns true.
	match, err := eval.EvaluateFilter(item, "   ", nil, nil)
	if err != nil {
		t.Errorf("Whitespace filter error: %v", err)
	}
	if !match {
		t.Error("Whitespace filter should match (empty)")
	}
}

func TestCoverage_CompareValues(t *testing.T) {
	// Direct testing of compareAttributeValues
	sVal := &models.AttributeValue{S: strPtr("s")}
	nVal := &models.AttributeValue{N: strPtr("1")}

	// 1. Nil checks
	// One nil, NE -> true
	match, err := compareAttributeValues(sVal, nil, TokenNE)
	if err != nil || !match {
		t.Error("NE with nil should be true")
	}

	// One nil, EQ -> false
	match, err = compareAttributeValues(sVal, nil, TokenEq)
	if err != nil || match {
		t.Error("EQ with nil should be false")
	}

	// both nil? resolveValue returns nil, nil.
	match, err = compareAttributeValues(nil, nil, TokenEq)
	// (nil == nil) logic? Code: if lhs==nil || rhs==nil { return false usually unless NE }
	// If both nil? It enters if lhs==nil || rhs==nil.
	// if op==NE -> true. (nil != nil? No, nil == nil).
	// But current logic: return true if NE.
	if err != nil || match {
		t.Error("EQ with both nil should be false")
	}
	// Code:
	// if lhs == nil || rhs == nil {
	//    if op == TokenNE { return true, nil }
	//    return false, nil
	// }
	// So nil == nil -> false.

	// 2. Type Mismatch
	// S vs N
	match, err = compareAttributeValues(sVal, nVal, TokenEq)
	if err != nil || match {
		t.Error("S vs N should be false")
	}

	// BOOL vs BOOL
	bTrue := &models.AttributeValue{BOOL: boolPtr(true)}
	bFalse := &models.AttributeValue{BOOL: boolPtr(false)}

	match, _ = compareAttributeValues(bTrue, bFalse, TokenEq)
	if match {
		t.Error("True == False should be false")
	}

	match, _ = compareAttributeValues(bTrue, bFalse, TokenNE)
	if !match {
		t.Error("True != False should be true")
	}
}

func TestCoverage_Lexer_Symbols(t *testing.T) {
	// Cover all switch cases in lexText
	input := "= <> < <= > >= ( ) , . [ ]"
	l := NewLexer(input)
	// Iterate to ensure all emitted
	count := 0
	for _, tok := range l.tokens {
		if tok.Type != TokenEOF {
			count++
		}
	}
	// 12 tokens
	if count < 12 {
		t.Errorf("Expected at least 12 symbol tokens, got %d", count)
	}
}

func TestCoverage_SizeMissing(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}
	vals := map[string]models.AttributeValue{":z": {N: strPtr("0")}}

	// size(missing) = :z (0)
	// My implementation returns nil for size(missing).
	// So nil == 0 -> false.
	// So size(missing) is not 0.

	match, err := eval.EvaluateFilter(item, "size(missing) = :z", nil, vals)
	if err != nil {
		t.Errorf("size(missing) error: %v", err)
	}
	if match {
		// If it matches, then size(missing) returned 0?
		// Code: if val == nil { return nil, nil }
		t.Error("size(missing) should be nil/false")
	}
}

func TestCoverage_MissingPlaceholder(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}
	// :v used but not provided
	_, err := eval.EvaluateFilter(item, ":v = :v", nil, nil)
	if err == nil {
		t.Error("Should error on missing placeholder value")
	}
}

func TestCoverage_Unreachable_EvalFunction(t *testing.T) {
	// Directly call evalNode/evalFunction with malformed nodes that Parser prevents
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}

	// BETWEEN with wrong args
	// Parser ensures 3 args (left, start, end).
	node := &FunctionNode{Name: "BETWEEN", Arguments: []Node{}}
	_, err := eval.evalNode(node, item, nil, nil)
	if err == nil {
		t.Error("BETWEEN with 0 args should error")
	}

	// attribute_exists with wrong args
	node = &FunctionNode{Name: "attribute_exists", Arguments: []Node{}}
	_, err = eval.evalNode(node, item, nil, nil)
	if err == nil {
		t.Error("attribute_exists with 0 args should error")
	}

	// attribute_not_exists with wrong args
	node = &FunctionNode{Name: "attribute_not_exists", Arguments: []Node{}}
	_, err = eval.evalNode(node, item, nil, nil)
	if err == nil {
		t.Error("attribute_not_exists with 0 args should error")
	}

	// begins_with with wrong args
	node = &FunctionNode{Name: "begins_with", Arguments: []Node{}}
	_, err = eval.evalNode(node, item, nil, nil)
	if err == nil {
		t.Error("begins_with with 0 args should error")
	}

	// contains with wrong args
	node = &FunctionNode{Name: "contains", Arguments: []Node{}}
	_, err = eval.evalNode(node, item, nil, nil)
	if err == nil {
		t.Error("contains with 0 args should error")
	}

	// IN with 1 arg (Parser ensures >= 2)
	node = &FunctionNode{Name: "IN", Arguments: []Node{&LiteralNode{Value: ":v"}}}
	_, err = eval.evalNode(node, item, nil, nil)
	if err == nil {
		t.Error("IN with 1 arg should error")
	}
}

func TestCoverage_Functions_UnsupportedTypes(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"n":    {N: strPtr("10")},
		"s":    {S: strPtr("str")},
		"bool": {BOOL: boolPtr(true)},
	}
	// begins_with(n, s) -> error/false (v1 not string)
	// Code: if v1.S == nil { return false }
	match, err := eval.EvaluateFilter(item, "begins_with(n, :s)", nil, map[string]models.AttributeValue{":s": {S: strPtr("s")}})
	if err != nil {
		t.Error("begins_with(n,s) should not error, just false")
	}
	if match {
		t.Error("begins_with(number, string) should be false")
	}

	// begins_with(s, n) -> error/false (v2 not string)
	match, err = eval.EvaluateFilter(item, "begins_with(s, :n)", nil, map[string]models.AttributeValue{":n": {N: strPtr("1")}})
	if err != nil {
		t.Error("begins_with(s,n) should not error, just false")
	}
	if match {
		t.Error("begins_with(string, number) should be false")
	}

	// contains(n, s) -> false (container not S/SS)
	match, err = eval.EvaluateFilter(item, "contains(n, :s)", nil, map[string]models.AttributeValue{":s": {S: strPtr("s")}})
	if err != nil {
		t.Error("contains(number, string) should not error, just false")
	}
	if match {
		t.Error("contains(number, string) should be false")
	}

	// contains(s, n) -> false (element not string? vs string contains string)
	// contains(string, number) -> false
	match, err = eval.EvaluateFilter(item, "contains(s, :n)", nil, map[string]models.AttributeValue{":n": {N: strPtr("1")}})
	if err != nil {
		t.Error("contains(string, number) should not error, just false")
	}
	if match {
		t.Error("contains(string, number) should be false")
	}

	// size(bool) -> unsupported type error
	// Code: size() not supported for type -> error
	_, err = eval.EvaluateFilter(item, "size(bool) = :z", nil, map[string]models.AttributeValue{":z": {N: strPtr("0")}})
	if err == nil {
		t.Error("size(bool) should error")
	}
	// Expect error "size() not supported for type" or similar.
}

func TestCoverage_Between_Mismatch(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"n": {N: strPtr("10")},
		"s": {S: strPtr("low")},
	}
	// BETWEEN(n, s, n) -> Mismatch types N, S, N
	// Should return false (no error)
	match, err := eval.EvaluateFilter(item, "n BETWEEN :s AND :n", nil, map[string]models.AttributeValue{
		":s": {S: strPtr("low")},
		":n": {N: strPtr("20")},
	})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if match {
		t.Error("BETWEEN mismatch should be false")
	}
}

func TestCoverage_SizeEmpty(t *testing.T) {
	// Hits parseFunctionCall empty args branch
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}
	vals := map[string]models.AttributeValue{":z": {N: strPtr("0")}}

	// size() fails evaluator (requires 1 arg) but passes parser?
	// parseFunctionCall handles empty args.
	_, err := eval.EvaluateFilter(item, "size() = :z", nil, vals)
	// Evaluator error: size() requires 1 arg.
	if err == nil {
		t.Error("size() should error in evaluator (but pass parser)")
	}
}

func TestCoverage_Parser_Helpers(t *testing.T) {
	// Manual parser helper coverage
	l := NewLexer("a b")
	p := NewParser(l.tokens)

	// Test peek at start
	if p.peek().Literal != "a" {
		t.Error("peek failed")
	}
	// peekPos() returns int?
	// check parser definition: func (p *Parser) peekPos() int { return p.pos }
	if p.peekPos() != 0 {
		t.Error("peekPos failed, expected 0")
	}

	// Advance
	p.advance()
	if p.previous().Literal != "a" {
		t.Error("previous failed")
	}

	// Advance to end
	p.advance() // b
	p.advance() // EOF

	if !p.isAtEnd() {
		t.Error("should be at end")
	}
	if p.peek().Type != TokenEOF {
		t.Error("peek at end should be EOF")
	}
}

func TestCoverage_Parser_Errors(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}

	// Missing comma in function
	// begins_with(a b)
	_, err := eval.EvaluateFilter(item, "begins_with(a b)", nil, nil)
	if err == nil {
		t.Error("expected error for missing comma")
	}

	// Missing closing paren in function
	// begins_with(a, b
	_, err = eval.EvaluateFilter(item, "begins_with(a, b", nil, nil)
	if err == nil {
		t.Error("expected error for missing rparen")
	}
}

func TestCoverage_AST_Manual(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}

	// 1. evalNode with non-boolean node (LiteralNode)
	// Default case in evalNode switch
	badNode := &LiteralNode{Value: "foo"}
	_, err := eval.evalNode(badNode, item, nil, nil)
	if err == nil {
		t.Error("evalNode with LiteralNode should error")
	}

	// 2. evalFunction with unknown function name
	// Default case in evalFunction switch
	funcNode := &FunctionNode{Name: "unknown_func", Arguments: []Node{}}
	_, err = eval.evalFunction(funcNode, item, nil, nil)
	if err == nil {
		t.Error("evalFunction with unknown_func should error")
	}

	// 3. resolveValue with non-value node (UnaryNode)
	// Default case in resolveValue switch
	unaryNode := &UnaryNode{Operator: Token{Type: TokenNOT}, Operand: badNode}
	_, err = eval.resolveValue(unaryNode, item, nil, nil)
	if err == nil {
		t.Error("resolveValue with UnaryNode should error")
	}

	// 4. resolveValue with function that doesn't return value (begins_with)
	// case *FunctionNode default
	voidFunc := &FunctionNode{Name: "begins_with", Arguments: []Node{}}
	_, err = eval.resolveValue(voidFunc, item, nil, nil)
	if err == nil {
		t.Error("resolveValue with begins_with should error")
	}
}

func TestCoverage_Parser_Deep(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}

	// 1. Unexpected token at end
	_, err := eval.EvaluateFilter(item, "a = b AND", nil, nil) // AND expects operand
	// Wait, "AND" at end fails parser.parseAnd -> error.
	// To hit "unexpected token at end" (Parser:31), we need valid complete expression followed by garbage.
	// "a = b c"
	_, err = eval.EvaluateFilter(item, "a = b c", nil, nil)
	if err == nil {
		t.Error("Should error on extra token")
	}

	// 2. Invalid array index (overflow)
	// Lexer treats digits as identifier? No, we need to check lexer behavior.
	// parser.go: "if p.match(TokenIdentifier)" -> Atoi.
	// If input is "a[9999999999999999999999999]", lexer emits TokenIdentifier "99..."
	// Atoi fails.
	_, err = eval.EvaluateFilter(item, "a[9999999999999999999999999] = :v", nil, nil)
	if err == nil {
		t.Error("Should error on index overflow")
	}
}

func TestCoverage_RuntimePath(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"list": {L: []models.AttributeValue{{S: strPtr("a")}}},
		"map":  {M: map[string]models.AttributeValue{"a": {S: strPtr("1")}}},
	}

	// List index out of bounds
	// list[1]
	match, err := eval.EvaluateFilter(item, "attribute_exists(list[1])", nil, nil)
	if err == nil && match {
		t.Error("list[1] should not exist/match")
	}

	// Index on map
	// map[0]
	// Provide valid RHS to ensure error (if any) comes from LHS or confirm LHS is just nil (missing)
	match, err = eval.EvaluateFilter(item, "map[0] = :v", nil, map[string]models.AttributeValue{":v": {S: strPtr("1")}})
	if err != nil {
		t.Errorf("map[0] error: %v", err)
	}
	if match {
		t.Error("map[0] should not match")
	}
}

func TestCoverage_RuntimePath_Negative(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"list": {L: []models.AttributeValue{{S: strPtr("a")}}},
	}
	// Manual AST for negative index
	pathNode := &PathNode{Parts: []PathPart{
		{Name: "list", IsIndex: false},
		{Index: -1, IsIndex: true},
	}}
	val, err := eval.resolveValue(pathNode, item, nil, nil)
	if err != nil {
		t.Error("resolveValue negative index should not error")
	}
	if val != nil {
		t.Error("resolveValue negative index should return nil")
	}
}

func TestCoverage_IN_ErrorIgnored(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"v": {S: strPtr("target")},
	}

	// IN (error_calc, target)
	// size(bool_missing) -> error? No, missing path -> nil.
	// We need resolveValue to return error.
	// size(invalid_type) -> error.
	vals := map[string]models.AttributeValue{
		":bool":   {BOOL: boolPtr(true)},
		":target": {S: strPtr("target")},
	}
	// "v IN (size(:bool), :target)"
	// size(:bool) errors. IN should continue and find :target.
	match, err := eval.EvaluateFilter(item, "v IN (size(:bool), :target)", nil, vals)
	if err != nil {
		t.Errorf("IN should ignore argument error, got: %v", err)
	}
	if !match {
		t.Error("IN should match :target despite previous arg error")
	}
}

func TestCoverage_Contains_SS(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"ss": {SS: []string{"a", "b", "c"}},
	}

	match, err := eval.EvaluateFilter(item, "contains(ss, :v)", nil, map[string]models.AttributeValue{":v": {S: strPtr("b")}})
	if err != nil || !match {
		t.Error("contains(ss, b) should match")
	}

	match, err = eval.EvaluateFilter(item, "contains(ss, :z)", nil, map[string]models.AttributeValue{":z": {S: strPtr("z")}})
	if err != nil || match {
		t.Error("contains(ss, z) should not match")
	}
}

func TestCoverage_CheckBetween_String(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{
		"s": {S: strPtr("b")},
	}
	// "b" between "a" and "c"
	match, err := eval.EvaluateFilter(item, "s BETWEEN :a AND :c", nil, map[string]models.AttributeValue{
		":a": {S: strPtr("a")},
		":c": {S: strPtr("c")},
	})
	if err != nil || !match {
		t.Error("BETWEEN string should match")
	}
}

func TestCoverage_Parser_Deep_Errors(t *testing.T) {
	eval := NewEvaluator()
	item := map[string]models.AttributeValue{}

	// begins_with without (
	_, err := eval.EvaluateFilter(item, "begins_with foo", nil, nil)
	if err == nil {
		t.Error("begins_with without paren should error")
	}

	// size without (
	_, err = eval.EvaluateFilter(item, "size = :v", nil, nil)
	if err == nil {
		t.Error("size without paren should error")
	}

	// Invalid index token
	_, err = eval.EvaluateFilter(item, "list[=] = :v", nil, nil)
	if err == nil {
		t.Error("list[=] should error")
	}

	// size(:v) -> literal arg, not path
	_, err = eval.EvaluateFilter(item, "size(:v) = :z", nil, map[string]models.AttributeValue{":v": {S: strPtr("s")}})
	if err == nil {
		t.Error("size(:v) should error (must be path)")
	}
}
