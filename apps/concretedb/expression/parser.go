package expression

import (
	"fmt"
	"strconv"
)

type Parser struct {
	tokens []Token
	pos    int
	len    int
}

func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
		len:    len(tokens),
	}
}

func (p *Parser) Parse() (Node, error) {
	if p.len == 0 || p.tokens[0].Type == TokenEOF {
		return nil, nil
	}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected token at end of expression: %s", p.peek().Literal)
	}
	return node, nil
}

// Precedence: OR < AND < NOT < Comparisons

func (p *Parser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.match(TokenOR) {
		op := p.prev()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryNode{Left: left, Operator: op, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Node, error) {
	left, err := p.parsenot()
	if err != nil {
		return nil, err
	}

	for p.match(TokenAND) {
		op := p.prev()
		right, err := p.parsenot()
		if err != nil {
			return nil, err
		}
		left = &BinaryNode{Left: left, Operator: op, Right: right}
	}
	return left, nil
}

func (p *Parser) parsenot() (Node, error) {
	if p.match(TokenNOT) {
		op := p.prev()
		operand, err := p.parseCondition() // NOT applies to a condition (which could be parenthesized)
		if err != nil {
			return nil, err
		}
		return &UnaryNode{Operator: op, Operand: operand}, nil
	}
	return p.parseCondition()
}

func (p *Parser) parseCondition() (Node, error) {
	// Condition can be:
	// - ( Expression )
	// - Path Operator Operand
	// - Function(args)
	// - Path BETWEEN v1 AND v2
	// - Path IN (v1, v2...)

	if p.match(TokenLParen) {
		// Grouping
		expr, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if !p.match(TokenRParen) {
			return nil, fmt.Errorf("expected ')'")
		}
		return expr, nil
	}

	// Check if it's a known function keyword that starts a condition
	// attribute_exists(path), attribute_not_exists(path), begins_with(path, val), contains(path, val)
	// size(path) can be LHS of comparison too!

	if p.isFunctionStart() {
		return p.parseFunctionCall()
	}

	// LHS: Path or Function (size)
	// For now, assume Path. If we see 'size', handle it.
	var left Node
	var err error

	if p.check(TokenSize) {
		left, err = p.parseFunctionCall()
	} else {
		left, err = p.parsePath()
	}

	if err != nil {
		return nil, err
	}

	// Operator
	if p.match(TokenEq, TokenNE, TokenLT, TokenLTE, TokenGT, TokenGTE) {
		op := p.prev()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &BinaryNode{Left: left, Operator: op, Right: right}, nil
	}

	if p.match(TokenBETWEEN) {
		start, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if !p.match(TokenAND) {
			return nil, fmt.Errorf("BETWEEN expected AND")
		}
		end, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		// Represent BETWEEN as a special BinaryNode or Function?
		// Usually standard AST uses a ternary or special node.
		// Let's use FunctionNode for simplicity or a custom node.
		// Actually, let's allow FunctionNode to handle BETWEEN with 3 args?
		// Or update BinaryNode to support range? No.
		// Let's overload FunctionNode for internal representation: "BETWEEN(left, start, end)"
		return &FunctionNode{
			Name:      "BETWEEN",
			Arguments: []Node{left, start, end},
		}, nil
	}

	if p.match(TokenIN) {
		// Path IN (val1, val2...)
		if !p.match(TokenLParen) {
			return nil, fmt.Errorf("IN expected '('")
		}
		args := []Node{left}
		for {
			arg, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			if !p.match(TokenComma) {
				break
			}
		}
		if !p.match(TokenRParen) {
			return nil, fmt.Errorf("IN expected ')'")
		}
		return &FunctionNode{Name: "IN", Arguments: args}, nil
	}

	return left, nil // Could just be a standalone boolean path (rare in DDB filters, usually conditions)
}

func (p *Parser) parseOperand() (Node, error) {
	// Operand can be Literal (:v) or Function (size) or Path (another attribute)
	if p.match(TokenValue) {
		return &LiteralNode{Value: p.prev().Literal}, nil
	}
	// TODO: Support raw numbers/strings if we scan them? Lexer currently only does identifiers and :start.
	// Assuming placeholders for now.
	if p.match(TokenSize) {
		// size(path) as operand
		// Don't backup, just parse path?
		// No, parseFunctionCall expects to sit on the token (Name).
		// But match() advanced it.
		// p.advance() in function call consumes StartToken.
		// match() has already consumed TokenSize.
		// So p.prev() is TokenSize.
		// parseFunctionCall starts with: startToken := p.advance().
		// If we call parseFunctionCall now, it will consume the NEXT token (LParen).
		// We want it to use the Previous token as name?
		// Or we construct FunctionNode manually here.
		// "size" is special because it's a keyword but used as function name.

		if !p.match(TokenLParen) {
			return nil, fmt.Errorf("expected '(' after size")
		}
		// ... manual parse loop ...
		// Actually, let's just reuse logic.
		// Construct node manually.

		// Logic similar to parseFunctionCall loop
		var args []Node
		if !p.check(TokenRParen) {
			arg, err := p.parseOperand() // recurses for size(size(x))
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			// size only takes 1 arg usually
		}
		if !p.match(TokenRParen) {
			return nil, fmt.Errorf("expected ')'")
		}
		return &FunctionNode{Name: "size", Arguments: args}, nil
	}
	// Assume path? DDB allows comparing two attributes
	return p.parsePath()
}

func (p *Parser) parsePath() (Node, error) {
	// path -> ident (. ident | [ index ])*
	if !p.match(TokenIdentifier) {
		// It might be a reserved word used as identifier (e.g. name, value, count)
		// If we encounter a keyword in a path position, treat as identifier if possible?
		// For now, require identifiers or #placeholders
		return nil, fmt.Errorf("expected identifier at pos %d, got %s", p.peekPos(), p.peek().Literal)
	}

	parts := []PathPart{{Name: p.prev().Literal, Index: -1, IsIndex: false}}

	for {
		if p.match(TokenDot) {
			if !p.match(TokenIdentifier) {
				// Allow keywords after dot?
				// Try to consume next token as identifier regardless of type
				p.advance()
				// Use literal
			}
			parts = append(parts, PathPart{Name: p.prev().Literal, Index: -1, IsIndex: false})
		} else if p.match(TokenLBracket) {
			// Expect number
			// Lexer doesn't parse numbers yet, it parses alphanumeric chunks.
			// Ideally we should lex numbers.
			// Current lexer: lexValue (:val), lexIdentifier (alphanum).
			// If we have `[1]`, `1` is parsed as TokenIdentifier (since lexIdentifier takes digits).
			if !p.match(TokenIdentifier) {
				return nil, fmt.Errorf("expected index")
			}
			idxStr := p.prev().Literal
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", idxStr)
			}
			parts = append(parts, PathPart{Index: idx, IsIndex: true})
			if !p.match(TokenRBracket) {
				return nil, fmt.Errorf("expected ']'")
			}
		} else {
			break
		}
	}
	return &PathNode{Parts: parts}, nil
}

func (p *Parser) parseFunctionCall() (Node, error) {
	// func(arg1, arg2...)
	startToken := p.advance() // Consume function name
	name := startToken.Literal

	if !p.match(TokenLParen) {
		return nil, fmt.Errorf("expected '(' after function name")
	}

	var args []Node
	if !p.check(TokenRParen) {
		for {
			// Argument usually path or operand
			// attribute_exists(path)
			// begins_with(path, :val)
			var arg Node
			var err error

			// Try parsing as condition (nested?) No, usually just Path or Value.
			// But size(path) takes a path.
			arg, err = p.parseOperand()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)

			if !p.match(TokenComma) {
				break
			}
		}
	}

	if !p.match(TokenRParen) {
		return nil, fmt.Errorf("expected ')'")
	}

	return &FunctionNode{Name: name, Arguments: args}, nil
}

func (p *Parser) isFunctionStart() bool {
	t := p.peek()
	return t.Type == TokenAttributeExists ||
		t.Type == TokenAttributeNotExists ||
		t.Type == TokenBeginsWith ||
		t.Type == TokenContains ||
		t.Type == TokenListAppend ||
		t.Type == TokenIfNotExists
}

// Helpers

func (p *Parser) match(types ...TokenType) bool {
	for _, t := range types {
		if p.check(t) {
			p.advance()
			return true
		}
	}
	return false
}

func (p *Parser) check(t TokenType) bool {
	if p.isAtEnd() {
		return false
	}
	return p.peek().Type == t
}

func (p *Parser) advance() Token {
	if !p.isAtEnd() {
		p.pos++
	}
	return p.previous()
}

func (p *Parser) isAtEnd() bool {
	return p.peek().Type == TokenEOF
}

func (p *Parser) peek() Token {
	return p.tokens[p.pos]
}

func (p *Parser) prev() Token {
	return p.tokens[p.pos-1]
}

func (p *Parser) previous() Token {
	return p.tokens[p.pos-1]
}

func (p *Parser) peekPos() int {
	return p.peek().Pos
}
