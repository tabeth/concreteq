package expression

import (
	"fmt"
)

// ParseUpdate parses a DynamoDB UpdateExpression.
func (p *Parser) ParseUpdate() (*UpdateNode, error) {
	node := &UpdateNode{}

	// We can have multiple clauses in any order, but usually distinct.
	// We will loop until EOF.
	for !p.isAtEnd() {
		if p.match(TokenSET) {
			actions, err := p.parseSetClause()
			if err != nil {
				return nil, err
			}
			node.SetActions = append(node.SetActions, actions...)
		} else if p.match(TokenREMOVE) {
			actions, err := p.parseRemoveClause()
			if err != nil {
				return nil, err
			}
			node.RemoveActions = append(node.RemoveActions, actions...)
		} else if p.match(TokenADD) {
			actions, err := p.parseAddClause()
			if err != nil {
				return nil, err
			}
			node.AddActions = append(node.AddActions, actions...)
		} else if p.match(TokenDELETE) {
			actions, err := p.parseDeleteClause()
			if err != nil {
				return nil, err
			}
			node.DeleteActions = append(node.DeleteActions, actions...)
		} else {
			return nil, fmt.Errorf("unexpected token in UpdateExpression: %s", p.peek().Literal)
		}
	}

	return node, nil
}

func (p *Parser) parseSetClause() ([]*SetAction, error) {
	var actions []*SetAction
	// path = value, path = value...
	for {
		path, err := p.parsePath()
		if err != nil {
			return nil, err
		}
		if !p.match(TokenEq) {
			return nil, fmt.Errorf("expected '=' after path in SET clause")
		}

		val, err := p.parseSetValue()
		if err != nil {
			return nil, err
		}

		actions = append(actions, &SetAction{Path: path, Value: val})

		if !p.match(TokenComma) {
			break
		}
	}
	return actions, nil
}

func (p *Parser) parseSetValue() (Node, error) {
	// Value can be:
	// - Operand (Path, Literal, Function)
	// - Operand + Operand
	// - Operand - Operand

	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	if p.match(TokenPlus) {
		op := p.prev()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &BinaryNode{Left: left, Operator: op, Right: right}, nil
	} else if p.match(TokenMinus) {
		op := p.prev()
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &BinaryNode{Left: left, Operator: op, Right: right}, nil
	}

	return left, nil
}

func (p *Parser) parseRemoveClause() ([]*RemoveAction, error) {
	var actions []*RemoveAction
	for {
		path, err := p.parsePath()
		if err != nil {
			return nil, err
		}
		actions = append(actions, &RemoveAction{Path: path})

		if !p.match(TokenComma) {
			break
		}
	}
	return actions, nil
}

func (p *Parser) parseAddClause() ([]*AddAction, error) {
	var actions []*AddAction
	for {
		path, err := p.parsePath()
		if err != nil {
			return nil, err
		}
		// Expect value (Literal usually)
		val, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		actions = append(actions, &AddAction{Path: path, Value: val})

		if !p.match(TokenComma) {
			break
		}
	}
	return actions, nil
}

func (p *Parser) parseDeleteClause() ([]*DeleteAction, error) {
	var actions []*DeleteAction
	for {
		path, err := p.parsePath()
		if err != nil {
			return nil, err
		}
		// Expect value (set subset)
		val, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		actions = append(actions, &DeleteAction{Path: path, Value: val})

		if !p.match(TokenComma) {
			break
		}
	}
	return actions, nil
}
