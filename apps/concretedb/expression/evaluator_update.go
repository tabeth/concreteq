package expression

import (
	"fmt"
	"strconv"

	"github.com/tabeth/concretedb/models"
)

// ApplyUpdate parses and applies the update expression to the item.
// It returns a map of top-level attribute names that were modified.
func (e *Evaluator) ApplyUpdate(item map[string]models.AttributeValue, updateExpr string, names map[string]string, values map[string]models.AttributeValue) (map[string]bool, error) {
	if updateExpr == "" {
		return nil, nil
	}

	lexer := NewLexer(updateExpr)
	parser := NewParser(lexer.tokens)
	node, err := parser.ParseUpdate()
	if err != nil {
		return nil, err
	}

	changed := make(map[string]bool)

	// SET
	for _, action := range node.SetActions {
		val, err := e.resolveUpdateValue(action.Value, item, names, values)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, fmt.Errorf("value not found for SET action")
		}

		// SetValue modifies item in-place
		affected, err := e.setValue(item, action.Path, *val, names)
		if err != nil {
			return nil, err
		}
		changed[affected] = true
	}

	// REMOVE
	for _, action := range node.RemoveActions {
		affected, err := e.removeValue(item, action.Path, names)
		if err != nil {
			return nil, err
		}
		if affected != "" {
			changed[affected] = true
		}
	}

	// ADD
	for _, action := range node.AddActions {
		val, err := e.resolveUpdateValue(action.Value, item, names, values)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, fmt.Errorf("value not found for ADD action")
		}
		affected, err := e.addValue(item, action.Path, *val, names)
		if err != nil {
			return nil, err
		}
		changed[affected] = true
	}

	// DELETE
	for _, action := range node.DeleteActions {
		val, err := e.resolveUpdateValue(action.Value, item, names, values)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, fmt.Errorf("value not found for DELETE action")
		}
		affected, err := e.deleteValue(item, action.Path, *val, names)
		if err != nil {
			return nil, err
		}
		changed[affected] = true
	}

	return changed, nil
}

func (e *Evaluator) resolveUpdateValue(node Node, item map[string]models.AttributeValue, names map[string]string, values map[string]models.AttributeValue) (*models.AttributeValue, error) {
	switch n := node.(type) {
	case *BinaryNode:
		// Arithmetic: left + right, left - right
		left, err := e.resolveUpdateValue(n.Left, item, names, values)
		if err != nil {
			return nil, err
		}
		right, err := e.resolveUpdateValue(n.Right, item, names, values)
		if err != nil {
			return nil, err
		}

		// Handle Arithmetic
		if n.Operator.Type == TokenPlus || n.Operator.Type == TokenMinus {
			return applyArithmetic(left, right, n.Operator.Type)
		}
		return nil, fmt.Errorf("unsupported operator in update value: %s", n.Operator.Literal)

	case *FunctionNode:
		// list_append(l1, l2), if_not_exists(path, default)
		if n.Name == "list_append" {
			if len(n.Arguments) != 2 {
				return nil, fmt.Errorf("list_append requires 2 arguments")
			}
			l1, err := e.resolveUpdateValue(n.Arguments[0], item, names, values)
			if err != nil {
				return nil, err
			}
			l2, err := e.resolveUpdateValue(n.Arguments[1], item, names, values)
			if err != nil {
				return nil, err
			}
			return applyListAppend(l1, l2)
		}
		if n.Name == "if_not_exists" {
			if len(n.Arguments) != 2 {
				return nil, fmt.Errorf("if_not_exists requires 2 arguments")
			}
			pathNode, ok := n.Arguments[0].(*PathNode)
			if !ok {
				return nil, fmt.Errorf("if_not_exists first argument must be a path")
			}

			// Try resolving path
			val, err := e.resolvePath(pathNode, item, names)
			if err != nil {
				return nil, err
			}
			if val != nil {
				return val, nil
			}
			// Fallback to default
			return e.resolveUpdateValue(n.Arguments[1], item, names, values)
		}
		return nil, fmt.Errorf("unknown function in update: %s", n.Name)
	}

	// Fallback to standard value resolution (Literal, Path)
	return e.resolveValue(node, item, names, values)
}

// SetValue sets the value at path.
func (e *Evaluator) setValue(item map[string]models.AttributeValue, pathNode Node, val models.AttributeValue, names map[string]string) (string, error) {
	pNode, ok := pathNode.(*PathNode)
	if !ok {
		return "", fmt.Errorf("SET target must be a path")
	}

	if len(pNode.Parts) == 0 {
		return "", fmt.Errorf("empty path")
	}

	// We need to traverse references until the last part.
	currentMap := item
	topLevelKey := resolveName(pNode.Parts[0].Name, names)

	// Traverse up to len-1
	for i := 0; i < len(pNode.Parts)-1; i++ {
		part := pNode.Parts[i]

		if part.IsIndex {
			return "", fmt.Errorf("nested types in SET path not fully supported")
		} else {
			key := resolveName(part.Name, names)
			nextVal, ok := currentMap[key]
			if !ok {
				return "", fmt.Errorf("path not found: %s", key)
			}
			if nextVal.M == nil {
				return "", fmt.Errorf("item at path %s is not a Map", key)
			}
			currentMap = nextVal.M
		}
	}

	lastPart := pNode.Parts[len(pNode.Parts)-1]
	if lastPart.IsIndex {
		return "", fmt.Errorf("list indexing in SET target not supported")
	} else {
		key := resolveName(lastPart.Name, names)
		currentMap[key] = val
	}

	return topLevelKey, nil
}

func (e *Evaluator) removeValue(item map[string]models.AttributeValue, pathNode Node, names map[string]string) (string, error) {
	pNode, ok := pathNode.(*PathNode)
	if !ok {
		return "", fmt.Errorf("REMOVE target must be a path")
	}

	if len(pNode.Parts) == 1 {
		key := resolveName(pNode.Parts[0].Name, names)
		delete(item, key)
		return key, nil
	}
	return "", fmt.Errorf("nested REMOVE not supported")
}

func (e *Evaluator) addValue(item map[string]models.AttributeValue, pathNode Node, val models.AttributeValue, names map[string]string) (string, error) {
	pNode, ok := pathNode.(*PathNode)
	if !ok {
		return "", fmt.Errorf("ADD target must be a path")
	}

	if len(pNode.Parts) != 1 {
		return "", fmt.Errorf("nested ADD not supported")
	}

	key := resolveName(pNode.Parts[0].Name, names)
	existing, exists := item[key]

	if !exists {
		item[key] = val
		return key, nil
	}

	// Math ADD
	if existing.N != nil && val.N != nil {
		res, err := applyArithmetic(&existing, &val, TokenPlus)
		if err != nil {
			return "", err
		}
		item[key] = *res
		return key, nil
	}

	// Set ADD
	if existing.SS != nil && val.SS != nil {
		// Union
		union := append([]string{}, existing.SS...)
		seen := make(map[string]bool)
		for _, s := range existing.SS {
			seen[s] = true
		}
		for _, s := range val.SS {
			if !seen[s] {
				union = append(union, s)
				seen[s] = true
			}
		}
		item[key] = models.AttributeValue{SS: union}
		return key, nil
	}
	// TODO: NS, BS

	return "", fmt.Errorf("ADD mismatch types or unsupported types")
}

func (e *Evaluator) deleteValue(item map[string]models.AttributeValue, pathNode Node, val models.AttributeValue, names map[string]string) (string, error) {
	// DELETE path value (Set only)
	pNode, ok := pathNode.(*PathNode)
	if !ok {
		return "", fmt.Errorf("DELETE target must be a path")
	}

	if len(pNode.Parts) != 1 {
		return "", fmt.Errorf("nested DELETE not supported")
	}
	key := resolveName(pNode.Parts[0].Name, names)
	existing, exists := item[key]
	if !exists {
		return key, nil // Nothing to delete
	}

	if existing.SS != nil && val.SS != nil {
		toDelete := make(map[string]bool)
		for _, s := range val.SS {
			toDelete[s] = true
		}

		var newSet []string
		for _, s := range existing.SS {
			if !toDelete[s] {
				newSet = append(newSet, s)
			}
		}
		item[key] = models.AttributeValue{SS: newSet}
		return key, nil
	}

	return "", fmt.Errorf("DELETE only supports Sets")
}

func applyArithmetic(left, right *models.AttributeValue, op TokenType) (*models.AttributeValue, error) {
	if left.N == nil || right.N == nil {
		return nil, fmt.Errorf("arithmetic requires Number types")
	}
	var a, b float64
	fmt.Sscan(*left.N, &a)
	fmt.Sscan(*right.N, &b)

	var res float64
	if op == TokenPlus {
		res = a + b
	} else {
		res = a - b
	}

	// Format back (naive)
	s := strconv.FormatFloat(res, 'f', -1, 64)
	return &models.AttributeValue{N: &s}, nil
}

func applyListAppend(l1, l2 *models.AttributeValue) (*models.AttributeValue, error) {
	if l1.L == nil || l2.L == nil {
		// Docs: "Both operands must be of type List".
		// Actually if one is not list, it errors in DynamoDB?
		// "If you try to append to a list that does not exist..." (handled by if_not_exists usually)
		return nil, fmt.Errorf("list_append operands must be Lists")
	}

	newList := make([]models.AttributeValue, 0, len(l1.L)+len(l2.L))
	newList = append(newList, l1.L...)
	newList = append(newList, l2.L...)
	return &models.AttributeValue{L: newList}, nil
}
