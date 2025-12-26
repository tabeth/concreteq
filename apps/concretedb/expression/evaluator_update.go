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
			fmt.Printf("DEBUG: list_append l1 len=%d l2 len=%d\n", len(l1.L), len(l2.L))
			res, err := applyListAppend(l1, l2)
			if err == nil {
				fmt.Printf("DEBUG: list_append result len=%d\n", len(res.L))
			}
			return res, err
		}
		if n.Name == "if_not_exists" {
			if len(n.Arguments) != 2 {
				return nil, fmt.Errorf("if_not_exists requires 2 arguments")
			}
			// Special handling: resolve path (arg0). If nil or err, use arg1.
			// Re-use logic: e.resolveValue returns nil if path not found?
			// But e.resolveValue errors on missing placeholders?
			// Arg0 MUST be a path.
			pathNode, ok := n.Arguments[0].(*PathNode)
			if !ok {
				return nil, fmt.Errorf("if_not_exists first argument must be a path")
			}

			// Try resolving path
			// Note: We need a version of resolvePath that doesn't return (nil, nil) if intermediate missing?
			// Actually resolvePath returns (nil, nil) if missing.

			// We need to resolve with current item context
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

	// We need to traverse references until the *last* part.
	// item is map[string]AV.

	// Create a pointer to the current map we are traversing
	currentMap := item

	// Top level key is always the first part name
	topLevelKey := resolveName(pNode.Parts[0].Name, names)

	// Traverse up to len-1
	for i := 0; i < len(pNode.Parts)-1; i++ {
		part := pNode.Parts[i]

		// Resolve next container
		if part.IsIndex {
			// Array indexing in SET path is tricky: "a[1].b = 2"
			// "item" here is not addressable easily if we recursed?
			// Wait, the structural traversal is hard with Go maps/structs copies.
			// But maps are references. Slices are references.
			// BUT: AttributeValue holds *pointers* to values? Or slice of structs?
			// type AttributeValue struct { L []AttributeValue ... }
			// L is a slice of structs.
			// modifying L[i] modifies the backing array, but we need to ensure we hold the pointer.
			return "", fmt.Errorf("nested types in SET path not fully supported in this MVP phase (requires complex pointer traversal)")
		} else {
			// Map key
			key := resolveName(part.Name, names)
			nextVal, ok := currentMap[key]
			if !ok {
				return "", fmt.Errorf("path not found: %s", key)
			}
			if nextVal.M == nil {
				return "", fmt.Errorf("item at path %s is not a Map", key)
			}
			currentMap = nextVal.M // This is a map, so it's a reference type! We can modify it.
		}
	}

	// Last part
	lastPart := pNode.Parts[len(pNode.Parts)-1]
	if lastPart.IsIndex {
		// SET a[1] = val
		// We need the parent slice.
		// If len(parts) == 1, parent is 'item' (map), so index access is invalid on 'item'.
		// Index access must imply the *previous* part yielded a List.
		// Logic error in loop above?
		// if lastPart is Index, the `currentMap` is WRONG context. We need `currentList`.
		// Refactoring traversal for mixed types is complex.
		// For MVP: Support Map nesting, but maybe not List indexing?
		// "Updating nested paths" was requested.
		// Let's stick to Map-only nesting for now + Top-level.
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

	// Simplify: Only Top Level removel for now for safety, or simple Map traversal
	if len(pNode.Parts) == 1 {
		key := resolveName(pNode.Parts[0].Name, names)
		delete(item, key)
		return key, nil
	}
	return "", fmt.Errorf("nested REMOVE not supported")
}

func (e *Evaluator) addValue(item map[string]models.AttributeValue, pathNode Node, val models.AttributeValue, names map[string]string) (string, error) {
	// ADD path value.
	// If path doesn't exist: create it with value.
	// If path exists and is Number: Add.
	// If path exists and is Set: Union.
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
