package filter

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// Matches checks if the given message (attributes or body map) matches the filter policy.
// policyJSON: The SNS Filter Policy JSON.
// messageData: The data to match against. Can be map[string]string (attributes) or map[string]interface{} (body).
func Matches(policyJSON string, messageData interface{}) (bool, error) {
	if policyJSON == "" || policyJSON == "{}" {
		return true, nil
	}

	var policy map[string]interface{}
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return false, fmt.Errorf("invalid filter policy json: %w", err)
	}

	// Normalize messageData to map[string]interface{}
	var data map[string]interface{}

	switch v := messageData.(type) {
	case map[string]interface{}:
		data = v
	case map[string]string:
		data = make(map[string]interface{})
		for k, val := range v {
			data[k] = val
		}
	default:
		return false, fmt.Errorf("unsupported message data type: %T", messageData)
	}

	return matchRecursive(policy, data), nil
}

func matchRecursive(policy map[string]interface{}, data map[string]interface{}) bool {
	for key, policyVal := range policy {
		dataVal, exists := data[key]
		if !exists {
			// Key missing in data -> No match (unless we support 'exists': false logic later)
			return false
		}

		// SNS Policy handling:
		// 1. If policyVal is a list [a, b], it means OR. dataVal must match one of them.
		// 2. If policyVal is a map, recurse (dataVal must be a map too).
		// 3. Simple equality not strictly standard SNS (SNS wraps everything in lists usually),
		//    but standard implementations might allow direct values.
		//    However, safe to assume standard SNS JSON syntax: always list for leaf values, object for nesting.

		switch pVal := policyVal.(type) {
		case map[string]interface{}:
			// Nested matching
			dMap, ok := dataVal.(map[string]interface{})
			if !ok {
				// Structure mismatch: policy expects object, data has leaf
				return false
			}
			if !matchRecursive(pVal, dMap) {
				return false
			}

		case []interface{}:
			// Leaf node (OR logic)
			// dataVal must match one of the elements in pVal
			matchedAny := false
			for _, allowed := range pVal {
				if matchValue(allowed, dataVal) {
					matchedAny = true
					break
				}
			}
			if !matchedAny {
				return false
			}

		default:
			// If user sends raw value in policy (non-standard but possible in custom implementations)
			if !matchValue(pVal, dataVal) {
				return false
			}
		}
	}
	return true
}

func matchValue(policyVal interface{}, dataVal interface{}) bool {
	// Exact match check with type leniency if needed, but strict is safer.
	// JSON unmarshal makes numbers float64.
	return reflect.DeepEqual(policyVal, dataVal)
}
