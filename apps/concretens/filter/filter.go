package filter

import (
	"encoding/json"
	"fmt"
)

// Policy represents a subscription filter policy.
// It maps attribute names to a list of allowed values.
// Only string matching is supported for now.
type Policy map[string][]string

// Matches checks if the given message attributes match the filter policy.
// Returns true if the attributes satisfy the policy, or if the policy is empty/invalid.
//
// Logic:
//  1. If policy is empty, match.
//  2. For every key in policy, the attribute must exist in msgAttributes.
//  3. For every key in policy, the specific attribute value must be one of the allowed values in policy (exact match).
//     If the policy value list is empty, it effectively matches nothing (unless we define exists logic).
//     Let's assume standard SNS behavior: OR within the list of values.
func Matches(policyJSON string, msgAttributes map[string]string) (bool, error) {
	if policyJSON == "" || policyJSON == "{}" {
		return true, nil
	}

	var policy Policy
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		// Invalid policy is treated as no-match or error?
		// SNS might reject subscription on creation if invalid.
		// Use match=false for runtime safety.
		return false, fmt.Errorf("invalid filter policy json: %w", err)
	}

	for key, allowedValues := range policy {
		val, exists := msgAttributes[key]
		if !exists {
			// Attribute missing but required by policy -> No Match
			return false, nil
		}

		// Check if val is in allowedValues
		match := false
		for _, allowed := range allowedValues {
			if val == allowed {
				match = true
				break
			}
		}

		if !match {
			return false, nil
		}
	}

	return true, nil
}
