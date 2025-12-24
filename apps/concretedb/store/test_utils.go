package store

// stringPtr returns a pointer to the string s.
// It is a helper for creating AttributeValues.
func stringPtr(s string) *string {
	return &s
}
