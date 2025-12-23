package models

// APIError is a custom error type that holds DynamoDB-compatible error info.
// By placing it in its own package, we avoid import cycles.
type APIError struct {
	Type    string
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

func New(typ, msg string) *APIError {
	return &APIError{Type: typ, Message: msg}
}
