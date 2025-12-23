package main

import (
	"context"
	"net/http"
	"time"
)

// TimeoutMiddleware wraps an HTTP handler and adds a request-scoped timeout.
func TimeoutMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create a new context with a 5-second timeout.
		// This context will be used for the entire request lifecycle.
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)

		// The defer call is crucial. It ensures that resources associated with
		// the context are released when the handler returns, preventing leaks.
		defer cancel()

		// Create a new request with the new context and call the next handler.
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}
