package main

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/tabeth/concretedb/config"
)

// TestMainServerStartup verifies that the main function can start an HTTP server
// and that the health check endpoint is responsive.
func TestMainServerStartup(t *testing.T) {
	// Run our main function in a goroutine.
	go main()

	// Give the server a moment to start.
	time.Sleep(100 * time.Millisecond)

	cfg := config.NewConfig()
	// CHANGED: The test now points to the /health endpoint.
	testURL := fmt.Sprintf("http://localhost:%d/health", cfg.Port)

	req, err := http.NewRequest("GET", testURL, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Provide a more helpful error message if the connection is refused.
		t.Fatalf("Failed to send request to server at %s: %v. Is the server running?", testURL, err)
	}
	defer resp.Body.Close()

	// 1. Check the HTTP status code.
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status code %d for health check, but got %d", http.StatusOK, resp.StatusCode)
	}

	// 2. Check the response body.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	// CHANGED: The expected body is now "OK\n" from our health handler.
	expectedBody := "OK\n"
	if string(body) != expectedBody {
		t.Errorf("expected response body '%s', but got '%s'", expectedBody, string(body))
	}
}
