// Package main is the entry point for the concreteq application.
// It sets up an HTTP server that provides an SQS-compatible API.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/tabeth/concreteq/store"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// main is the primary function that starts the application.
// It initializes the data store, sets up the HTTP router, registers the API endpoints,
// and starts the web server.
func main() {
	// The port for the server can be specified via a command-line flag.
	// This allows for easy configuration when running the application.
	// For example: go run . -port=9090
	port := flag.String("port", "8080", "Port for the HTTP server to listen on")
	flag.Parse()

	// Initialize the data store. This application uses FoundationDB as its backend.
	// NewFDBStore() establishes a connection to the database and prepares the necessary
	// directory structure for the application's data.
	// The `store.Store` interface is used to abstract the database implementation,
	// so the handlers are not directly tied to FoundationDB.
	fdbStore, err := store.NewFDBStore()
	if err != nil {
		log.Fatalf("Failed to initialize FoundationDB store: %v", err)
	}

	// The App struct is a container for the application's dependencies, like the data store.
	// This is a form of dependency injection, making the handlers easier to test
	// by allowing a mock store to be injected during tests.
	app := &App{
		Store: fdbStore,
	}

	// A new HTTP router is created using Chi, a lightweight and idiomatic router for Go.
	// Chi is used to define the API endpoints and link them to their handler functions.
	r := chi.NewRouter()

	// Middleware provides common functionality that runs before the actual handlers.
	// - Logger: Logs incoming requests, including the method, path, and duration.
	// - Recoverer: Catches panics from downstream handlers and converts them into a 500 error,
	//   preventing the server from crashing.
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// RegisterSQSHandlers sets up all the API routes. This function, defined in handlers.go,
	// maps the SQS action names (like "CreateQueue", "SendMessage") to the corresponding
	// handler methods on the App struct.
	app.RegisterSQSHandlers(r)

	// Finally, the HTTP server is started on the configured port.
	// http.ListenAndServe blocks until the server is stopped or an error occurs.
	addr := fmt.Sprintf(":%s", *port)
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
