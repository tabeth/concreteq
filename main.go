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

func main() {
	// Define and parse the port flag
	port := flag.String("port", "8080", "Port for the HTTP server to listen on")
	flag.Parse()

	// Initialize the FoundationDB store
	fdbStore, err := store.NewFDBStore()
	if err != nil {
		log.Fatalf("Failed to initialize FoundationDB store: %v", err)
	}

	app := &App{
		Store: fdbStore,
	}

	// Create a new Chi router
	r := chi.NewRouter()

	// Add some middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Register the SQS API handlers
	app.RegisterSQSHandlers(r)

	// Start the HTTP server
	addr := fmt.Sprintf(":%s", *port)
	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
