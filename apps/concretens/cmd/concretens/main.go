package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/kiroku-inc/kiroku-core/apps/concretens/server"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/service"
	"github.com/kiroku-inc/kiroku-core/apps/concretens/store"
)

func main() {
	// Initialize Store
	// API Version 710 is standard for modern FDB
	st, err := store.NewStore(710)
	if err != nil {
		log.Fatalf("Failed to initialize store: %v", err)
	}

	srv := server.NewServer(st)

	http.HandleFunc("/createTopic", srv.CreateTopicHandler)
	http.HandleFunc("/publish", srv.PublishHandler)
	http.HandleFunc("/subscribe", srv.SubscribeHandler)
	http.HandleFunc("/confirmSubscription", srv.ConfirmSubscriptionHandler)
	http.HandleFunc("/deleteTopic", srv.DeleteTopicHandler)
	http.HandleFunc("/deleteSubscription", srv.DeleteSubscriptionHandler)
	http.HandleFunc("/listTopics", srv.ListTopicsHandler)
	http.HandleFunc("/listSubscriptions", srv.ListSubscriptionsHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	publicURL := os.Getenv("PUBLIC_URL")
	if publicURL == "" {
		publicURL = "http://localhost:" + port
	}

	// Start Dispatcher
	// Use 10 workers for concurrency
	dispatcher := service.NewDispatcher(st, 10, publicURL)
	dispatcher.Start(context.Background())
	defer dispatcher.Stop()

	log.Printf("Server listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
