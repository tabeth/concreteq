package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/tabeth/concretedb/store"
	"github.com/tabeth/kiroku-core/libs/fdb"
)

func main() {
	db, err := fdb.OpenDB(730)
	if err != nil {
		log.Fatalf("Failed to open FDB: %v", err)
	}

	_, err = store.NewFDBStore(db)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ConcreteDB is running"))
	})

	// TODO: Add API handlers using st

	log.Println("Starting ConcreteDB on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
