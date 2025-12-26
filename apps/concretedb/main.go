package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/tabeth/concretedb/config"
	"github.com/tabeth/concretedb/service"
	"github.com/tabeth/concretedb/store"
	"github.com/tabeth/concretedb/ttl"
)

func main() {
	logger := log.New(os.Stdout, "concretedb: ", log.LstdFlags|log.Lshortfile)

	// Initialize configuration and dependencies
	cfg := config.NewConfig()

	fdb.MustAPIVersion(710)
	var fdbConn fdb.Database
	type fdbConnResult struct {
		db  fdb.Database
		err error
	}
	fdbChan := make(chan fdbConnResult, 1)
	go func() {
		db, err := fdb.OpenDefault()
		fdbChan <- fdbConnResult{db: db, err: err}

	}()

	select {
	case res := <-fdbChan:
		if res.err != nil {
			logger.Fatalf("Failed to connect to FoundationDB: %v", res.err)
		}
		fdbConn = res.db
		logger.Println("Successfully connected to FoundationDB.")

	case <-time.After(10 * time.Second):
		logger.Fatalf("Failed to connect to FoundationDB after 10 seconds")
	}

	fdbStore := store.NewFoundationDBStore(fdbConn)
	tableService := service.NewTableService(fdbStore)
	apiHandler := NewDynamoDBHandler(tableService)

	// Start Background Jobs
	ttlWorker := ttl.NewTTLWorker(tableService, 1*time.Minute)
	ttlWorker.Start()
	defer ttlWorker.Stop()

	// Configure HTTP server
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})

	mux.Handle("/", apiHandler)

	// Start the server
	portStr := strconv.Itoa(cfg.Port)
	addr := ":" + portStr

	handlerWithTimeout := TimeoutMiddleware(mux)

	server := &http.Server{
		Addr:    addr,
		Handler: handlerWithTimeout,

		// ReadTimeout is the maximum duration for reading the entire
		// request, including the body. Protects against slow clients.
		ReadTimeout: 15 * time.Second,

		// WriteTimeout is the maximum duration before timing out
		// writes of the response.
		WriteTimeout: 15 * time.Second,

		// IdleTimeout is the maximum amount of time to wait for the
		// next request when keep-alives are enabled.
		IdleTimeout: 60 * time.Second,
	}

	logger.Printf("Server starting on port %s, health check available at /health", portStr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatalf("could not start server: %v", err)
	}
}
