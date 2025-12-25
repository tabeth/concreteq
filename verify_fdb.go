package main

import (
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func main() {
	fdb.MustAPIVersion(710)
	db, err := fdb.OpenDefault()
	if err != nil {
		log.Fatalf("Failed to open FDB: %v", err)
	}

	fmt.Println("Attempting to access database...")
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return tr.Get(fdb.Key("foo")).Get()
	})
	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}
	fmt.Println("Successfully connected and read from FDB!")
}
