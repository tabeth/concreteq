package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	_ "github.com/tabeth/concretesql/driver"
	"github.com/tabeth/concretesql/store"
	"github.com/tabeth/kiroku-core/libs/fdb"
)

func main() {
	var verbose bool
	flag.BoolVar(&verbose, "verbose", false, "Verbose logging")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage: concretesql <db-name> OR concretesql dump <db-name> <output-file>")
		os.Exit(1)
	}

	if args[0] == "dump" {
		if len(args) != 3 {
			fmt.Println("Usage: concretesql dump <db-name> <output-file>")
			os.Exit(1)
		}
		if err := runDump(args[1], args[2]); err != nil {
			log.Fatalf("Dump failed: %v", err)
		}
		return
	}

	dbName := args[0]
	// Construct DSN
	dsn := fmt.Sprintf("file:%s?vfs=concretesql", dbName)
	if verbose {
		log.Printf("Connecting to %s", dsn)
	}

	db, err := sql.Open("concretesql", dsn)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	fmt.Printf("Connected to %s via ConcreteSQL/FDB.\n", dbName)
	fmt.Println("Enter SQL commands terminated by ';'")
	fmt.Println("Type .help for special commands.")

	scanner := bufio.NewScanner(os.Stdin)
	var queryBuffer strings.Builder // Accumulate lines for multi-line SQL

	fmt.Print("concretesql> ")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Handle Dot Commands
		if strings.HasPrefix(line, ".") {
			handleDotCommand(db, line)
			fmt.Print("concretesql> ")
			queryBuffer.Reset()
			continue
		}

		if line == "" {
			if queryBuffer.Len() > 0 {
				fmt.Print("   ...> ")
			} else {
				fmt.Print("concretesql> ")
			}
			continue
		}

		// Append line to buffer
		if queryBuffer.Len() > 0 {
			queryBuffer.WriteString(" ")
		}
		queryBuffer.WriteString(line)

		// Check for terminator
		if strings.HasSuffix(line, ";") {
			fullQuery := queryBuffer.String()
			executeQuery(db, fullQuery)
			queryBuffer.Reset()
			fmt.Print("concretesql> ")
		} else {
			fmt.Print("   ...> ")
		}
	}
}

func handleDotCommand(db *sql.DB, cmd string) {
	parts := strings.Fields(cmd)
	switch parts[0] {
	case ".tables":
		executeQuery(db, "SELECT name FROM sqlite_master WHERE type='table';")
	case ".schema":
		executeQuery(db, "SELECT sql FROM sqlite_master;")
	case ".quit", ".exit":
		fmt.Println("Bye!")
		os.Exit(0)
	case ".help":
		fmt.Println(".tables     List tables")
		fmt.Println(".schema     Show schema")
		fmt.Println(".quit       Exit program")
		fmt.Println(".help       Show this help")
	default:
		fmt.Printf("Unknown command: %s\n", parts[0])
	}
}

func executeQuery(db *sql.DB, query string) {
	// Simple heuristic: SELECT vs others
	q := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(q, "SELECT") || strings.HasPrefix(q, "PRAGMA") {
		rows, err := db.Query(query)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			fmt.Printf("Error getting columns: %v\n", err)
			return
		}

		// Pretty print
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug)
		// Header
		for i, col := range cols {
			fmt.Fprint(w, col)
			if i < len(cols)-1 {
				fmt.Fprint(w, "\t")
			}
		}
		fmt.Fprintln(w)

		// Data
		count := 0
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				fmt.Printf("Error scanning: %v\n", err)
				return
			}
			for i, val := range values {
				var s string
				if val == nil {
					s = "NULL"
				} else {
					s = fmt.Sprintf("%v", val)
				}
				fmt.Fprint(w, s)
				if i < len(values)-1 {
					fmt.Fprint(w, "\t")
				}
			}
			fmt.Fprintln(w)
			count++
		}
		w.Flush()
		// fmt.Printf("(%d rows)\n", count)
	} else {
		res, err := db.Exec(query)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		aff, _ := res.RowsAffected()
		// Only print if meaningful? SQLite shell is quiet usually.
		if aff > 0 {
			// fmt.Printf("Affected rows: %d\n", aff)
		}
	}
}

func runDump(dbName, outputFile string) error {
	// Initialize FDB
	database, err := fdb.OpenDB(620)
	if err != nil {
		return fmt.Errorf("failed to open FDB: %w", err)
	}

	// Logic matches vfs.FDBVFS.Open prefix generation: ("concretesql", dbName)
	prefix := tuple.Tuple{"concretesql", dbName}
	ps := store.NewPageStore(database, prefix, store.DefaultConfig())

	f, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer f.Close()

	fmt.Printf("Dumping database '%s' to '%s'...\n", dbName, outputFile)

	// Use buffered writer for performance
	bw := bufio.NewWriter(f)
	if err := ps.DumpToWriter(context.Background(), bw); err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	fmt.Println("Dump complete.")
	return nil
}
