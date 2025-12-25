package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	// Import workload to register 'core' workload
	_ "github.com/pingcap/go-ycsb/pkg/workload"
)

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}

	command := os.Args[1]
	dbName := os.Args[2]

	// Parse flags
	fs := flag.NewFlagSet("ycsb", flag.ExitOnError)
	var propertyFiles stringSlice
	var propertyValues stringSlice

	fs.Var(&propertyFiles, "P", "Property file")
	fs.Var(&propertyValues, "p", "Property key=value")

	err := fs.Parse(os.Args[3:])
	if err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	p := properties.NewProperties()
	for _, f := range propertyFiles {
		newP, err := properties.LoadFile(f, properties.UTF8)
		if err != nil {
			fmt.Printf("Error loading property file %s: %v\n", f, err)
			os.Exit(1)
		}
		p.Merge(newP)
	}

	for _, v := range propertyValues {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 {
			p.Set(parts[0], parts[1])
		}
	}

	// Setup command properties
	p.Set(prop.MeasurementType, "raw")

	measurement.InitMeasure(p)

	if command == "load" {
		p.Set(prop.DoTransactions, "false")
	} else if command == "run" {
		p.Set(prop.DoTransactions, "true")
	} else {
		fmt.Printf("Unknown command: %s\n", command)
		usage()
		os.Exit(1)
	}

	dbCreator := ycsb.GetDBCreator(dbName)
	if dbCreator == nil {
		fmt.Printf("Database %s not registered\n", dbName)
		os.Exit(1)
	}
	db, err := dbCreator.Create(p)
	if err != nil {
		fmt.Printf("Error creating DB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	workloadName := p.GetString(prop.Workload, "core")
	workloadCreator := ycsb.GetWorkloadCreator(workloadName)
	if workloadCreator == nil {
		fmt.Printf("Workload %s not registered\n", workloadName)
		os.Exit(1)
	}
	workload, err := workloadCreator.Create(p)
	if err != nil {
		fmt.Printf("Error creating workload: %v\n", err)
		os.Exit(1)
	}
	defer workload.Close()

	c := client.NewClient(p, workload, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		cancel()
	}()

	start := time.Now()
	c.Run(ctx)
	fmt.Printf("Total execution time: %v\n", time.Since(start))

	measurement.Output()
}

func usage() {
	fmt.Println("Usage: ycsb <command> <database> [options]")
	fmt.Println("Commands: load, run")
	fmt.Println("Options:")
	fmt.Println("  -P <file>       Property file")
	fmt.Println("  -p <key=value>  Property key=value")
}

type stringSlice []string

func (s *stringSlice) String() string {
	return fmt.Sprintf("%v", *s)
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}
