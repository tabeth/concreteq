module github.com/tabeth/concretesql/benchmark/ycsb

go 1.24.0

replace github.com/tabeth/concretesql => ../..

replace github.com/tabeth/kiroku-core/libs/fdb => ../../../../libs/fdb

require (
	github.com/magiconair/properties v1.8.0
	github.com/pingcap/go-ycsb v1.0.1
	github.com/tabeth/concretesql v0.0.0-00010101000000-000000000000
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/apple/foundationdb/bindings/go v0.0.0-20250501205732-f0ed69ea389a // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/mattn/go-sqlite3 v2.0.1+incompatible // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c // indirect
	github.com/psanford/sqlite3vfs v0.0.0-20251127171934-4e34e03a991a // indirect
	github.com/tabeth/kiroku-core/libs/fdb v0.0.0-00010101000000-000000000000 // indirect
	go.uber.org/atomic v1.9.0 // indirect
)
