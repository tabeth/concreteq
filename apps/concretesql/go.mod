module github.com/tabeth/concretesql

go 1.24.0

replace github.com/tabeth/kiroku-core/libs/fdb => ../../libs/fdb

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20250501205732-f0ed69ea389a
	github.com/google/uuid v1.6.0
	github.com/mattn/go-sqlite3 v1.14.32
	github.com/psanford/sqlite3vfs v0.0.0-20251127171934-4e34e03a991a
	github.com/stretchr/testify v1.11.1
	github.com/tabeth/kiroku-core/libs/fdb v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
