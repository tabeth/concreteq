module github.com/tabeth/concreteq

go 1.24

require github.com/go-chi/chi/v5 v5.2.2

require github.com/stretchr/testify v1.10.0

require github.com/google/uuid v1.6.0

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20250501205732-f0ed69ea389a
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/tabeth/kiroku-core/libs/fdb v0.0.0
	github.com/tabeth/kiroku-core/libs/kms v0.0.0
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/tabeth/kiroku-core/libs/fdb => ../../libs/fdb
	github.com/tabeth/kiroku-core/libs/kms => ../../libs/kms
)
