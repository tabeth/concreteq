module github.com/tabeth/concretedb

go 1.24

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20250501205732-f0ed69ea389a
	github.com/aws/aws-sdk-go v1.55.8
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.11.1
	github.com/tabeth/kiroku-core/libs/fdb v0.0.0-00010101000000-000000000000
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/tabeth/kiroku-core/libs/fdb => ../../libs/fdb
	github.com/tabeth/kiroku-core/libs/kms => ../../libs/kms
)
