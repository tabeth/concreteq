module github.com/tabeth/concretedb

go 1.24

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20250501205732-f0ed69ea389a
	github.com/tabeth/kiroku-core/libs/fdb v0.0.0-00010101000000-000000000000
)

require (
	github.com/aws/aws-sdk-go v1.55.8 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
)

replace (
	github.com/tabeth/kiroku-core/libs/fdb => ../../libs/fdb
	github.com/tabeth/kiroku-core/libs/kms => ../../libs/kms
)
