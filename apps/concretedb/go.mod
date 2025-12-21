module github.com/tabeth/concretedb

go 1.24

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20250501205732-f0ed69ea389a
	github.com/go-chi/chi/v5 v5.2.2
	github.com/tabeth/kiroku-core/libs/fdb v0.0.0
	github.com/tabeth/kiroku-core/libs/kms v0.0.0
)

replace (
	github.com/tabeth/kiroku-core/libs/fdb => ../../libs/fdb
	github.com/tabeth/kiroku-core/libs/kms => ../../libs/kms
)
