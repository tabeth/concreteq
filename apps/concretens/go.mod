module github.com/kiroku-inc/kiroku-core/apps/concretens

go 1.24

replace github.com/tabeth/kiroku-core/libs/fdb => ../../libs/fdb

replace github.com/tabeth/kiroku-core/libs/kms => ../../libs/kms

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20250501205732-f0ed69ea389a
	github.com/google/uuid v1.6.0
	github.com/tabeth/kiroku-core/libs/fdb v0.0.0-00010101000000-000000000000
)
