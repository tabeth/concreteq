.PHONY: all build clean test extension

all: build extension

build:
	cd cmd/concretesql && go build -tags sqlite_json -o ../../concretesql

extension:
	cd cmd/concretesql-extension && go build -buildmode=c-shared -tags sqlite_json -o ../../concretesql.so

test:
	go test -tags sqlite_json ./...

clean:
	rm -f concretesql concretesql.so
