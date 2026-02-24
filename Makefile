.PHONY: build test vet lint clean docs release benchmark

BIN := bin/beam
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

build:
	go build $(LDFLAGS) -o $(BIN) ./cmd/beam

test:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

docs:
	go run $(LDFLAGS) ./cmd/beam gen-docs --dir docs/man --format man

clean:
	rm -rf bin/

benchmark: build
	scripts/benchmark.sh --beam ./bin/beam

release:
	goreleaser release --clean
