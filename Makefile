.PHONY: build test vet lint clean

BIN := bin/beam
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

build:
	go build $(LDFLAGS) -o $(BIN) ./cmd/beam

test:
	go test -race ./...

vet:
	go vet ./...

clean:
	rm -rf bin/
