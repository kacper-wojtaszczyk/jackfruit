.PHONY: build run test clean

# Default target
all: build test

# Build the ingestion binary
build:
	go build -o bin/ingestion ./cmd/ingestion

# Run
run:
	go run ./cmd/ingestion ${ARGS}

# Run tests
test:
	go test -cover ./...

# Clean build artifacts
clean:
	rm -rf bin/
	go clean -cache
