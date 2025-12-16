.PHONY: build run test lint clean

# Default target
all: build

# Build the ingestion binary
build:
	go build -o bin/ingestion ./cmd/ingestion

# Run with hot reload (optional, requires 'air' or similar)
run:
	go run ./cmd/ingestion ${ARGS}

# Run tests
test:
	go test ./...

# Clean build artifacts
clean:
	rm -rf bin/
	go clean -cache
