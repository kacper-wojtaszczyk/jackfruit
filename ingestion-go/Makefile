.PHONY: build run test test-integration clean
# Default target
all: build test
# Build the ingestion binary
build:
	go build -o bin/ingestion ./cmd/ingestion
# Run
run:
	go run ./cmd/ingestion ${ARGS}
# Run unit tests (skip integration tests)
test:
	go test -short -cover ./...
# Run all tests including integration (requires MinIO running)
test-integration:
	go test -cover ./...
# Clean build artifacts
clean:
	rm -rf bin/
	go clean -cache
