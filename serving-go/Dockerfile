# Stage 1: build
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/serving ./cmd/serving

# Stage 2: minimal runtime
FROM alpine:3.23
RUN apk add --no-cache wget
COPY --from=builder /bin/serving /bin/serving
EXPOSE 8080
ENTRYPOINT ["/bin/serving"]
