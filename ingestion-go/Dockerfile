FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/ingestion ./cmd/ingestion

FROM gcr.io/distroless/static-debian12:nonroot AS prod

COPY --from=builder /bin/ingestion /ingestion

ENTRYPOINT ["/ingestion"]
