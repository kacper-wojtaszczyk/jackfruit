# Stage 1: builder
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/serving ./cmd/serving

# Stage 2: dev (extends builder — Go toolchain + warm module cache retained)
FROM builder AS dev

RUN apk add --no-cache curl

# Rebuild without stripped symbols/DWARF and with optimisations off so Delve
# can read debug info. (-s -w from the builder stage makes dlv exec fail.)
RUN CGO_ENABLED=0 GOOS=linux go build -gcflags="all=-N -l" -o /bin/serving ./cmd/serving

RUN go install github.com/go-delve/delve/cmd/dlv@latest

EXPOSE 8080 2345

ENTRYPOINT ["dlv", "exec", "/bin/serving", "--listen=:2345", "--headless", "--api-version=2", "--accept-multiclient", "--"]
