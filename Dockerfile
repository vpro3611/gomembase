# Multi-stage build for GObase server
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build statically linked binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o gobase-server main.go

# Minimal runtime image
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy compiled binary
COPY --from=builder /app/gobase-server /app/gobase-server

# Default environment variables
ENV WAL_PATH=walfile.wal \
    SNAPSHOT_PATH=snapshotfile.rdb \
    WAL_FLUSH_INTERVAL=1s \
    SNAPSHOT_INTERVAL=5m \
    EXPIRATION_CLEANUP_INTERVAL=10s \
    MAX_SUB_INSTANCES=5 \
    PORT=6381

# Default port exposed
EXPOSE 6381

ENTRYPOINT ["/app/gobase-server"]

