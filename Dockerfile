# Multi-stage build for GObase server
FROM golang:1.22-alpine AS builder

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

# Default port exposed
EXPOSE 6381

ENTRYPOINT ["/app/gobase-server"]
