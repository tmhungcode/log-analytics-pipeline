# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o server ./cmd/server


# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates curl

WORKDIR /app

COPY --from=builder /build/server /app/server
COPY --from=builder /build/configs /app/configs

# Create directories expected by config:
# file_storage.root_dir: ./.tmp/file-storage  ->  /app/.tmp/file-storage
RUN mkdir -p /app/.tmp/file-storage

EXPOSE 8080

CMD ["/app/server"]
