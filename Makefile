.PHONY: build test run clean docker-build docker-up docker-down

# Build the application
build:
	go build -o bin/server ./cmd/server

# Run tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run e2e test scenario
test-e2e:
	go run ./tests/e2e/scenarios/001_basic_minute_rollup/scenario.go

# Run the application
run:
	go run ./cmd/server/main.go

# Clean build artifacts
clean:
	rm -rf bin/
	rm -rf ./.tmp/
	rm -f coverage.out coverage.html

# Docker commands
docker-build:
	docker-compose build

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

