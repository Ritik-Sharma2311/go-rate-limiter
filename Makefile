.PHONY: test test-redis run

test:
	go vet ./...
	go test -race -count=1 ./...

# Also runs the real-Redis integration test.
test-redis:
	REDIS_ADDR=localhost:6379 go test -race -count=1 ./...

run:
	go run ./cmd/gateway
