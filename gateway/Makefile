.PHONY: all test clean

all: air-gateway

air-gateway:
	cd .. && go build -o gateway/air-gateway ./cmd/air-gateway

test:
	cd .. && go test -v -race ./internal/gateway/...

clean:
	rm -f air-gateway
