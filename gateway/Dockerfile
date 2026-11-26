# syntax=docker/dockerfile:1.7

# Stage 1: Build standalone Go air-gateway binary
FROM golang:1.26-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out-air-gateway ./cmd/air-gateway

# Stage 2: Minimal runtime image for air-gateway
FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --uid 10001 airgw

COPY --from=builder /out-air-gateway /usr/local/bin/air-gateway

USER airgw
ENV AIR_GATEWAY_ADDR=0.0.0.0:5432 \
    AIR_GATEWAY_ADMIN_ADDR=127.0.0.1:8081 \
    AIR_POOL_MODE=true

EXPOSE 5432 8081
ENTRYPOINT ["/usr/local/bin/air-gateway"]
