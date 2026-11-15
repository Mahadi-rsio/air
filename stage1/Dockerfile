# syntax=docker/dockerfile:1
FROM golang:1.24-bookworm AS go-builder
WORKDIR /src
COPY go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /pg-cell-agent ./cmd/pg-cell-agent && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /stage1-runner ./cmd/stage1-runner

FROM postgres:17-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates util-linux quota iproute2 iputils-ping procps \
    build-essential postgresql-server-dev-17 pgbouncer zstd \
    && rm -rf /var/lib/apt/lists/*

COPY supautils /tmp/supautils
RUN make -C /tmp/supautils && make -C /tmp/supautils install && rm -rf /tmp/supautils

COPY --from=go-builder /pg-cell-agent /usr/local/bin/pg-cell-agent
COPY --from=go-builder /stage1-runner /usr/local/bin/stage1-runner
RUN mkdir -p /vol && chown -R postgres:postgres /vol
ENTRYPOINT ["/usr/local/bin/stage1-runner"]
