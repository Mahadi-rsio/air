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
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /stage2-runner ./cmd/stage2-runner

FROM postgres:17-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates util-linux quota iproute2 iputils-ping procps \
    build-essential postgresql-server-dev-17 pgbouncer zstd \
    && rm -rf /var/lib/apt/lists/*

COPY supautils /tmp/supautils
RUN make -C /tmp/supautils && make -C /tmp/supautils install && rm -rf /tmp/supautils

COPY --from=go-builder /stage2-runner /usr/local/bin/stage2-runner
COPY config/alertmanager-rules.yaml /etc/air/alertmanager-rules.yaml
COPY runbooks/dr/DR-01-to-12.md /etc/air/DR-01-to-12.md
RUN mkdir -p /vol && chown -R postgres:postgres /vol
ENTRYPOINT ["/usr/local/bin/stage2-runner"]
