# syntax=docker/dockerfile:1
FROM golang:1.24-bookworm AS go-builder
WORKDIR /src
COPY go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY main.go ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /stage0-runner .

FROM postgres:17-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates util-linux quota iproute2 iputils-ping procps \
    build-essential postgresql-server-dev-17 \
    && rm -rf /var/lib/apt/lists/*

COPY supautils /tmp/supautils
RUN make -C /tmp/supautils && make -C /tmp/supautils install && rm -rf /tmp/supautils

COPY --from=go-builder /stage0-runner /usr/local/bin/stage0-runner
RUN mkdir -p /vol && chown -R postgres:postgres /vol
ENTRYPOINT ["/usr/local/bin/stage0-runner"]
