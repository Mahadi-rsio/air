# syntax=docker/dockerfile:1.7
FROM golang:1.24-bookworm AS gobuilder
WORKDIR /src
COPY go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download || true
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && CGO_ENABLED=0 go build -o /out/stage3-runner ./cmd/stage3-runner

FROM postgres:17-bookworm AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential postgresql-server-dev-17 zstd pgbouncer \
    ca-certificates procps iproute2 util-linux libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY supautils /tmp/supautils
RUN make -C /tmp/supautils && make -C /tmp/supautils install && rm -rf /tmp/supautils

COPY --from=gobuilder /out/stage3-runner /usr/local/bin/stage3-runner

ENV PATH="/usr/lib/postgresql/17/bin:${PATH}"
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/stage3-runner"]
