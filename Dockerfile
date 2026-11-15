# syntax=docker/dockerfile:1.7

# Stage 1: Build air-server Go binary
FROM golang:1.25-bookworm AS go-builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY migrations/ ./migrations/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/air-server ./cmd/air-server

# Stage 2: Build supautils extension against PostgreSQL 17
FROM debian:bookworm-slim AS pg-builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg git build-essential clang llvm \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL --retry 5 --retry-all-errors https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    postgresql-17 postgresql-server-dev-17 \
    && git clone --depth 1 https://github.com/supabase/supautils.git /tmp/supautils \
    && make -C /tmp/supautils PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config \
    && make -C /tmp/supautils PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config install

# Stage 3: Production AIR single-cell runtime with PostgreSQL 17 + supautils
FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg zstd gosu \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL --retry 5 --retry-all-errors https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    postgresql-17 postgresql-contrib-17 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/air && chown -R postgres:postgres /var/lib/air && chmod 0755 /var/lib/air

COPY --from=pg-builder /usr/lib/postgresql/17/lib/supautils.so /usr/lib/postgresql/17/lib/supautils.so
COPY --from=go-builder /out/air-server /usr/local/bin/air-server
COPY migrations/ /app/migrations/

WORKDIR /app
ENV AIR_DATA_DIR=/var/lib/air \
    AIR_PG_BIN_DIR=/usr/lib/postgresql/17/bin \
    AIR_HTTP_ADDR=0.0.0.0:8080 \
    AIR_GATEWAY_ADDR=0.0.0.0:5432 \
    AIR_POOL_MODE=true

EXPOSE 8080 5432
ENTRYPOINT ["/bin/sh", "-c", "mkdir -p \"${AIR_DATA_DIR:-/var/lib/air}\"; if [ \"$(id -u)\" = \"0\" ]; then chown -R postgres:postgres \"${AIR_DATA_DIR:-/var/lib/air}\"; exec gosu postgres /usr/local/bin/air-server \"$@\"; fi; exec /usr/local/bin/air-server \"$@\"", "--"]
