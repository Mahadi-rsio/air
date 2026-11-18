# syntax=docker/dockerfile:1.7

# Stage 1: Build air-server Go binary
FROM golang:1.26-bookworm AS go-builder
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

# Stage 1b: Build C++ air-gateway binary
FROM debian:bookworm-slim AS cpp-builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ make pkg-config libpq-dev libcurl4-openssl-dev libssl-dev \
    libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src/gateway
COPY proto/airdb.proto /src/proto/airdb.proto
COPY gateway/ ./
RUN protoc -I/src/proto --cpp_out=src/pb --grpc_out=src/pb --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin /src/proto/airdb.proto \
    && make all && cp air-gateway /out-air-gateway

# Stage 2: Build patched PostgreSQL 17.12 (smgr_hook) + supautils + air_smgr
FROM debian:bookworm-slim AS pg-builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl bzip2 patch git build-essential pkg-config \
    bison flex libreadline-dev zlib1g-dev libssl-dev libxml2-dev uuid-dev libossp-uuid-dev \
    && curl -fsSL --retry 5 --retry-all-errors https://ftp.postgresql.org/pub/source/v17.12/postgresql-17.12.tar.bz2 -o /tmp/pg17.tar.bz2 \
    && tar -xjf /tmp/pg17.tar.bz2 -C /tmp \
    && rm -f /tmp/pg17.tar.bz2

COPY patches/pg17-smgr-hook.patch /tmp/pg17-smgr-hook.patch
RUN patch -d /tmp/postgresql-17.12 -p1 < /tmp/pg17-smgr-hook.patch \
    && cd /tmp/postgresql-17.12 \
    && ./configure --prefix=/usr/lib/postgresql/17 --with-openssl --with-libxml --with-uuid=ossp \
    && make -j"$(nproc)" world-bin \
    && make install-world-bin \
    && rm -rf /tmp/postgresql-17.12

RUN git clone --depth 1 https://github.com/supabase/supautils.git /tmp/supautils \
    && make -C /tmp/supautils PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config \
    && make -C /tmp/supautils PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config install \
    && rm -rf /tmp/supautils

COPY extensions/air_smgr /tmp/air_smgr
RUN make -C /tmp/air_smgr PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config \
    && make -C /tmp/air_smgr PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config install \
    && rm -rf /tmp/air_smgr

# Stage 3: Production AIR single-cell runtime with patched PostgreSQL 17 + supautils + air_smgr
FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg zstd gosu \
    libreadline8 zlib1g libssl3 libxml2 libossp-uuid16 libcurl4 \
    libgrpc++1.51 libprotobuf32 \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL --retry 5 --retry-all-errors https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    postgresql-17 postgresql-contrib-17 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/air && chown -R postgres:postgres /var/lib/air && chmod 0755 /var/lib/air

COPY --from=pg-builder /usr/lib/postgresql/17 /usr/lib/postgresql/17
COPY --from=go-builder /out/air-server /usr/local/bin/air-server
COPY --from=cpp-builder /out-air-gateway /usr/local/bin/air-gateway
RUN ldd /usr/local/bin/air-gateway && (! ldd /usr/local/bin/air-gateway | grep -q "not found")
COPY scripts/air-entrypoint.sh /usr/local/bin/air-entrypoint.sh
RUN chmod +x /usr/local/bin/air-entrypoint.sh
COPY migrations/ /app/migrations/

WORKDIR /app
ENV AIR_DATA_DIR=/var/lib/air \
    AIR_PG_BIN_DIR=/usr/lib/postgresql/17/bin \
    AIR_HTTP_ADDR=0.0.0.0:8080 \
    AIR_GATEWAY_ADDR=0.0.0.0:5432 \
    AIR_GATEWAY_ADMIN_ADDR=127.0.0.1:8081 \
    AIR_GATEWAY_ADMIN_URL=http://127.0.0.1:8081 \
    AIR_CONTROL_PLANE_URL=http://127.0.0.1:8080 \
    AIR_POOL_MODE=true

EXPOSE 8080 5432
ENTRYPOINT ["/usr/local/bin/air-entrypoint.sh"]
