# syntax=docker/dockerfile:1.7

# Stage 1: Build standalone C++ air-gateway binary
FROM debian:bookworm-slim AS builder
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ make pkg-config libpq-dev libssl-dev \
    libgrpc++-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/gateway
COPY proto/airdb.proto /src/proto/airdb.proto
COPY gateway/ ./
RUN protoc -I/src/proto --cpp_out=src/pb --grpc_out=src/pb \
      --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin /src/proto/airdb.proto \
    && make clean && make -j"$(nproc)" all && cp air-gateway /out-air-gateway

# Stage 2: Minimal runtime image for air-gateway
FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libpq5 libssl3 libgrpc++1.51 libprotobuf32 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --uid 10001 airgw

COPY --from=builder /out-air-gateway /usr/local/bin/air-gateway
RUN ldd /usr/local/bin/air-gateway && (! ldd /usr/local/bin/air-gateway | grep -q "not found")

USER airgw
ENV AIR_GATEWAY_ADDR=0.0.0.0:5432 \
    AIR_GATEWAY_ADMIN_ADDR=0.0.0.0:8081 \
    AIR_POOL_MODE=true

EXPOSE 5432 8081
ENTRYPOINT ["/usr/local/bin/air-gateway"]
