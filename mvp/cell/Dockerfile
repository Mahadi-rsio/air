# syntax=docker/dockerfile:1
FROM golang:1.24-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY main.go ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod tidy && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /cell-agent .

FROM postgres:17-bookworm
RUN rm -f /etc/apt/apt.conf.d/docker-clean && \
    echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates util-linux quota iproute2 procps
RUN mkdir -p /vol && chown -R postgres:postgres /vol
COPY --from=builder /cell-agent /usr/local/bin/cell-agent
ENV PG_BIN=/usr/lib/postgresql/17/bin
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/cell-agent"]
