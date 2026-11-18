#pragma once

#include "discovery.hpp"
#include "metrics.hpp"
#include "pooler.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

namespace air::gw {

/**
 * Lightweight HTTP/1.1 admin & observability server exposing:
 *   - GET  /healthz
 *   - GET  /metrics                  (Prometheus)
 *   - GET  /internal/v1/metrics      (JSON)
 *   - GET  /internal/v1/tenants
 *   - POST /internal/v1/tenants/<id>/limits
 *   - POST /internal/v1/tenants/<id>/drain
 *   - /v1/databases* (and /admin/v1/databases*) proxied to Control Plane gRPC
 */
class AdminHTTPServer {
public:
    AdminHTTPServer(std::string addr,
                    std::string internal_token,
                    TenantDiscovery& discovery,
                    PoolManager& pools,
                    MetricsRegistry& metrics);
    ~AdminHTTPServer();

    bool start();
    void stop();

private:
    struct HttpRequest;

    std::string addr_;
    std::string internal_token_;
    TenantDiscovery& discovery_;
    PoolManager& pools_;
    MetricsRegistry& metrics_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread worker_;
    std::atomic<int> active_http_{0};
    std::mutex http_mu_;
    std::condition_variable http_cv_;

    void accept_loop();
    void handle_http(int fd);
    bool is_authorized(const HttpRequest& req) const;
    bool handle_databases_route(int fd, const HttpRequest& req);
    bool handle_internal_route(int fd, const HttpRequest& req);

    static void send_http(int fd,
                          int code,
                          const std::string& status,
                          const std::string& body,
                          const std::string& content_type = "application/json");
    static std::string status_text(int code);
};

} // namespace air::gw
