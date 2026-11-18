#include "admin_http.hpp"
#include "net/socket.hpp"
#include "pg/auth.hpp"

#include <chrono>
#include <sstream>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

namespace air::gw {

namespace {

struct FdGuard {
    int fd = -1;
    ~FdGuard() {
        if (fd >= 0) ::close(fd);
    }
};

bool ends_with(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

} // namespace

struct AdminHTTPServer::HttpRequest {
    std::string raw;
    std::string method;
    std::string path;
    std::string query;
    std::string body;

    std::string header(const std::string& name) const {
        std::istringstream hs(raw);
        std::string line;
        std::getline(hs, line);
        while (std::getline(hs, line)) {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) break;
            auto col = line.find(':');
            if (col == std::string::npos) continue;
            if (iequals(trim_ascii(line.substr(0, col)), name)) {
                return trim_ascii(line.substr(col + 1));
            }
        }
        return "";
    }

    std::string query_param(const std::string& key) const {
        std::string needle = key + "=";
        size_t pos = 0;
        while (pos < query.size()) {
            size_t amp = query.find('&', pos);
            std::string part = (amp == std::string::npos)
                                   ? query.substr(pos)
                                   : query.substr(pos, amp - pos);
            if (part.rfind(needle, 0) == 0) return part.substr(needle.size());
            if (amp == std::string::npos) break;
            pos = amp + 1;
        }
        return "";
    }
};

AdminHTTPServer::AdminHTTPServer(std::string addr,
                                 std::string internal_token,
                                 TenantDiscovery& discovery,
                                 PoolManager& pools,
                                 MetricsRegistry& metrics)
    : addr_(std::move(addr)),
      internal_token_(std::move(internal_token)),
      discovery_(discovery),
      pools_(pools),
      metrics_(metrics) {}

AdminHTTPServer::~AdminHTTPServer() { stop(); }

bool AdminHTTPServer::start() {
    if (addr_.empty()) return true;
    listen_fd_ = listen_tcp_addr(addr_);
    if (listen_fd_ < 0) return false;
    running_.store(true);
    worker_ = std::thread([this]() { accept_loop(); });
    return true;
}

void AdminHTTPServer::stop() {
    running_.store(false);
    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (worker_.joinable()) worker_.join();
    std::unique_lock<std::mutex> lk(http_mu_);
    http_cv_.wait_for(lk, std::chrono::seconds(3), [this]() {
        return active_http_.load(std::memory_order_acquire) == 0;
    });
}

void AdminHTTPServer::accept_loop() {
    while (running_.load()) {
        int cfd = ::accept(listen_fd_, nullptr, nullptr);
        if (cfd < 0) {
            if (!running_.load()) break;
            continue;
        }
        if (active_http_.load(std::memory_order_relaxed) >= 64) {
            send_http(cfd, 503, "Service Unavailable", R"({"error":"admin http busy"})");
            ::close(cfd);
            continue;
        }
        active_http_.fetch_add(1, std::memory_order_relaxed);
        std::thread([this, cfd]() {
            handle_http(cfd);
            if (active_http_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                std::lock_guard<std::mutex> lk(http_mu_);
                http_cv_.notify_all();
            }
        }).detach();
    }
}

void AdminHTTPServer::send_http(int fd,
                                int code,
                                const std::string& status,
                                const std::string& body,
                                const std::string& content_type) {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << code << " " << status << "\r\n"
        << "Content-Type: " << content_type << "\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n\r\n"
        << body;
    std::string resp = oss.str();
    size_t off = 0;
    while (off < resp.size()) {
        ssize_t n = ::send(fd, resp.data() + off, resp.size() - off, MSG_NOSIGNAL);
        if (n <= 0) break;
        off += static_cast<size_t>(n);
    }
}

std::string AdminHTTPServer::status_text(int code) {
    switch (code) {
        case 200: return "OK";
        case 201: return "Created";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 409: return "Conflict";
        case 502: return "Bad Gateway";
        case 503: return "Service Unavailable";
        default: return "OK";
    }
}

bool AdminHTTPServer::is_authorized(const HttpRequest& req) const {
    if (internal_token_.empty()) return true;
    if (req.header("X-Air-Internal-Token") == internal_token_) return true;
    std::string auth = req.header("Authorization");
    return auth.size() > 7 &&
           iequals(auth.substr(0, 7), "Bearer ") &&
           trim_ascii(auth.substr(7)) == internal_token_;
}

bool AdminHTTPServer::handle_databases_route(int fd, const HttpRequest& req) {
    // Normalize /admin/v1/databases* -> /v1/databases*
    std::string db_path = req.path;
    if (db_path.rfind("/admin/v1/databases", 0) == 0) {
        db_path = db_path.substr(6);
    }

    if (db_path == "/v1/databases" && (req.method == "POST" || req.method == "GET")) {
        if (!is_authorized(req)) {
            send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
            return true;
        }
        int code = 0;
        std::string resp;
        if (req.method == "POST") {
            discovery_.create_database(
                detail::json_extract_string(req.body, "tenant_id"),
                detail::json_extract_string(req.body, "account_id"),
                req.header("Idempotency-Key"),
                code, resp);
        } else {
            discovery_.list_databases(req.query_param("account_id"), code, resp);
        }
        send_http(fd, code, status_text(code), resp.empty() ? "{}" : resp);
        return true;
    }

    const std::string db_prefix = "/v1/databases/";
    if (db_path.rfind(db_prefix, 0) != 0 || db_path.size() <= db_prefix.size()) {
        return false;
    }
    if (!is_authorized(req)) {
        send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
        return true;
    }

    std::string rest = db_path.substr(db_prefix.size());
    std::string tid = rest;
    std::string action;
    if (auto slash = rest.find('/'); slash != std::string::npos) {
        tid = rest.substr(0, slash);
        action = rest.substr(slash + 1);
    }
    if (!validate_tenant_id(tid)) {
        send_http(fd, 400, "Bad Request", R"({"error":"invalid tenant id"})");
        return true;
    }

    if (action.empty() && (req.method == "GET" || req.method == "DELETE")) {
        int code = 0;
        std::string resp;
        if (req.method == "DELETE") {
            std::string derr;
            if (!pools_.request_drain(tid, 2000, derr)) {
                send_http(fd, 409, "Conflict", std::string(R"({"error":")") + json_escape(derr) + R"("})");
                return true;
            }
            pools_.remove_tenant(tid);
            discovery_.evict_cache(tid);
            discovery_.delete_database(tid, code, resp);
        } else {
            discovery_.get_database(tid, code, resp);
        }
        send_http(fd, code, status_text(code), resp.empty() ? "{}" : resp);
        return true;
    }

    if (req.method == "POST" && (action == "suspend" || action == "start" || action == "wake" ||
                                 action == "lock" || action == "unlock")) {
        if (action == "wake") action = "start";
        if (action == "suspend" || action == "lock") {
            std::string derr;
            if (!pools_.request_drain(tid, 3000, derr)) {
                send_http(fd, 409, "Conflict", std::string(R"({"error":")") + json_escape(derr) + R"("})");
                return true;
            }
            if (action == "lock") pools_.remove_tenant(tid);
        }
        int code = 0;
        std::string resp;
        if (action == "suspend") discovery_.suspend_database(tid, code, resp);
        else if (action == "start") discovery_.start_database(tid, code, resp);
        else if (action == "lock") discovery_.lock_database(tid, code, resp);
        else discovery_.unlock_database(tid, code, resp);
        send_http(fd, code, status_text(code), resp.empty() ? "{}" : resp);
        return true;
    }

    return false;
}

bool AdminHTTPServer::handle_internal_route(int fd, const HttpRequest& req) {
    if (req.method == "GET" && req.path == "/metrics") {
        if (!is_authorized(req)) {
            send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
            return true;
        }
        send_http(fd, 200, "OK", metrics_.to_prometheus(pools_.snapshots()),
                  "text/plain; version=0.0.4; charset=utf-8");
        return true;
    }

    if (req.method == "GET" && req.path == "/internal/v1/metrics") {
        if (!is_authorized(req)) {
            send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
            return true;
        }
        send_http(fd, 200, "OK",
                  metrics_.to_json(pools_.snapshots(),
                                   req.query_param("tenant_id"),
                                   req.query_param("user")));
        return true;
    }

    if (req.method == "GET" && req.path == "/internal/v1/tenants") {
        if (!is_authorized(req)) {
            send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
            return true;
        }
        auto list = discovery_.list_cached();
        std::ostringstream out;
        out << "{\"tenants\":[";
        for (size_t i = 0; i < list.size(); ++i) {
            if (i > 0) out << ",";
            out << "{\"tenant_id\":\"" << list[i].tenant_id
                << "\",\"state\":\"" << list[i].state
                << "\",\"port\":" << list[i].port << "}";
        }
        out << "]}";
        send_http(fd, 200, "OK", out.str());
        return true;
    }

    const std::string prefix = "/internal/v1/tenants/";
    const std::string limits_suffix = "/limits";
    if (req.method == "POST" && req.path.rfind(prefix, 0) == 0 &&
        req.path.size() > prefix.size() + limits_suffix.size() &&
        ends_with(req.path, limits_suffix)) {
        if (!is_authorized(req)) {
            send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
            return true;
        }
        std::string tenant_id = req.path.substr(prefix.size(), req.path.size() - prefix.size() - limits_suffix.size());
        if (!validate_tenant_id(tenant_id)) {
            send_http(fd, 400, "Bad Request", R"({"error":"invalid tenant id"})");
            return true;
        }
        int max_client_conns = detail::json_extract_int(req.body, "max_client_conns");
        int max_backends = detail::json_extract_int(req.body, "max_backends");
        if (max_client_conns > 0) metrics_.set_tenant_client_limit(tenant_id, max_client_conns);
        if (max_backends > 0) pools_.set_tenant_max_backends(tenant_id, max_backends);
        std::ostringstream out;
        out << "{\"status\":\"updated\",\"tenant_id\":\"" << json_escape(tenant_id)
            << "\",\"max_client_conns\":" << metrics_.get_tenant_client_limit(tenant_id) << "}";
        send_http(fd, 200, "OK", out.str());
        return true;
    }

    const std::string drain_suffix = "/drain";
    if (req.method == "POST" && req.path.rfind(prefix, 0) == 0 &&
        req.path.size() > prefix.size() + drain_suffix.size() &&
        ends_with(req.path, drain_suffix)) {
        if (!is_authorized(req)) {
            send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
            return true;
        }
        std::string tenant_id = req.path.substr(prefix.size(), req.path.size() - prefix.size() - drain_suffix.size());
        if (!validate_tenant_id(tenant_id)) {
            send_http(fd, 400, "Bad Request", R"({"error":"invalid tenant id"})");
            return true;
        }
        int timeout_ms = 2000;
        if (std::string tval = req.query_param("timeout"); !tval.empty()) {
            try {
                if (tval.size() > 2 && tval.substr(tval.size() - 2) == "ms") {
                    timeout_ms = std::stoi(tval.substr(0, tval.size() - 2));
                } else if (tval.back() == 's') {
                    timeout_ms = std::stoi(tval.substr(0, tval.size() - 1)) * 1000;
                }
            } catch (...) {}
        }
        std::string err;
        if (!pools_.request_drain(tenant_id, timeout_ms, err)) {
            send_http(fd, 409, "Conflict", std::string(R"({"error":")") + err + R"("})");
            return true;
        }
        send_http(fd, 200, "OK", std::string(R"({"status":"drained","tenant_id":")") + tenant_id + R"("})");
        return true;
    }

    return false;
}

void AdminHTTPServer::handle_http(int fd) {
    FdGuard guard{fd};
    struct timeval tv{3, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    char buf[4096];
    ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) return;
    buf[n] = '\0';

    HttpRequest req;
    req.raw.assign(buf, static_cast<size_t>(n));
    std::istringstream iss(req.raw);
    std::string target;
    iss >> req.method >> target;
    req.path = target;
    if (auto qpos = target.find('?'); qpos != std::string::npos) {
        req.path = target.substr(0, qpos);
        req.query = target.substr(qpos + 1);
    }
    if (auto hdr_end = req.raw.find("\r\n\r\n"); hdr_end != std::string::npos) {
        req.body = req.raw.substr(hdr_end + 4);
    }

    if (req.method == "GET" && req.path == "/healthz") {
        send_http(fd, 200, "OK", R"({"status":"ok"})");
        return;
    }
    if (handle_databases_route(fd, req)) return;
    if (handle_internal_route(fd, req)) return;

    send_http(fd, 404, "Not Found", R"({"error":"not found"})");
}

} // namespace air::gw
