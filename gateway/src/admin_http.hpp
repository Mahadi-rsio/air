#pragma once

#include "discovery.hpp"
#include "metrics.hpp"
#include "pg_wire.hpp"
#include "pooler.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <netdb.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace air::gw {

inline int listen_tcp_addr(const std::string& addr_str) {
    auto colon = addr_str.rfind(':');
    if (colon == std::string::npos) return -1;
    std::string host = addr_str.substr(0, colon);
    std::string port = addr_str.substr(colon + 1);
    if (host.empty()) host = "0.0.0.0";

    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    struct addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0 || !res) {
        return -1;
    }
    int fd = -1;
    for (auto* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        if (::bind(fd, p->ai_addr, p->ai_addrlen) == 0 && ::listen(fd, 128) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    return fd;
}

class AdminHTTPServer {
public:
    AdminHTTPServer(std::string addr, std::string internal_token,
                    TenantDiscovery& discovery, PoolManager& pools,
                    MetricsRegistry& metrics)
        : addr_(std::move(addr)),
          internal_token_(std::move(internal_token)),
          discovery_(discovery),
          pools_(pools),
          metrics_(metrics) {}

    ~AdminHTTPServer() { stop(); }

    bool start() {
        if (addr_.empty()) return true;
        listen_fd_ = listen_tcp_addr(addr_);
        if (listen_fd_ < 0) return false;
        running_.store(true);
        worker_ = std::thread([this]() { accept_loop(); });
        return true;
    }

    void stop() {
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

private:
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

    void accept_loop() {
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

    static void send_http(int fd, int code, const std::string& status,
                          const std::string& body,
                          const std::string& content_type = "application/json") {
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

    void handle_http(int fd) {
        struct timeval tv{3, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        char buf[4096];
        ssize_t n = ::recv(fd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            ::close(fd);
            return;
        }
        buf[n] = '\0';
        std::string req(buf, static_cast<size_t>(n));
        std::istringstream iss(req);
        std::string method, target;
        iss >> method >> target;

        std::string path = target;
        std::string query;
        if (auto qpos = target.find('?'); qpos != std::string::npos) {
            path = target.substr(0, qpos);
            query = target.substr(qpos + 1);
        }

        if (method == "GET" && path == "/healthz") {
            send_http(fd, 200, "OK", R"({"status":"ok"})");
            ::close(fd);
            return;
        }

        auto get_header = [&](const std::string& name) -> std::string {
            std::istringstream hs(req);
            std::string line;
            std::getline(hs, line);
            while (std::getline(hs, line)) {
                if (!line.empty() && line.back() == '\r') line.pop_back();
                if (line.empty()) break;
                auto col = line.find(':');
                if (col == std::string::npos) continue;
                std::string k = trim_ascii(line.substr(0, col));
                if (iequals(k, name)) return trim_ascii(line.substr(col + 1));
            }
            return "";
        };

        auto is_authorized = [&]() -> bool {
            if (internal_token_.empty()) return true;
            std::string tok = get_header("X-Air-Internal-Token");
            if (tok == internal_token_) return true;
            std::string auth = get_header("Authorization");
            if (auth.size() > 7 && iequals(auth.substr(0, 7), "Bearer ") &&
                trim_ascii(auth.substr(7)) == internal_token_) {
                return true;
            }
            return false;
        };

        auto parse_query_param = [&](const std::string& key) -> std::string {
            std::string needle = key + "=";
            size_t pos = 0;
            while (pos < query.size()) {
                size_t amp = query.find('&', pos);
                std::string part = (amp == std::string::npos) ? query.substr(pos) : query.substr(pos, amp - pos);
                if (part.rfind(needle, 0) == 0) return part.substr(needle.size());
                if (amp == std::string::npos) break;
                pos = amp + 1;
            }
            return "";
        };

        std::string req_body;
        if (auto hdr_end = req.find("\r\n\r\n"); hdr_end != std::string::npos) {
            req_body = req.substr(hdr_end + 4);
        }

        auto status_text = [](int code) -> std::string {
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
        };

        // Normalize /admin/v1/databases* -> /v1/databases*
        std::string db_path = path;
        if (db_path.rfind("/admin/v1/databases", 0) == 0) {
            db_path = db_path.substr(6); // strip "/admin"
        }

        if (db_path == "/v1/databases" && (method == "POST" || method == "GET")) {
            if (!is_authorized()) {
                send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
                ::close(fd);
                return;
            }
            int code = 0;
            std::string resp;
            if (method == "POST") {
                std::string tid = detail::json_extract_string(req_body, "tenant_id");
                std::string aid = detail::json_extract_string(req_body, "account_id");
                std::string idem_key = get_header("Idempotency-Key");
                discovery_.create_database(tid, aid, idem_key, code, resp);
            } else {
                std::string aid = parse_query_param("account_id");
                discovery_.list_databases(aid, code, resp);
            }
            send_http(fd, code, status_text(code), resp.empty() ? "{}" : resp);
            ::close(fd);
            return;
        }

        const std::string db_prefix = "/v1/databases/";
        if (db_path.rfind(db_prefix, 0) == 0 && db_path.size() > db_prefix.size()) {
            if (!is_authorized()) {
                send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
                ::close(fd);
                return;
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
                ::close(fd);
                return;
            }

            if (action.empty() && (method == "GET" || method == "DELETE")) {
                int code = 0;
                std::string resp;
                if (method == "DELETE") {
                    std::string derr;
                    if (!pools_.request_drain(tid, 2000, derr)) {
                        send_http(fd, 409, "Conflict", std::string(R"({"error":")") + json_escape(derr) + R"("})");
                        ::close(fd);
                        return;
                    }
                    pools_.remove_tenant(tid);
                    discovery_.evict_cache(tid);
                    discovery_.delete_database(tid, code, resp);
                } else {
                    discovery_.get_database(tid, code, resp);
                }
                send_http(fd, code, status_text(code), resp.empty() ? "{}" : resp);
                ::close(fd);
                return;
            }

            if (method == "POST" && (action == "suspend" || action == "start" || action == "wake" ||
                                     action == "lock" || action == "unlock")) {
                if (action == "wake") action = "start";
                if (action == "suspend" || action == "lock") {
                    std::string derr;
                    if (!pools_.request_drain(tid, 3000, derr)) {
                        send_http(fd, 409, "Conflict", std::string(R"({"error":")") + json_escape(derr) + R"("})");
                        ::close(fd);
                        return;
                    }
                    if (action == "lock") {
                        pools_.remove_tenant(tid);
                    }
                }
                int code = 0;
                std::string resp;
                if (action == "suspend") {
                    discovery_.suspend_database(tid, code, resp);
                } else if (action == "start") {
                    discovery_.start_database(tid, code, resp);
                } else if (action == "lock") {
                    discovery_.lock_database(tid, code, resp);
                } else if (action == "unlock") {
                    discovery_.unlock_database(tid, code, resp);
                }
                send_http(fd, code, status_text(code), resp.empty() ? "{}" : resp);
                ::close(fd);
                return;
            }
        }

        if (method == "GET" && path == "/metrics") {
            if (!is_authorized()) {
                send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
                ::close(fd);
                return;
            }
            send_http(fd, 200, "OK", metrics_.to_prometheus(pools_.snapshots()),
                      "text/plain; version=0.0.4; charset=utf-8");
            ::close(fd);
            return;
        }

        if (method == "GET" && path == "/internal/v1/metrics") {
            if (!is_authorized()) {
                send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
                ::close(fd);
                return;
            }
            std::string tf = parse_query_param("tenant_id");
            std::string uf = parse_query_param("user");
            send_http(fd, 200, "OK", metrics_.to_json(pools_.snapshots(), tf, uf));
            ::close(fd);
            return;
        }

        if (method == "GET" && path == "/internal/v1/tenants") {
            if (!is_authorized()) {
                send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
                ::close(fd);
                return;
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
            ::close(fd);
            return;
        }

        const std::string prefix = "/internal/v1/tenants/";
        const std::string limits_suffix = "/limits";
        if (method == "POST" &&
            path.rfind(prefix, 0) == 0 &&
            path.size() > prefix.size() + limits_suffix.size() &&
            path.compare(path.size() - limits_suffix.size(), limits_suffix.size(), limits_suffix) == 0) {
            if (!is_authorized()) {
                send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
                ::close(fd);
                return;
            }
            std::string tenant_id = path.substr(prefix.size(), path.size() - prefix.size() - limits_suffix.size());
            if (!validate_tenant_id(tenant_id)) {
                send_http(fd, 400, "Bad Request", R"({"error":"invalid tenant id"})");
                ::close(fd);
                return;
            }
            int max_client_conns = detail::json_extract_int(req_body, "max_client_conns");
            int max_backends = detail::json_extract_int(req_body, "max_backends");
            if (max_client_conns > 0) {
                metrics_.set_tenant_client_limit(tenant_id, max_client_conns);
            }
            if (max_backends > 0) {
                pools_.set_tenant_max_backends(tenant_id, max_backends);
            }
            std::ostringstream out;
            out << "{\"status\":\"updated\",\"tenant_id\":\"" << json_escape(tenant_id)
                << "\",\"max_client_conns\":" << metrics_.get_tenant_client_limit(tenant_id) << "}";
            send_http(fd, 200, "OK", out.str());
            ::close(fd);
            return;
        }

        const std::string suffix = "/drain";
        if (method == "POST" &&
            path.rfind(prefix, 0) == 0 &&
            path.size() > prefix.size() + suffix.size() &&
            path.compare(path.size() - suffix.size(), suffix.size(), suffix) == 0) {
            if (!is_authorized()) {
                send_http(fd, 401, "Unauthorized", R"({"error":"unauthorized"})");
                ::close(fd);
                return;
            }
            std::string tenant_id = path.substr(prefix.size(), path.size() - prefix.size() - suffix.size());
            if (!validate_tenant_id(tenant_id)) {
                send_http(fd, 400, "Bad Request", R"({"error":"invalid tenant id"})");
                ::close(fd);
                return;
            }
            int timeout_ms = 2000;
            if (auto tpos = query.find("timeout="); tpos != std::string::npos) {
                std::string tval = query.substr(tpos + 8);
                auto amp = tval.find('&');
                if (amp != std::string::npos) tval = tval.substr(0, amp);
                try {
                    if (tval.size() > 2 && tval.substr(tval.size() - 2) == "ms") {
                        timeout_ms = std::stoi(tval.substr(0, tval.size() - 2));
                    } else if (!tval.empty() && tval.back() == 's') {
                        timeout_ms = std::stoi(tval.substr(0, tval.size() - 1)) * 1000;
                    }
                } catch (...) {}
            }
            std::string err;
            if (!pools_.request_drain(tenant_id, timeout_ms, err)) {
                send_http(fd, 409, "Conflict", std::string(R"({"error":")") + err + R"("})");
                ::close(fd);
                return;
            }
            send_http(fd, 200, "OK", std::string(R"({"status":"drained","tenant_id":")") + tenant_id + R"("})");
            ::close(fd);
            return;
        }

        send_http(fd, 404, "Not Found", R"({"error":"not found"})");
        ::close(fd);
    }
};

} // namespace air::gw
