#include "admin_http.hpp"
#include "config.hpp"
#include "discovery.hpp"
#include "metrics.hpp"
#include "pg_wire.hpp"
#include "pooler.hpp"

#include <atomic>
#include <csignal>
#include <curl/curl.h>
#include <iostream>
#include <poll.h>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_running{true};
int g_listen_fd = -1;

void signal_handler(int) {
    g_running.store(false);
    if (g_listen_fd >= 0) {
        ::shutdown(g_listen_fd, SHUT_RDWR);
        ::close(g_listen_fd);
        g_listen_fd = -1;
    }
}

void proxy_bidirectional(air::gw::Stream& client, air::gw::Stream& backend) {
    uint8_t buf[32768];
    auto pump_one = [&](air::gw::Stream& src, air::gw::Stream& dst) {
        while (true) {
            ssize_t n = 0;
            if (src.is_tls()) {
                // Handled via simple blocking read when TLS is active
                uint8_t b;
                if (!src.read_exact(&b, 1)) return;
                if (!dst.write_all(&b, 1)) return;
            } else {
                n = ::recv(src.fd(), buf, sizeof(buf), 0);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) return;
                if (!dst.write_all(buf, static_cast<size_t>(n))) return;
            }
        }
    };
    if (client.is_tls()) {
        std::thread t1([&]() { pump_one(client, backend); backend.close(); client.close(); });
        pump_one(backend, client);
        backend.close();
        client.close();
        t1.join();
        return;
    }

    struct pollfd pfds[2];
    pfds[0] = {client.fd(), POLLIN, 0};
    pfds[1] = {backend.fd(), POLLIN, 0};
    while (true) {
        int rc = ::poll(pfds, 2, 30000);
        if (rc < 0 && errno == EINTR) continue;
        if (rc <= 0) continue;
        if (pfds[0].revents & (POLLIN | POLLHUP | POLLERR)) {
            ssize_t n = ::recv(client.fd(), buf, sizeof(buf), 0);
            if (n <= 0 || !backend.write_all(buf, static_cast<size_t>(n))) break;
        }
        if (pfds[1].revents & (POLLIN | POLLHUP | POLLERR)) {
            ssize_t n = ::recv(backend.fd(), buf, sizeof(buf), 0);
            if (n <= 0 || !client.write_all(buf, static_cast<size_t>(n))) break;
        }
    }
}

struct GlobalConnGuard {
    air::gw::MetricsRegistry& metrics;
    ~GlobalConnGuard() { metrics.release_global(); }
};

struct TenantUserConnGuard {
    air::gw::MetricsRegistry& metrics;
    std::string tenant_id;
    std::string user;
    bool active = false;
    ~TenantUserConnGuard() {
        if (active) metrics.release_tenant_user(tenant_id, user);
    }
};

void handle_client(int cfd,
                   const air::gw::Config& cfg,
                   air::gw::TLSContext& tls_ctx,
                   air::gw::TenantDiscovery& discovery,
                   air::gw::PoolManager& pools,
                   air::gw::MetricsRegistry& metrics) {
    GlobalConnGuard global_guard{metrics};
    air::gw::Stream client(cfd);
    client.set_timeouts(5000, 5000);

    uint8_t hdr[8];
    if (!client.read_exact(hdr, 8)) return;
    uint32_t msg_len = air::gw::read_be32(hdr);
    uint32_t code = air::gw::read_be32(hdr + 4);

    std::string sni_host;
    bool tls_negotiated = false;

    if (msg_len == 8 && code == air::gw::kSSLRequestCode) {
        uint8_t yes = 'S';
        if (!client.write_all(&yes, 1)) return;
        SSL* ssl = tls_ctx.accept_tls(client.fd(), sni_host);
        if (!ssl) return;
        client.set_ssl(ssl);
        tls_negotiated = true;

        if (!client.read_exact(hdr, 8)) return;
        msg_len = air::gw::read_be32(hdr);
        code = air::gw::read_be32(hdr + 4);
    }

    if (cfg.require_tls && !tls_negotiated) {
        client.write_all(air::gw::build_error_response("FATAL", "28000", "SSL/TLS connection is required"));
        return;
    }

    if (msg_len < 8 || msg_len > air::gw::kMaxStartupMessageBytes) {
        client.write_all(air::gw::build_error_response("FATAL", "08P01", "StartupMessage length exceeds 8KB security limit"));
        return;
    }

    std::vector<uint8_t> body(msg_len - 8);
    if (!body.empty() && !client.read_exact(body.data(), body.size())) return;

    auto params = air::gw::parse_startup_params(body);
    std::string startup_tenant = air::gw::extract_tenant(sni_host, params);
    if (startup_tenant.empty() || !air::gw::validate_tenant_id(startup_tenant)) {
        client.write_all(air::gw::build_error_response("FATAL", "28000", "missing or invalid tenant identifier"));
        return;
    }

    std::string req_user;
    if (auto it = params.find("user"); it != params.end()) {
        req_user = air::gw::trim_ascii(it->second);
    }
    if (req_user.empty() || air::gw::iequals(req_user, "postgres")) {
        client.write_all(air::gw::build_error_response(
            "FATAL", "28000", "reserved superuser role not permitted through tenant gateway"));
        return;
    }

    if (!sni_host.empty()) {
        std::string sni_tenant = air::gw::parse_sni_tenant(sni_host);
        std::string db_tenant;
        if (auto it = params.find("database"); it != params.end()) {
            db_tenant = air::gw::trim_ascii(it->second);
        }
        if (!sni_tenant.empty() && !db_tenant.empty() && db_tenant != "postgres" && sni_tenant != db_tenant) {
            client.write_all(air::gw::build_error_response(
                "FATAL", "28000",
                "SNI tenant (" + sni_tenant + ") does not match StartupMessage database (" + db_tenant + ")"));
            return;
        }
    }

    std::string limit_err;
    if (!metrics.try_acquire_tenant_user(startup_tenant, req_user, limit_err)) {
        client.write_all(air::gw::build_error_response("FATAL", "53300", limit_err));
        return;
    }
    TenantUserConnGuard tu_guard{metrics, startup_tenant, req_user, true};

    air::gw::TenantRoute route;
    std::string resolve_err;
    if (!discovery.resolve_tenant(startup_tenant, route, resolve_err)) {
        client.write_all(air::gw::build_error_response(
            "FATAL", "3D000", "tenant database \"" + startup_tenant + "\" not found: " + resolve_err));
        return;
    }
    if (route.state != "ACTIVE" || route.port <= 0) {
        client.write_all(air::gw::build_error_response(
            "FATAL", "57P03", "tenant database \"" + startup_tenant + "\" is in state " + route.state));
        return;
    }

    discovery.touch_activity(startup_tenant);
    client.set_timeouts(0, 30000);

    if (cfg.pool_mode) {
        auto pool = pools.get_or_create(route, [&discovery, startup_tenant](air::gw::TenantRoute& wr, std::string& we) {
            return discovery.wake_tenant(startup_tenant, wr, we);
        });
        pool->serve_client(client, req_user, route.app_password, [&discovery, startup_tenant]() {
            discovery.touch_activity(startup_tenant);
        }, &metrics);
        return;
    }

    int bfd = air::gw::dial_backend_socket(route.host, route.sock_dir, route.port);
    if (bfd < 0) {
        client.write_all(air::gw::build_error_response("FATAL", "08006", "failed to dial cell backend"));
        return;
    }
    air::gw::Stream backend(bfd);
    params["database"] = startup_tenant;
    if (!backend.write_all(air::gw::build_startup_message(params))) return;
    proxy_bidirectional(client, backend);
}

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    curl_global_init(CURL_GLOBAL_ALL);

    air::gw::Config cfg = air::gw::Config::load(argc, argv);
    air::gw::TLSContext tls_ctx;
    std::string tls_err;
    if (!tls_ctx.init(cfg.tls_cert_path, cfg.tls_key_path, tls_err)) {
        std::cerr << "[air-gateway] TLS init failed: " << tls_err << std::endl;
        curl_global_cleanup();
        return 1;
    }

    air::gw::MetricsRegistry metrics(
        cfg.max_client_conns, cfg.max_tenant_client_conns, cfg.max_user_client_conns);
    air::gw::PoolManager pools(cfg.pool_max_backends);
    air::gw::TenantDiscovery discovery(cfg);
    discovery.set_callbacks(
        [&pools](const air::gw::TenantRoute& r) { pools.upsert_discovered(r); },
        [&pools](const std::string& tid) { pools.remove_tenant(tid); });
    discovery.start();

    air::gw::AdminHTTPServer admin_srv(
        cfg.admin_addr, cfg.internal_token, discovery, pools, metrics);
    if (!admin_srv.start()) {
        std::cerr << "[air-gateway] failed to bind admin HTTP on " << cfg.admin_addr << std::endl;
        discovery.stop();
        curl_global_cleanup();
        return 1;
    }

    g_listen_fd = air::gw::listen_tcp_addr(cfg.gateway_addr);
    if (g_listen_fd < 0) {
        std::cerr << "[air-gateway] failed to listen on " << cfg.gateway_addr << std::endl;
        admin_srv.stop();
        discovery.stop();
        curl_global_cleanup();
        return 1;
    }

    std::cout << "[air-gateway] C++ PG Gateway listening on " << cfg.gateway_addr
              << " (admin=" << cfg.admin_addr
              << ", control_plane=" << cfg.control_plane_url
              << ", pool_mode=" << (cfg.pool_mode ? "true" : "false")
              << ", max_client_conns=" << cfg.max_client_conns
              << ", max_tenant_client_conns=" << cfg.max_tenant_client_conns
              << ", max_user_client_conns=" << cfg.max_user_client_conns
              << ", require_tls=" << (cfg.require_tls ? "true" : "false") << ")" << std::endl;

    while (g_running.load()) {
        int cfd = ::accept(g_listen_fd, nullptr, nullptr);
        if (cfd < 0) {
            if (!g_running.load()) break;
            if (errno == EINTR) continue;
            break;
        }
        if (!metrics.try_acquire_global()) {
            air::gw::Stream tmp(cfd);
            tmp.set_timeouts(1000, 1000);
            tmp.write_all(air::gw::build_error_response(
                "FATAL", "53300", "sorry, too many clients already (gateway global connection limit reached)"));
            continue;
        }
        std::thread([cfd, &cfg, &tls_ctx, &discovery, &pools, &metrics]() {
            handle_client(cfd, cfg, tls_ctx, discovery, pools, metrics);
        }).detach();
    }

    std::cout << "[air-gateway] shutting down..." << std::endl;
    admin_srv.stop();
    discovery.stop();
    pools.close_all();
    curl_global_cleanup();
    return 0;
}
