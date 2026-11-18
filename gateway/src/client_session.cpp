#include "client_session.hpp"
#include "pg/protocol.hpp"

#include <cerrno>
#include <iostream>
#include <poll.h>
#include <sys/socket.h>
#include <thread>
#include <vector>

namespace air::gw {

namespace {

struct GlobalConnGuard {
    MetricsRegistry& metrics;
    ~GlobalConnGuard() { metrics.release_global(); }
};

struct TenantUserConnGuard {
    MetricsRegistry& metrics;
    std::string tenant_id;
    std::string user;
    bool active = false;
    ~TenantUserConnGuard() {
        if (active) metrics.release_tenant_user(tenant_id, user);
    }
};

void proxy_bidirectional(Stream& client, Stream& backend) {
    auto pump_one = [](Stream& src, Stream& dst) {
        uint8_t buf[32768];
        while (true) {
            ssize_t n = src.read_some(buf, sizeof(buf));
            if (n <= 0) return;
            if (!dst.write_all(buf, static_cast<size_t>(n))) return;
        }
    };
    if (client.is_tls()) {
        std::thread t1([&]() {
            pump_one(client, backend);
            backend.close();
            client.close();
        });
        pump_one(backend, client);
        backend.close();
        client.close();
        t1.join();
        return;
    }

    uint8_t buf[32768];
    struct pollfd pfds[2];
    pfds[0] = {client.fd(), POLLIN, 0};
    pfds[1] = {backend.fd(), POLLIN, 0};
    while (true) {
        int rc = ::poll(pfds, 2, 30000);
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (rc == 0) continue;
        if ((pfds[0].revents & POLLNVAL) || (pfds[1].revents & POLLNVAL)) break;
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

void forward_cancel_request(Stream& client, CancelRegistry& cancel_reg) {
    uint8_t cancel_payload[8];
    if (!client.read_exact(cancel_payload, 8)) return;
    uint32_t gw_pid = read_be32(cancel_payload);
    uint32_t gw_secret = read_be32(cancel_payload + 4);
    CancelTarget target;
    if (cancel_reg.lookup(gw_pid, gw_secret, target) && target.port > 0) {
        int bfd = dial_backend_socket(target.host, target.sock_dir, target.port);
        if (bfd >= 0) {
            Stream bstream(bfd);
            bstream.write_all(build_cancel_request(target.backend_pid, target.backend_secret));
        }
    }
}

} // namespace

void handle_client_connection(int cfd,
                              const Config& cfg,
                              TLSContext& tls_ctx,
                              TenantDiscovery& discovery,
                              PoolManager& pools,
                              MetricsRegistry& metrics,
                              CancelRegistry& cancel_reg) {
    GlobalConnGuard global_guard{metrics};
    Stream client(cfd);
    client.set_timeouts(cfg.auth_timeout_ms, 5000);

    uint8_t hdr[8];
    if (!client.read_exact(hdr, 8)) {
        metrics.record_timeout();
        return;
    }
    uint32_t msg_len = read_be32(hdr);
    uint32_t code = read_be32(hdr + 4);

    std::string sni_host;
    bool tls_negotiated = false;

    // Negotiate optional GSSENCRequest (declined) and SSLRequest (TLS upgrade).
    for (int pre = 0; pre < 2; ++pre) {
        if (msg_len == 8 && code == kGSSENCRequestCode) {
            uint8_t no = 'N';
            if (!client.write_all(&no, 1)) return;
        } else if (msg_len == 8 && code == kSSLRequestCode && !tls_negotiated) {
            uint8_t yes = 'S';
            if (!client.write_all(&yes, 1)) return;
            SSL* ssl = tls_ctx.accept_tls(client.fd(), sni_host);
            if (!ssl) {
                metrics.record_protocol_error();
                return;
            }
            client.set_ssl(ssl);
            tls_negotiated = true;
        } else {
            break;
        }
        if (!client.read_exact(hdr, 8)) return;
        msg_len = read_be32(hdr);
        code = read_be32(hdr + 4);
    }

    if (msg_len == 16 && code == kCancelRequestCode) {
        forward_cancel_request(client, cancel_reg);
        return;
    }

    if (cfg.require_tls && !tls_negotiated) {
        metrics.record_auth_failure();
        client.write_all(build_error_response("FATAL", "28000", "SSL/TLS connection is required"));
        return;
    }

    if (msg_len < 8 || msg_len > kMaxStartupMessageBytes) {
        metrics.record_protocol_error();
        client.write_all(build_error_response("FATAL", "08P01", "StartupMessage length exceeds 8KB security limit"));
        return;
    }

    if (code != kPGProtocolVersion3) {
        metrics.record_protocol_error();
        client.write_all(build_error_response("FATAL", "0A000", "unsupported frontend protocol version"));
        return;
    }

    std::vector<uint8_t> body(msg_len - 8);
    if (!body.empty() && !client.read_exact(body.data(), body.size())) return;

    auto params = parse_startup_params(body);
    std::string startup_tenant = extract_tenant(sni_host, params);
    if (startup_tenant.empty() || !validate_tenant_id(startup_tenant)) {
        metrics.record_auth_failure();
        client.write_all(build_error_response("FATAL", "28000", "missing or invalid tenant identifier"));
        return;
    }

    std::string req_user;
    if (auto it = params.find("user"); it != params.end()) {
        req_user = trim_ascii(it->second);
    }
    if (req_user.empty() || iequals(req_user, "postgres") ||
        (cfg.pool_mode && req_user != "app_user")) {
        metrics.record_auth_failure();
        client.write_all(build_error_response(
            "FATAL", "28000", "user \"" + req_user + "\" is not permitted through tenant gateway"));
        return;
    }

    if (!sni_host.empty()) {
        std::string sni_tenant = parse_sni_tenant(sni_host);
        std::string db_tenant;
        if (auto it = params.find("database"); it != params.end()) {
            db_tenant = trim_ascii(it->second);
        }
        if (!sni_tenant.empty() && !db_tenant.empty() && db_tenant != "postgres" && sni_tenant != db_tenant) {
            metrics.record_auth_failure();
            client.write_all(build_error_response(
                "FATAL", "28000",
                "SNI tenant (" + sni_tenant + ") does not match StartupMessage database (" + db_tenant + ")"));
            return;
        }
    }

    std::string limit_err;
    if (!metrics.try_acquire_tenant_user(startup_tenant, req_user, limit_err)) {
        client.write_all(build_error_response("FATAL", "53300", limit_err));
        return;
    }
    TenantUserConnGuard tu_guard{metrics, startup_tenant, req_user, true};

    TenantRoute route;
    std::string resolve_err;
    grpc::StatusCode rpc_code = grpc::StatusCode::OK;
    if (!discovery.resolve_tenant(startup_tenant, route, resolve_err, &rpc_code)) {
        const char* sqlstate = "08006";
        if (rpc_code == grpc::StatusCode::NOT_FOUND) sqlstate = "3D000";
        else if (rpc_code == grpc::StatusCode::UNAVAILABLE || rpc_code == grpc::StatusCode::DEADLINE_EXCEEDED) sqlstate = "57P03";
        std::cerr << "[air-gateway] routing failed tenant=" << startup_tenant
                  << " grpc_code=" << static_cast<int>(rpc_code)
                  << " err=" << resolve_err << std::endl;
        client.write_all(build_error_response(
            "FATAL", sqlstate, "tenant database \"" + startup_tenant + "\" unavailable: " + resolve_err));
        return;
    }
    if (route.state != "ACTIVE" || route.port <= 0) {
        client.write_all(build_error_response(
            "FATAL", "57P03", "tenant database \"" + startup_tenant + "\" is in state " + route.state));
        return;
    }

    discovery.touch_activity(startup_tenant);

    if (cfg.pool_mode) {
        auto pool = pools.get_or_create(route, [&discovery, startup_tenant](TenantRoute& wr, std::string& we) {
            return discovery.wake_tenant(startup_tenant, wr, we);
        });
        pool->serve_client(client, req_user, route.app_password, [&discovery, startup_tenant]() {
            discovery.touch_activity(startup_tenant);
        }, &metrics, &cancel_reg, cfg.client_idle_timeout_ms);
        return;
    }

    client.set_timeouts(cfg.client_idle_timeout_ms, 30000);
    int bfd = dial_backend_socket(route.host, route.sock_dir, route.port);
    if (bfd < 0) {
        discovery.evict_cache(startup_tenant);
        client.write_all(build_error_response("FATAL", "08006", "failed to dial cell backend"));
        return;
    }
    Stream backend(bfd);
    params["database"] = startup_tenant;
    if (!backend.write_all(build_startup_message(params))) return;
    proxy_bidirectional(client, backend);
}

} // namespace air::gw
