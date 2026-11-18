#include "admin_grpc.hpp"
#include "admin_http.hpp"
#include "client_session.hpp"
#include "config.hpp"
#include "discovery.hpp"
#include "metrics.hpp"
#include "net/socket.hpp"
#include "net/tls.hpp"
#include "pg/auth.hpp"
#include "pg/protocol.hpp"
#include "pooler.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <iostream>
#include <mutex>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

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

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    air::gw::Config cfg = air::gw::Config::load(argc, argv);
    air::gw::TLSContext tls_ctx;
    std::string tls_err;
    if (!tls_ctx.init(cfg.tls_cert_path, cfg.tls_key_path, tls_err)) {
        std::cerr << "[air-gateway] TLS init failed: " << tls_err << std::endl;
        return 1;
    }

    air::gw::MetricsRegistry metrics(
        cfg.max_client_conns, cfg.max_tenant_client_conns, cfg.max_user_client_conns);
    air::gw::PoolManager pools(cfg.pool_max_backends, cfg.pool_idle_timeout_sec);
    air::gw::CancelRegistry cancel_reg;
    air::gw::TenantDiscovery discovery(cfg);
    discovery.set_metrics(&metrics);
    discovery.set_callbacks(
        [&pools](const air::gw::TenantRoute& r) { pools.upsert_discovered(r); },
        [&pools](const std::string& tid) { pools.remove_tenant(tid); });
    discovery.start();

    air::gw::AdminHTTPServer admin_srv(
        cfg.admin_addr, cfg.internal_token, discovery, pools, metrics);
    if (!admin_srv.start()) {
        std::cerr << "[air-gateway] failed to bind admin HTTP on " << cfg.admin_addr << std::endl;
        discovery.stop();
        return 1;
    }

    air::gw::AdminGRPCServer admin_grpc_srv(
        cfg.admin_grpc_addr, cfg.internal_token, pools, metrics);
    if (!cfg.admin_grpc_addr.empty() && !admin_grpc_srv.start()) {
        std::cerr << "[air-gateway] failed to bind admin gRPC on " << cfg.admin_grpc_addr << std::endl;
        admin_srv.stop();
        discovery.stop();
        return 1;
    }

    g_listen_fd = air::gw::listen_tcp_addr(cfg.gateway_addr);
    if (g_listen_fd < 0) {
        std::cerr << "[air-gateway] failed to listen on " << cfg.gateway_addr << std::endl;
        admin_grpc_srv.stop();
        admin_srv.stop();
        discovery.stop();
        return 1;
    }

    std::cout << "[air-gateway] C++ PG Gateway listening on " << cfg.gateway_addr
              << " (admin=" << cfg.admin_addr
              << ", admin_grpc=" << (cfg.admin_grpc_addr.empty() ? "disabled" : cfg.admin_grpc_addr)
              << ", db_grpc=" << cfg.grpc_addr
              << ", pool_mode=" << (cfg.pool_mode ? "true" : "false")
              << ", max_client_conns=" << cfg.max_client_conns
              << ", max_tenant_client_conns=" << cfg.max_tenant_client_conns
              << ", max_user_client_conns=" << cfg.max_user_client_conns
              << ", require_tls=" << (cfg.require_tls ? "true" : "false") << ")" << std::endl;

    std::atomic<int> active_clients{0};
    std::mutex clients_mu;
    std::condition_variable clients_cv;

    std::atomic<bool> reaper_running{true};
    std::mutex reaper_mu;
    std::condition_variable reaper_cv;
    std::thread reaper_thread([&]() {
        while (reaper_running.load()) {
            std::unique_lock<std::mutex> lk(reaper_mu);
            reaper_cv.wait_for(lk, std::chrono::seconds(30), [&]() { return !reaper_running.load(); });
            if (!reaper_running.load()) break;
            pools.evict_expired_idle();
        }
    });

    while (g_running.load()) {
        int cfd = ::accept(g_listen_fd, nullptr, nullptr);
        if (cfd < 0) {
            if (!g_running.load()) break;
            if (errno == EINTR) continue;
            break;
        }
        air::gw::set_tcp_nodelay(cfd);
        if (!metrics.try_acquire_global()) {
            air::gw::Stream tmp(cfd);
            tmp.set_timeouts(1000, 1000);
            tmp.write_all(air::gw::build_error_response(
                "FATAL", "53300", "sorry, too many clients already (gateway global connection limit reached)"));
            continue;
        }
        active_clients.fetch_add(1, std::memory_order_relaxed);
        std::thread([cfd, &cfg, &tls_ctx, &discovery, &pools, &metrics, &cancel_reg,
                     &active_clients, &clients_mu, &clients_cv]() {
            air::gw::handle_client_connection(cfd, cfg, tls_ctx, discovery, pools, metrics, cancel_reg);
            if (active_clients.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                std::lock_guard<std::mutex> lk(clients_mu);
                clients_cv.notify_all();
            }
        }).detach();
    }

    std::cout << "[air-gateway] shutting down..." << std::endl;
    reaper_running.store(false);
    reaper_cv.notify_all();
    if (reaper_thread.joinable()) reaper_thread.join();
    admin_grpc_srv.stop();
    admin_srv.stop();
    discovery.stop();
    pools.close_all();
    {
        std::unique_lock<std::mutex> lk(clients_mu);
        clients_cv.wait_for(lk, std::chrono::seconds(5), [&active_clients]() {
            return active_clients.load(std::memory_order_acquire) == 0;
        });
    }
    return 0;
}
