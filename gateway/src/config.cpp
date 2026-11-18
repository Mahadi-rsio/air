#include "config.hpp"

#include <cstdlib>

namespace air::gw {

namespace {

int env_int_min(const char* key, int fallback, int min_val) {
    if (const char* v = std::getenv(key); v != nullptr && v[0] != '\0') {
        try {
            int parsed = std::stoi(v);
            if (parsed >= min_val) return parsed;
        } catch (...) {
        }
    }
    return fallback;
}

void parse_int_arg(int& i, int argc, char** argv, int& dst) {
    if (i + 1 >= argc) return;
    try {
        dst = std::stoi(argv[++i]);
    } catch (...) {
    }
}

} // namespace

std::string Config::env_or(const char* key, const std::string& fallback) {
    if (const char* v = std::getenv(key); v != nullptr && v[0] != '\0') {
        return std::string(v);
    }
    return fallback;
}

bool Config::env_bool(const char* key, bool fallback) {
    if (const char* v = std::getenv(key); v != nullptr && v[0] != '\0') {
        std::string s(v);
        return s == "1" || s == "true" || s == "TRUE" || s == "True";
    }
    return fallback;
}

int Config::env_int(const char* key, int fallback) {
    return env_int_min(key, fallback, 1);
}

Config Config::load(int argc, char** argv) {
    Config c;
    c.gateway_addr = env_or("AIR_GATEWAY_ADDR", c.gateway_addr);
    c.admin_addr = env_or("AIR_GATEWAY_ADMIN_ADDR", c.admin_addr);
    c.admin_grpc_addr = env_or("AIR_GATEWAY_ADMIN_GRPC_ADDR", c.admin_grpc_addr);
    c.meta_dsn = env_or("AIR_META_DSN", "");
    c.meta_port = env_int("AIR_META_PORT", c.meta_port);
    c.control_plane_url = env_or("AIR_CONTROL_PLANE_URL", c.control_plane_url);
    c.grpc_addr = env_or("AIR_DB_GRPC_ADDR", c.grpc_addr);
    c.internal_token = env_or("AIR_INTERNAL_TOKEN", "");
    c.backend_host = env_or("AIR_BACKEND_HOST", "");
    c.require_tls = env_bool("AIR_REQUIRE_TLS", c.require_tls);
    c.tls_cert_path = env_or("AIR_TLS_CERT", "");
    c.tls_key_path = env_or("AIR_TLS_KEY", "");
    c.pool_mode = env_bool("AIR_POOL_MODE", c.pool_mode);
    c.pool_max_backends = env_int("AIR_POOL_MAX_BACKENDS", c.pool_max_backends);
    c.pool_idle_timeout_sec = env_int("AIR_POOL_IDLE_TIMEOUT_SEC", c.pool_idle_timeout_sec);
    c.auth_timeout_ms = env_int("AIR_AUTH_TIMEOUT_MS", c.auth_timeout_ms);
    c.client_idle_timeout_ms = env_int_min("AIR_CLIENT_IDLE_TIMEOUT_MS", c.client_idle_timeout_ms, 0);
    c.max_client_conns = env_int("AIR_MAX_CLIENT_CONNS", c.max_client_conns);
    c.max_tenant_client_conns = env_int("AIR_MAX_TENANT_CLIENT_CONNS", c.max_tenant_client_conns);
    c.max_user_client_conns = env_int("AIR_MAX_USER_CLIENT_CONNS", c.max_user_client_conns);
    c.discovery_interval_ms = env_int("AIR_DISCOVERY_INTERVAL_MS", c.discovery_interval_ms);

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next_val = [&](std::string& dst) {
            if (i + 1 < argc) dst = argv[++i];
        };
        if (arg == "--gateway-addr") next_val(c.gateway_addr);
        else if (arg == "--gateway-admin-addr") next_val(c.admin_addr);
        else if (arg == "--gateway-admin-grpc-addr") next_val(c.admin_grpc_addr);
        else if (arg == "--meta-dsn") next_val(c.meta_dsn);
        else if (arg == "--control-plane-url") next_val(c.control_plane_url);
        else if (arg == "--db-grpc-addr") next_val(c.grpc_addr);
        else if (arg == "--internal-token") next_val(c.internal_token);
        else if (arg == "--backend-host") next_val(c.backend_host);
        else if (arg == "--tls-cert") next_val(c.tls_cert_path);
        else if (arg == "--tls-key") next_val(c.tls_key_path);
        else if (arg == "--require-tls") c.require_tls = true;
        else if (arg == "--pool-mode") c.pool_mode = true;
        else if (arg == "--no-pool-mode") c.pool_mode = false;
        else if (arg == "--pool-max-backends") parse_int_arg(i, argc, argv, c.pool_max_backends);
        else if (arg == "--pool-idle-timeout-sec") parse_int_arg(i, argc, argv, c.pool_idle_timeout_sec);
        else if (arg == "--auth-timeout-ms") parse_int_arg(i, argc, argv, c.auth_timeout_ms);
        else if (arg == "--client-idle-timeout-ms") parse_int_arg(i, argc, argv, c.client_idle_timeout_ms);
        else if (arg == "--max-client-conns") parse_int_arg(i, argc, argv, c.max_client_conns);
        else if (arg == "--max-tenant-client-conns") parse_int_arg(i, argc, argv, c.max_tenant_client_conns);
        else if (arg == "--max-user-client-conns") parse_int_arg(i, argc, argv, c.max_user_client_conns);
    }

    if (c.meta_dsn.empty()) {
        c.meta_dsn = "host=127.0.0.1 port=" + std::to_string(c.meta_port) +
                     " user=postgres dbname=postgres connect_timeout=3";
    }
    while (!c.control_plane_url.empty() && c.control_plane_url.back() == '/') {
        c.control_plane_url.pop_back();
    }
    return c;
}

} // namespace air::gw
