#pragma once

#include <cstdlib>
#include <string>

namespace air::gw {

struct Config {
    std::string gateway_addr = "127.0.0.1:5432";
    std::string admin_addr = "127.0.0.1:8081";
    std::string meta_dsn;
    int meta_port = 15432;
    std::string control_plane_url = "http://127.0.0.1:8080";
    std::string internal_token;
    std::string backend_host;
    bool require_tls = false;
    std::string tls_cert_path;
    std::string tls_key_path;
    bool pool_mode = true;
    int pool_max_backends = 16;
    int discovery_interval_ms = 2000;

    static std::string env_or(const char* key, const std::string& fallback) {
        if (const char* v = std::getenv(key); v != nullptr && v[0] != '\0') {
            return std::string(v);
        }
        return fallback;
    }

    static bool env_bool(const char* key, bool fallback) {
        if (const char* v = std::getenv(key); v != nullptr && v[0] != '\0') {
            std::string s(v);
            return s == "1" || s == "true" || s == "TRUE" || s == "True";
        }
        return fallback;
    }

    static int env_int(const char* key, int fallback) {
        if (const char* v = std::getenv(key); v != nullptr && v[0] != '\0') {
            try {
                int parsed = std::stoi(v);
                if (parsed > 0) return parsed;
            } catch (...) {
            }
        }
        return fallback;
    }

    static Config load(int argc, char** argv) {
        Config c;
        c.gateway_addr = env_or("AIR_GATEWAY_ADDR", c.gateway_addr);
        c.admin_addr = env_or("AIR_GATEWAY_ADMIN_ADDR", c.admin_addr);
        c.meta_dsn = env_or("AIR_META_DSN", "");
        c.meta_port = env_int("AIR_META_PORT", c.meta_port);
        c.control_plane_url = env_or("AIR_CONTROL_PLANE_URL", c.control_plane_url);
        c.internal_token = env_or("AIR_INTERNAL_TOKEN", "");
        c.backend_host = env_or("AIR_BACKEND_HOST", "");
        c.require_tls = env_bool("AIR_REQUIRE_TLS", c.require_tls);
        c.tls_cert_path = env_or("AIR_TLS_CERT", "");
        c.tls_key_path = env_or("AIR_TLS_KEY", "");
        c.pool_mode = env_bool("AIR_POOL_MODE", c.pool_mode);
        c.pool_max_backends = env_int("AIR_POOL_MAX_BACKENDS", c.pool_max_backends);
        c.discovery_interval_ms = env_int("AIR_DISCOVERY_INTERVAL_MS", c.discovery_interval_ms);

        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            auto next_val = [&](std::string& dst) {
                if (i + 1 < argc) dst = argv[++i];
            };
            if (arg == "--gateway-addr") next_val(c.gateway_addr);
            else if (arg == "--gateway-admin-addr") next_val(c.admin_addr);
            else if (arg == "--meta-dsn") next_val(c.meta_dsn);
            else if (arg == "--control-plane-url") next_val(c.control_plane_url);
            else if (arg == "--internal-token") next_val(c.internal_token);
            else if (arg == "--backend-host") next_val(c.backend_host);
            else if (arg == "--tls-cert") next_val(c.tls_cert_path);
            else if (arg == "--tls-key") next_val(c.tls_key_path);
            else if (arg == "--require-tls") c.require_tls = true;
            else if (arg == "--pool-mode") c.pool_mode = true;
            else if (arg == "--no-pool-mode") c.pool_mode = false;
            else if (arg == "--pool-max-backends" && i + 1 < argc) {
                try { c.pool_max_backends = std::stoi(argv[++i]); } catch (...) {}
            }
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
};

} // namespace air::gw
