#pragma once

#include <string>

namespace air::gw {

/**
 * Runtime configuration for air-gateway, loaded from environment variables
 * and overridden by CLI flags.
 */
struct Config {
    std::string gateway_addr = "127.0.0.1:5432";
    std::string admin_addr = "127.0.0.1:8081";
    std::string admin_grpc_addr;
    std::string meta_dsn;
    int meta_port = 15432;
    std::string control_plane_url = "http://127.0.0.1:8080";
    std::string grpc_addr = "127.0.0.1:9090";
    std::string internal_token;
    std::string backend_host;
    bool require_tls = false;
    std::string tls_cert_path;
    std::string tls_key_path;
    bool pool_mode = true;
    int pool_max_backends = 16;
    int pool_idle_timeout_sec = 300;
    int auth_timeout_ms = 5000;
    int client_idle_timeout_ms = 0;
    int max_client_conns = 1024;
    int max_tenant_client_conns = 128;
    int max_user_client_conns = 0;
    int discovery_interval_ms = 2000;

    static std::string env_or(const char* key, const std::string& fallback);
    static bool env_bool(const char* key, bool fallback);
    static int env_int(const char* key, int fallback);

    /// Populate Config from AIR_* environment variables and CLI arguments.
    static Config load(int argc, char** argv);
};

} // namespace air::gw
