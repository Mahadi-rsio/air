#pragma once

#include "net/socket.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace air::gw {

// --- String, tenant ID, and security comparison utilities ---

std::string trim_ascii(const std::string& s);
bool iequals(const std::string& a, const std::string& b);

/// Constant-time string comparison via CRYPTO_memcmp (prevents timing side-channels on passwords).
bool constant_time_equals(const std::string& a, const std::string& b);

/// Validate tenant identifier: 1..63 chars of [A-Za-z0-9_-].
bool validate_tenant_id(const std::string& id);

/// Extract leading subdomain label from SNI host, ignoring IPv4 literals and single-label hosts.
std::string parse_sni_tenant(const std::string& sni_host);

/// Resolve tenant from explicit `database` startup parameter (if non-"postgres") or SNI subdomain.
std::string extract_tenant(const std::string& sni_host,
                           const std::unordered_map<std::string, std::string>& params);

// --- Cryptography & SCRAM-SHA-256 ---

std::string base64_encode(const uint8_t* data, size_t len);
std::vector<uint8_t> base64_decode(const std::string& b64);
std::vector<uint8_t> hmac_sha256(const std::vector<uint8_t>& key, const std::string& data);
std::vector<uint8_t> sha256_bytes(const std::vector<uint8_t>& data);

/// Perform client-side SCRAM-SHA-256 SASL exchange against a PostgreSQL backend stream.
bool scram_sha256_client_auth(Stream& s,
                              const std::string& user,
                              const std::string& password,
                              std::string& err_out);

// --- Query cancellation routing ---

struct CancelTarget {
    std::string host;
    std::string sock_dir;
    int port = 0;
    uint32_t backend_pid = 0;
    uint32_t backend_secret = 0;
};

/**
 * Thread-safe registry mapping gateway-issued (pid, secret) pairs from BackendKeyData
 * to the currently active backend PostgreSQL (pid, secret, endpoint) for CancelRequest forwarding.
 */
class CancelRegistry {
public:
    void register_target(uint32_t gw_pid, uint32_t gw_secret, CancelTarget target);
    void unregister_target(uint32_t gw_pid, uint32_t gw_secret);
    bool lookup(uint32_t gw_pid, uint32_t gw_secret, CancelTarget& out) const;

private:
    static uint64_t make_key(uint32_t pid, uint32_t secret) {
        return (static_cast<uint64_t>(pid) << 32) | static_cast<uint64_t>(secret);
    }

    mutable std::mutex mu_;
    std::unordered_map<uint64_t, CancelTarget> targets_;
};

} // namespace air::gw
