#pragma once

#include "net/socket.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace air::gw {

// PostgreSQL v3 wire protocol constants and safety limits.
constexpr uint32_t kMaxStartupMessageBytes = 8192;
constexpr uint32_t kMaxPGPacketBytes = 32 * 1024 * 1024;
constexpr uint32_t kCancelRequestCode = 80877102;
constexpr uint32_t kSSLRequestCode = 80877103;
constexpr uint32_t kGSSENCRequestCode = 80877104;
constexpr uint32_t kPGProtocolVersion3 = 196608; // 3.0

// --- Big-endian binary encoding/decoding helpers ---

inline uint32_t read_be32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) |
           (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) |
           static_cast<uint32_t>(p[3]);
}

inline uint16_t read_be16(const uint8_t* p) {
    return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) |
                                 static_cast<uint16_t>(p[1]));
}

inline void append_be32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

inline void append_be16(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

inline void append_cstr(std::vector<uint8_t>& buf, const std::string& s) {
    buf.insert(buf.end(), s.begin(), s.end());
    buf.push_back(0);
}

/// Read a NUL-terminated string from `buf` starting at `off`, advancing `off` past the NUL.
std::string read_cstr(const std::vector<uint8_t>& buf, size_t& off);

// --- PostgreSQL wire message builders ---

std::vector<uint8_t> build_pg_message(char type, const std::vector<uint8_t>& body);
std::vector<uint8_t> build_error_response(const std::string& severity,
                                          const std::string& sqlstate,
                                          const std::string& msg);
std::vector<uint8_t> build_auth_ok();
std::vector<uint8_t> build_auth_cleartext();
std::vector<uint8_t> build_param_status(const std::string& k, const std::string& v);
std::vector<uint8_t> build_backend_key_data(uint32_t pid, uint32_t secret);
std::vector<uint8_t> build_ready_for_query(char status);
std::vector<uint8_t> build_password_message(const std::string& pw);
std::vector<uint8_t> build_simple_query(const std::string& sql);
std::vector<uint8_t> build_sync_message();
std::vector<uint8_t> build_parse_message(const std::string& name,
                                         const std::string& query,
                                         const std::vector<uint32_t>& oids);
std::vector<uint8_t> build_cancel_request(uint32_t pid, uint32_t secret);
std::vector<uint8_t> build_startup_message(
    const std::unordered_map<std::string, std::string>& params);

// --- Packet reading & startup/extended-query parsing/rewriting ---

/// Read a typed PostgreSQL packet (1-byte type + 4-byte length + body), enforcing kMaxPGPacketBytes.
bool read_pg_packet(Stream& s, char& type_out, std::vector<uint8_t>& body_out);

/// Parse key/value pairs from a StartupMessage body (after version word).
std::unordered_map<std::string, std::string> parse_startup_params(
    const std::vector<uint8_t>& body);

/// Rewrite a Parse ('P') body to use `global_name`, extracting SQL and parameter OIDs.
std::vector<uint8_t> rewrite_parse_body(const std::vector<uint8_t>& body,
                                        const std::string& global_name,
                                        std::string& query_out,
                                        std::vector<uint32_t>& oids_out);

/// Rewrite a Bind ('B') body to reference `global_stmt_name`.
std::vector<uint8_t> rewrite_bind_body(const std::vector<uint8_t>& body,
                                       const std::string& global_stmt_name);

/// Rewrite a Describe ('D') or Close ('C') body when targeting a prepared statement ('S').
std::vector<uint8_t> rewrite_named_op_body(const std::vector<uint8_t>& body,
                                           const std::string& global_name);

} // namespace air::gw
