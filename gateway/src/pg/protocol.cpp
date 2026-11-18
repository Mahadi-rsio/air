#include "pg/protocol.hpp"

namespace air::gw {

std::string read_cstr(const std::vector<uint8_t>& buf, size_t& off) {
    size_t start = off;
    while (off < buf.size() && buf[off] != 0) ++off;
    std::string s(reinterpret_cast<const char*>(buf.data() + start), off - start);
    if (off < buf.size() && buf[off] == 0) ++off;
    return s;
}

std::vector<uint8_t> build_pg_message(char type, const std::vector<uint8_t>& body) {
    std::vector<uint8_t> out;
    out.reserve(5 + body.size());
    out.push_back(static_cast<uint8_t>(type));
    append_be32(out, static_cast<uint32_t>(4 + body.size()));
    out.insert(out.end(), body.begin(), body.end());
    return out;
}

std::vector<uint8_t> build_error_response(const std::string& severity,
                                          const std::string& sqlstate,
                                          const std::string& msg) {
    std::vector<uint8_t> body;
    body.push_back('S');
    append_cstr(body, severity);
    body.push_back('C');
    append_cstr(body, sqlstate);
    body.push_back('M');
    append_cstr(body, msg);
    body.push_back(0);
    return build_pg_message('E', body);
}

std::vector<uint8_t> build_auth_ok() {
    std::vector<uint8_t> body;
    append_be32(body, 0);
    return build_pg_message('R', body);
}

std::vector<uint8_t> build_auth_cleartext() {
    std::vector<uint8_t> body;
    append_be32(body, 3);
    return build_pg_message('R', body);
}

std::vector<uint8_t> build_param_status(const std::string& k, const std::string& v) {
    std::vector<uint8_t> body;
    append_cstr(body, k);
    append_cstr(body, v);
    return build_pg_message('S', body);
}

std::vector<uint8_t> build_backend_key_data(uint32_t pid, uint32_t secret) {
    std::vector<uint8_t> body;
    append_be32(body, pid);
    append_be32(body, secret);
    return build_pg_message('K', body);
}

std::vector<uint8_t> build_ready_for_query(char status) {
    std::vector<uint8_t> body{static_cast<uint8_t>(status)};
    return build_pg_message('Z', body);
}

std::vector<uint8_t> build_password_message(const std::string& pw) {
    std::vector<uint8_t> body;
    append_cstr(body, pw);
    return build_pg_message('p', body);
}

std::vector<uint8_t> build_simple_query(const std::string& sql) {
    std::vector<uint8_t> body;
    append_cstr(body, sql);
    return build_pg_message('Q', body);
}

std::vector<uint8_t> build_sync_message() {
    return build_pg_message('S', {});
}

std::vector<uint8_t> build_parse_message(const std::string& name,
                                         const std::string& query,
                                         const std::vector<uint32_t>& oids) {
    std::vector<uint8_t> body;
    append_cstr(body, name);
    append_cstr(body, query);
    append_be16(body, static_cast<uint16_t>(oids.size()));
    for (uint32_t oid : oids) {
        append_be32(body, oid);
    }
    return build_pg_message('P', body);
}

std::vector<uint8_t> build_cancel_request(uint32_t pid, uint32_t secret) {
    std::vector<uint8_t> out;
    out.reserve(16);
    append_be32(out, 16);
    append_be32(out, kCancelRequestCode);
    append_be32(out, pid);
    append_be32(out, secret);
    return out;
}

std::vector<uint8_t> build_startup_message(
    const std::unordered_map<std::string, std::string>& params) {
    std::vector<uint8_t> kv;
    for (const auto& [k, v] : params) {
        append_cstr(kv, k);
        append_cstr(kv, v);
    }
    kv.push_back(0);
    std::vector<uint8_t> out;
    append_be32(out, static_cast<uint32_t>(8 + kv.size()));
    append_be32(out, kPGProtocolVersion3);
    out.insert(out.end(), kv.begin(), kv.end());
    return out;
}

bool read_pg_packet(Stream& s, char& type_out, std::vector<uint8_t>& body_out) {
    uint8_t hdr[5];
    if (!s.read_exact(hdr, 5)) return false;
    type_out = static_cast<char>(hdr[0]);
    uint32_t len = read_be32(hdr + 1);
    if (len < 4 || len > kMaxPGPacketBytes) return false;
    size_t body_len = len - 4;
    body_out.resize(body_len);
    if (body_len > 0 && !s.read_exact(body_out.data(), body_len)) {
        return false;
    }
    return true;
}

std::unordered_map<std::string, std::string> parse_startup_params(
    const std::vector<uint8_t>& body) {
    std::unordered_map<std::string, std::string> params;
    size_t i = 0;
    while (i < body.size()) {
        if (body[i] == 0) break;
        size_t k_end = i;
        while (k_end < body.size() && body[k_end] != 0) ++k_end;
        if (k_end >= body.size()) break;
        std::string key(reinterpret_cast<const char*>(body.data() + i), k_end - i);
        size_t v_start = k_end + 1;
        size_t v_end = v_start;
        while (v_end < body.size() && body[v_end] != 0) ++v_end;
        if (v_end > body.size()) break;
        std::string val(reinterpret_cast<const char*>(body.data() + v_start), v_end - v_start);
        params[key] = val;
        i = v_end + 1;
    }
    return params;
}

std::vector<uint8_t> rewrite_parse_body(const std::vector<uint8_t>& body,
                                        const std::string& global_name,
                                        std::string& query_out,
                                        std::vector<uint32_t>& oids_out) {
    size_t off = 0;
    (void)read_cstr(body, off); // discard client-local statement name
    query_out = read_cstr(body, off);
    oids_out.clear();
    if (off + 2 <= body.size()) {
        uint16_t n = read_be16(body.data() + off);
        off += 2;
        for (uint16_t i = 0; i < n && off + 4 <= body.size(); ++i) {
            oids_out.push_back(read_be32(body.data() + off));
            off += 4;
        }
    }
    std::vector<uint8_t> out;
    append_cstr(out, global_name);
    append_cstr(out, query_out);
    append_be16(out, static_cast<uint16_t>(oids_out.size()));
    for (uint32_t oid : oids_out) append_be32(out, oid);
    return out;
}

std::vector<uint8_t> rewrite_bind_body(const std::vector<uint8_t>& body,
                                       const std::string& global_stmt_name) {
    size_t off = 0;
    std::string portal = read_cstr(body, off);
    (void)read_cstr(body, off); // discard client-local statement name
    size_t tail_off = off;

    std::vector<uint8_t> out;
    append_cstr(out, portal);
    append_cstr(out, global_stmt_name);
    if (tail_off < body.size()) {
        out.insert(out.end(), body.begin() + static_cast<ptrdiff_t>(tail_off), body.end());
    }
    return out;
}

std::vector<uint8_t> rewrite_named_op_body(const std::vector<uint8_t>& body,
                                           const std::string& global_name) {
    if (body.empty()) return body;
    char kind = static_cast<char>(body[0]);
    if (kind != 'S') return body; // Portal ('P') targets are connection-local, not rewritten.
    size_t off = 1;
    (void)read_cstr(body, off);
    size_t tail_off = off;

    std::vector<uint8_t> out;
    out.push_back('S');
    append_cstr(out, global_name);
    if (tail_off < body.size()) {
        out.insert(out.end(), body.begin() + static_cast<ptrdiff_t>(tail_off), body.end());
    }
    return out;
}

} // namespace air::gw
