#pragma once

#include <arpa/inet.h>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <netdb.h>
#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace air::gw {

constexpr uint32_t kMaxStartupMessageBytes = 8192;
constexpr uint32_t kSSLRequestCode = 80877103;
constexpr uint32_t kPGProtocolVersion3 = 196608; // 3.0

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

class Stream {
public:
    explicit Stream(int fd = -1) : fd_(fd) {}
    ~Stream() { close(); }

    Stream(const Stream&) = delete;
    Stream& operator=(const Stream&) = delete;

    Stream(Stream&& other) noexcept : fd_(other.fd_), ssl_(other.ssl_) {
        other.fd_ = -1;
        other.ssl_ = nullptr;
    }

    Stream& operator=(Stream&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = other.fd_;
            ssl_ = other.ssl_;
            other.fd_ = -1;
            other.ssl_ = nullptr;
        }
        return *this;
    }

    int fd() const { return fd_; }
    bool valid() const { return fd_ >= 0; }
    bool is_tls() const { return ssl_ != nullptr; }

    void set_ssl(SSL* ssl) { ssl_ = ssl; }

    void set_timeouts(int recv_ms, int send_ms) {
        if (fd_ < 0) return;
        struct timeval tv{};
        if (recv_ms >= 0) {
            tv.tv_sec = recv_ms / 1000;
            tv.tv_usec = (recv_ms % 1000) * 1000;
            setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        }
        if (send_ms >= 0) {
            tv.tv_sec = send_ms / 1000;
            tv.tv_usec = (send_ms % 1000) * 1000;
            setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        }
    }

    bool read_exact(uint8_t* dst, size_t len) {
        size_t off = 0;
        while (off < len) {
            ssize_t n = 0;
            if (ssl_) {
                n = SSL_read(ssl_, dst + off, static_cast<int>(len - off));
                if (n <= 0) return false;
            } else {
                n = ::recv(fd_, dst + off, len - off, 0);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) return false;
            }
            off += static_cast<size_t>(n);
        }
        return true;
    }

    bool write_all(const uint8_t* src, size_t len) {
        size_t off = 0;
        while (off < len) {
            ssize_t n = 0;
            if (ssl_) {
                n = SSL_write(ssl_, src + off, static_cast<int>(len - off));
                if (n <= 0) return false;
            } else {
                n = ::send(fd_, src + off, len - off, MSG_NOSIGNAL);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) return false;
            }
            off += static_cast<size_t>(n);
        }
        return true;
    }

    ssize_t read_some(uint8_t* dst, size_t max_len) {
        if (max_len == 0) return 0;
        while (true) {
            ssize_t n = 0;
            if (ssl_) {
                n = SSL_read(ssl_, dst, static_cast<int>(max_len));
                if (n <= 0) return -1;
                return n;
            }
            n = ::recv(fd_, dst, max_len, 0);
            if (n < 0 && errno == EINTR) continue;
            return n;
        }
    }

    bool write_all(const std::vector<uint8_t>& buf) {
        return write_all(buf.data(), buf.size());
    }

    void close() {
        if (ssl_) {
            SSL_shutdown(ssl_);
            SSL_free(ssl_);
            ssl_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    int fd_ = -1;
    SSL* ssl_ = nullptr;
};

class TLSContext {
public:
    TLSContext() = default;
    ~TLSContext() {
        if (ctx_) SSL_CTX_free(ctx_);
    }

    TLSContext(const TLSContext&) = delete;
    TLSContext& operator=(const TLSContext&) = delete;

    bool init(const std::string& cert_path, const std::string& key_path, std::string& err_out) {
        ctx_ = SSL_CTX_new(TLS_server_method());
        if (!ctx_) {
            err_out = "SSL_CTX_new failed";
            return false;
        }
        SSL_CTX_set_min_proto_version(ctx_, TLS1_2_VERSION);

        if (!cert_path.empty() && !key_path.empty()) {
            if (SSL_CTX_use_certificate_chain_file(ctx_, cert_path.c_str()) <= 0) {
                err_out = "failed to load TLS certificate: " + cert_path;
                return false;
            }
            if (SSL_CTX_use_PrivateKey_file(ctx_, key_path.c_str(), SSL_FILETYPE_PEM) <= 0) {
                err_out = "failed to load TLS private key: " + key_path;
                return false;
            }
            return true;
        }

        return generate_self_signed(err_out);
    }

    SSL* accept_tls(int fd, std::string& sni_host) {
        if (!ctx_) return nullptr;
        SSL* ssl = SSL_new(ctx_);
        if (!ssl) return nullptr;
        SSL_set_fd(ssl, fd);
        if (SSL_accept(ssl) <= 0) {
            SSL_free(ssl);
            return nullptr;
        }
        if (const char* sni = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name); sni != nullptr) {
            sni_host = sni;
        }
        return ssl;
    }

private:
    SSL_CTX* ctx_ = nullptr;

    bool generate_self_signed(std::string& err_out) {
        EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
        if (!pctx) {
            err_out = "EVP_PKEY_CTX_new_id failed";
            return false;
        }
        EVP_PKEY* pkey = nullptr;
        if (EVP_PKEY_keygen_init(pctx) <= 0 ||
            EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1) <= 0 ||
            EVP_PKEY_keygen(pctx, &pkey) <= 0) {
            EVP_PKEY_CTX_free(pctx);
            err_out = "ECDSA P-256 keygen failed";
            return false;
        }
        EVP_PKEY_CTX_free(pctx);

        X509* x509 = X509_new();
        ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
        X509_gmtime_adj(X509_getm_notBefore(x509), -3600);
        X509_gmtime_adj(X509_getm_notAfter(x509), 365L * 24 * 3600);
        X509_set_pubkey(x509, pkey);

        X509_NAME* name = X509_get_subject_name(x509);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   reinterpret_cast<const unsigned char*>("*.air.local"), -1, -1, 0);
        X509_set_issuer_name(x509, name);

        X509V3_CTX v3ctx;
        X509V3_set_ctx_nodb(&v3ctx);
        X509V3_set_ctx(&v3ctx, x509, x509, nullptr, nullptr, 0);
        X509_EXTENSION* ext = X509V3_EXT_conf_nid(nullptr, &v3ctx, NID_subject_alt_name,
                                                  const_cast<char*>("DNS:*.air.local,DNS:localhost"));
        if (ext) {
            X509_add_ext(x509, ext, -1);
            X509_EXTENSION_free(ext);
        }

        if (X509_sign(x509, pkey, EVP_sha256()) <= 0) {
            X509_free(x509);
            EVP_PKEY_free(pkey);
            err_out = "X509_sign failed";
            return false;
        }

        bool ok = SSL_CTX_use_certificate(ctx_, x509) == 1 &&
                  SSL_CTX_use_PrivateKey(ctx_, pkey) == 1;
        X509_free(x509);
        EVP_PKEY_free(pkey);
        if (!ok) {
            err_out = "SSL_CTX_use_certificate/PrivateKey failed";
        }
        return ok;
    }
};

inline std::vector<uint8_t> build_pg_message(char type, const std::vector<uint8_t>& body) {
    std::vector<uint8_t> out;
    out.reserve(5 + body.size());
    out.push_back(static_cast<uint8_t>(type));
    append_be32(out, static_cast<uint32_t>(4 + body.size()));
    out.insert(out.end(), body.begin(), body.end());
    return out;
}

inline std::vector<uint8_t> build_error_response(const std::string& severity,
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

inline std::vector<uint8_t> build_auth_ok() {
    std::vector<uint8_t> body;
    append_be32(body, 0);
    return build_pg_message('R', body);
}

inline std::vector<uint8_t> build_auth_cleartext() {
    std::vector<uint8_t> body;
    append_be32(body, 3);
    return build_pg_message('R', body);
}

inline std::vector<uint8_t> build_param_status(const std::string& k, const std::string& v) {
    std::vector<uint8_t> body;
    append_cstr(body, k);
    append_cstr(body, v);
    return build_pg_message('S', body);
}

inline std::vector<uint8_t> build_backend_key_data(uint32_t pid, uint32_t secret) {
    std::vector<uint8_t> body;
    append_be32(body, pid);
    append_be32(body, secret);
    return build_pg_message('K', body);
}

inline std::vector<uint8_t> build_ready_for_query(char status) {
    std::vector<uint8_t> body{static_cast<uint8_t>(status)};
    return build_pg_message('Z', body);
}

inline std::vector<uint8_t> build_password_message(const std::string& pw) {
    std::vector<uint8_t> body;
    append_cstr(body, pw);
    return build_pg_message('p', body);
}

inline std::vector<uint8_t> build_simple_query(const std::string& sql) {
    std::vector<uint8_t> body;
    append_cstr(body, sql);
    return build_pg_message('Q', body);
}

inline std::vector<uint8_t> build_sync_message() {
    return build_pg_message('S', {});
}

inline std::vector<uint8_t> build_parse_message(const std::string& name,
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

inline std::vector<uint8_t> build_startup_message(
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

inline bool read_pg_packet(Stream& s, char& type_out, std::vector<uint8_t>& body_out) {
    uint8_t hdr[5];
    if (!s.read_exact(hdr, 5)) return false;
    type_out = static_cast<char>(hdr[0]);
    uint32_t len = read_be32(hdr + 1);
    if (len < 4 || len > 64 * 1024 * 1024) return false;
    size_t body_len = len - 4;
    body_out.resize(body_len);
    if (body_len > 0 && !s.read_exact(body_out.data(), body_len)) {
        return false;
    }
    return true;
}

inline std::unordered_map<std::string, std::string> parse_startup_params(const std::vector<uint8_t>& body) {
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

inline std::string trim_ascii(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
    return s.substr(start, end - start);
}

inline bool iequals(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

inline bool validate_tenant_id(const std::string& id) {
    if (id.empty() || id.size() > 63) return false;
    for (unsigned char ch : id) {
        if (!(std::isalnum(ch) || ch == '_' || ch == '-')) {
            return false;
        }
    }
    return true;
}

inline std::string parse_sni_tenant(const std::string& sni_host) {
    std::string trimmed = trim_ascii(sni_host);
    if (trimmed.empty()) return "";
    auto dot = trimmed.find('.');
    if (dot == std::string::npos) return trimmed;
    if (dot == 0) return "";
    return trimmed.substr(0, dot);
}

inline std::string extract_tenant(const std::string& sni_host,
                                  const std::unordered_map<std::string, std::string>& params) {
    auto it = params.find("database");
    if (it != params.end()) {
        std::string db = trim_ascii(it->second);
        if (!db.empty() && db != "postgres") {
            return db;
        }
    }
    return parse_sni_tenant(sni_host);
}

inline int dial_backend_socket(const std::string& host, const std::string& sock_dir, int port) {
    bool is_local = host.empty() || host == "127.0.0.1" || host == "localhost";
    if (!sock_dir.empty() && is_local) {
        std::string sock_path = sock_dir + "/.s.PGSQL." + std::to_string(port);
        if (sock_path.size() < sizeof(sockaddr_un::sun_path)) {
            int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
            if (fd >= 0) {
                struct timeval tv{5, 0};
                setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
                sockaddr_un addr{};
                addr.sun_family = AF_UNIX;
                std::strncpy(addr.sun_path, sock_path.c_str(), sizeof(addr.sun_path) - 1);
                if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
                    return fd;
                }
                ::close(fd);
            }
        }
    }

    std::string target_host = host.empty() ? "127.0.0.1" : host;
    std::string port_str = std::to_string(port);
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (::getaddrinfo(target_host.c_str(), port_str.c_str(), &hints, &res) != 0 || !res) {
        return -1;
    }
    int fd = -1;
    for (auto* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        struct timeval tv{5, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    return fd;
}

inline std::string base64_encode(const uint8_t* data, size_t len) {
    std::string out(4 * ((len + 2) / 3), '\0');
    int n = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()), data, static_cast<int>(len));
    if (n < 0) return "";
    out.resize(static_cast<size_t>(n));
    return out;
}

inline std::vector<uint8_t> base64_decode(const std::string& b64) {
    if (b64.empty()) return {};
    std::vector<uint8_t> out(3 * ((b64.size() + 3) / 4));
    int n = EVP_DecodeBlock(out.data(), reinterpret_cast<const unsigned char*>(b64.data()),
                            static_cast<int>(b64.size()));
    if (n < 0) return {};
    size_t pad = 0;
    if (!b64.empty() && b64.back() == '=') ++pad;
    if (b64.size() >= 2 && b64[b64.size() - 2] == '=') ++pad;
    size_t actual = static_cast<size_t>(n);
    if (pad <= actual) actual -= pad;
    out.resize(actual);
    return out;
}

inline std::vector<uint8_t> hmac_sha256(const std::vector<uint8_t>& key, const std::string& data) {
    std::vector<uint8_t> out(EVP_MAX_MD_SIZE);
    unsigned int out_len = 0;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(),
         out.data(), &out_len);
    out.resize(out_len);
    return out;
}

inline std::vector<uint8_t> sha256_bytes(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> out(SHA256_DIGEST_LENGTH);
    SHA256(data.data(), data.size(), out.data());
    return out;
}

inline bool scram_sha256_client_auth(Stream& s, const std::string& user, const std::string& password,
                                     std::string& err_out) {
    uint8_t rand_bytes[18];
    if (RAND_bytes(rand_bytes, sizeof(rand_bytes)) != 1) {
        err_out = "RAND_bytes failed";
        return false;
    }
    std::string client_nonce = base64_encode(rand_bytes, sizeof(rand_bytes));
    std::string client_first_bare = "n=" + user + ",r=" + client_nonce;
    std::string client_first = "n,," + client_first_bare;

    // SASLInitialResponse ('p')
    std::vector<uint8_t> init_body;
    append_cstr(init_body, "SCRAM-SHA-256");
    append_be32(init_body, static_cast<uint32_t>(client_first.size()));
    init_body.insert(init_body.end(), client_first.begin(), client_first.end());
    if (!s.write_all(build_pg_message('p', init_body))) {
        err_out = "failed to write SASLInitialResponse";
        return false;
    }

    char type = 0;
    std::vector<uint8_t> body;
    if (!read_pg_packet(s, type, body) || type != 'R' || body.size() < 4) {
        err_out = "failed to read SASLContinue";
        return false;
    }
    uint32_t auth_code = read_be32(body.data());
    if (auth_code != 11) {
        err_out = "expected AuthenticationSASLContinue (11)";
        return false;
    }
    std::string server_first(reinterpret_cast<const char*>(body.data() + 4), body.size() - 4);

    // Parse r=...,s=...,i=...
    std::string server_nonce, salt_b64;
    int iterations = 0;
    size_t pos = 0;
    while (pos < server_first.size()) {
        auto comma = server_first.find(',', pos);
        std::string tok = (comma == std::string::npos) ? server_first.substr(pos)
                                                       : server_first.substr(pos, comma - pos);
        if (tok.rfind("r=", 0) == 0) server_nonce = tok.substr(2);
        else if (tok.rfind("s=", 0) == 0) salt_b64 = tok.substr(2);
        else if (tok.rfind("i=", 0) == 0) iterations = std::stoi(tok.substr(2));
        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
    if (server_nonce.rfind(client_nonce, 0) != 0 || salt_b64.empty() || iterations <= 0) {
        err_out = "invalid SCRAM server-first-message";
        return false;
    }

    auto salt = base64_decode(salt_b64);
    std::vector<uint8_t> salted_pw(32);
    if (PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
                          salt.data(), static_cast<int>(salt.size()),
                          iterations, EVP_sha256(), 32, salted_pw.data()) != 1) {
        err_out = "PBKDF2 failed";
        return false;
    }

    auto client_key = hmac_sha256(salted_pw, "Client Key");
    auto stored_key = sha256_bytes(client_key);
    std::string client_final_without_proof = "c=biws,r=" + server_nonce;
    std::string auth_msg = client_first_bare + "," + server_first + "," + client_final_without_proof;
    auto client_sig = hmac_sha256(stored_key, auth_msg);

    std::vector<uint8_t> proof(32);
    for (size_t i = 0; i < 32; ++i) {
        proof[i] = client_key[i] ^ client_sig[i];
    }
    std::string client_final = client_final_without_proof + ",p=" + base64_encode(proof.data(), proof.size());

    std::vector<uint8_t> final_body(client_final.begin(), client_final.end());
    if (!s.write_all(build_pg_message('p', final_body))) {
        err_out = "failed to write SASLResponse";
        return false;
    }

    if (!read_pg_packet(s, type, body) || type != 'R' || body.size() < 4) {
        err_out = "failed to read SASLFinal";
        return false;
    }
    uint32_t final_code = read_be32(body.data());
    if (final_code != 12) {
        err_out = "SCRAM authentication rejected by backend";
        return false;
    }
    return true;
}

} // namespace air::gw
