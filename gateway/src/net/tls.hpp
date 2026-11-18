#pragma once

#include <openssl/ssl.h>
#include <string>

namespace air::gw {

/**
 * Manages the OpenSSL server SSL_CTX for PostgreSQL SSLRequest upgrades.
 * Uses explicit cert/key files when provided, or generates an ephemeral ECDSA P-256
 * self-signed certificate for `*.air.local` / `localhost` in development.
 */
class TLSContext {
public:
    TLSContext() = default;
    ~TLSContext();

    TLSContext(const TLSContext&) = delete;
    TLSContext& operator=(const TLSContext&) = delete;

    /// Initialize SSL_CTX with TLSv1.2+ and load or generate server credentials.
    bool init(const std::string& cert_path, const std::string& key_path, std::string& err_out);

    /// Perform TLS handshake on `fd` and extract optional SNI hostname. Returns nullptr on failure.
    SSL* accept_tls(int fd, std::string& sni_host);

private:
    SSL_CTX* ctx_ = nullptr;

    bool generate_self_signed(std::string& err_out);
};

} // namespace air::gw
