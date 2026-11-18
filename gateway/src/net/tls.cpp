#include "net/tls.hpp"

#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

namespace air::gw {

TLSContext::~TLSContext() {
    if (ctx_) SSL_CTX_free(ctx_);
}

bool TLSContext::init(const std::string& cert_path, const std::string& key_path, std::string& err_out) {
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

SSL* TLSContext::accept_tls(int fd, std::string& sni_host) {
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

bool TLSContext::generate_self_signed(std::string& err_out) {
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

} // namespace air::gw
