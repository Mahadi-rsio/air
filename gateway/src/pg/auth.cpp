#include "pg/auth.hpp"
#include "pg/protocol.hpp"

#include <arpa/inet.h>
#include <cctype>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

namespace air::gw {

std::string trim_ascii(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
    return s.substr(start, end - start);
}

bool iequals(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

bool constant_time_equals(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    if (a.empty()) return true;
    return CRYPTO_memcmp(a.data(), b.data(), a.size()) == 0;
}

bool validate_tenant_id(const std::string& id) {
    if (id.empty() || id.size() > 63) return false;
    for (unsigned char ch : id) {
        if (!(std::isalnum(ch) || ch == '_' || ch == '-')) {
            return false;
        }
    }
    return true;
}

std::string parse_sni_tenant(const std::string& sni_host) {
    std::string trimmed = trim_ascii(sni_host);
    if (trimmed.empty()) return "";
    // Single-label hosts (e.g. "localhost") or IPv4 literals are not tenant SNI subdomains.
    struct in_addr ipv4{};
    if (::inet_pton(AF_INET, trimmed.c_str(), &ipv4) == 1) return "";
    auto dot = trimmed.find('.');
    if (dot == std::string::npos || dot == 0) return "";
    return trimmed.substr(0, dot);
}

std::string extract_tenant(const std::string& sni_host,
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

std::string base64_encode(const uint8_t* data, size_t len) {
    std::string out(4 * ((len + 2) / 3), '\0');
    int n = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()), data, static_cast<int>(len));
    if (n < 0) return "";
    out.resize(static_cast<size_t>(n));
    return out;
}

std::vector<uint8_t> base64_decode(const std::string& b64) {
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

std::vector<uint8_t> hmac_sha256(const std::vector<uint8_t>& key, const std::string& data) {
    std::vector<uint8_t> out(EVP_MAX_MD_SIZE);
    unsigned int out_len = 0;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(),
         out.data(), &out_len);
    out.resize(out_len);
    return out;
}

std::vector<uint8_t> sha256_bytes(const std::vector<uint8_t>& data) {
    std::vector<uint8_t> out(SHA256_DIGEST_LENGTH);
    SHA256(data.data(), data.size(), out.data());
    return out;
}

bool scram_sha256_client_auth(Stream& s,
                              const std::string& user,
                              const std::string& password,
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
    if (read_be32(body.data()) != 11) {
        err_out = "expected AuthenticationSASLContinue (11)";
        return false;
    }
    std::string server_first(reinterpret_cast<const char*>(body.data() + 4), body.size() - 4);

    // Parse r=<nonce>,s=<salt_b64>,i=<iterations>
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
    if (read_be32(body.data()) != 12) {
        err_out = "SCRAM authentication rejected by backend";
        return false;
    }
    return true;
}

void CancelRegistry::register_target(uint32_t gw_pid, uint32_t gw_secret, CancelTarget target) {
    std::lock_guard<std::mutex> lk(mu_);
    targets_[make_key(gw_pid, gw_secret)] = std::move(target);
}

void CancelRegistry::unregister_target(uint32_t gw_pid, uint32_t gw_secret) {
    std::lock_guard<std::mutex> lk(mu_);
    targets_.erase(make_key(gw_pid, gw_secret));
}

bool CancelRegistry::lookup(uint32_t gw_pid, uint32_t gw_secret, CancelTarget& out) const {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = targets_.find(make_key(gw_pid, gw_secret));
    if (it == targets_.end()) return false;
    out = it->second;
    return true;
}

} // namespace air::gw
