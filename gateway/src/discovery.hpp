#pragma once

#include "config.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <curl/curl.h>
#include <functional>
#include <iostream>
#include <libpq-fe.h>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace air::gw {

struct TenantRoute {
    std::string tenant_id;
    std::string state;
    std::string host;
    int port = 0;
    std::string sock_dir;
    std::string app_password;
};

namespace detail {

inline size_t curl_write_cb(void* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* out = static_cast<std::string*>(userdata);
    out->append(static_cast<const char*>(ptr), size * nmemb);
    return size * nmemb;
}

inline std::string json_extract_string(const std::string& json, const std::string& key) {
    std::string pat = "\"" + key + "\"";
    auto pos = json.find(pat);
    if (pos == std::string::npos) return "";
    pos = json.find(':', pos + pat.size());
    if (pos == std::string::npos) return "";
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n' || json[pos] == '\r')) {
        ++pos;
    }
    if (pos >= json.size() || json[pos] != '"') return "";
    ++pos;
    std::string val;
    while (pos < json.size()) {
        char ch = json[pos++];
        if (ch == '\\' && pos < json.size()) {
            char esc = json[pos++];
            switch (esc) {
                case '"': val.push_back('"'); break;
                case '\\': val.push_back('\\'); break;
                case '/': val.push_back('/'); break;
                case 'n': val.push_back('\n'); break;
                case 'r': val.push_back('\r'); break;
                case 't': val.push_back('\t'); break;
                default: val.push_back(esc); break;
            }
        } else if (ch == '"') {
            break;
        } else {
            val.push_back(ch);
        }
    }
    return val;
}

inline int json_extract_int(const std::string& json, const std::string& key) {
    std::string pat = "\"" + key + "\"";
    auto pos = json.find(pat);
    if (pos == std::string::npos) return 0;
    pos = json.find(':', pos + pat.size());
    if (pos == std::string::npos) return 0;
    ++pos;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) ++pos;
    size_t end = pos;
    if (end < json.size() && json[end] == '-') ++end;
    while (end < json.size() && std::isdigit(static_cast<unsigned char>(json[end]))) ++end;
    if (end == pos) return 0;
    try {
        return std::stoi(json.substr(pos, end - pos));
    } catch (...) {
        return 0;
    }
}

inline bool parse_route_json(const std::string& body, TenantRoute& out) {
    out.tenant_id = json_extract_string(body, "tenant_id");
    out.state = json_extract_string(body, "state");
    out.host = json_extract_string(body, "host");
    out.port = json_extract_int(body, "port");
    out.sock_dir = json_extract_string(body, "sock_dir");
    out.app_password = json_extract_string(body, "app_password");
    return !out.tenant_id.empty();
}

} // namespace detail

class TenantDiscovery {
public:
    using TenantCallback = std::function<void(const TenantRoute&)>;
    using TenantRemovedCallback = std::function<void(const std::string&)>;

    explicit TenantDiscovery(const Config& cfg)
        : cfg_(cfg) {}

    ~TenantDiscovery() {
        stop();
        if (pg_conn_) {
            PQfinish(pg_conn_);
            pg_conn_ = nullptr;
        }
    }

    TenantDiscovery(const TenantDiscovery&) = delete;
    TenantDiscovery& operator=(const TenantDiscovery&) = delete;

    void set_callbacks(TenantCallback on_upsert, TenantRemovedCallback on_remove) {
        std::lock_guard<std::mutex> lk(cb_mu_);
        on_upsert_ = std::move(on_upsert);
        on_remove_ = std::move(on_remove);
    }

    void start() {
        running_.store(true);
        // Initial synchronous discovery attempt (non-fatal if metadata PG is still booting).
        poll_metadata_once();
        worker_ = std::thread([this]() { discovery_loop(); });
    }

    void stop() {
        running_.store(false);
        cv_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    std::vector<TenantRoute> list_cached() const {
        std::lock_guard<std::mutex> lk(cache_mu_);
        std::vector<TenantRoute> out;
        out.reserve(cache_.size());
        for (const auto& [_, r] : cache_) {
            out.push_back(r);
        }
        return out;
    }

    bool resolve_tenant(const std::string& tenant_id, TenantRoute& route_out, std::string& err_out) {
        // Check metadata DB on cache miss so newly created DBs are discovered immediately.
        bool known = false;
        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            known = cache_.find(tenant_id) != cache_.end();
        }
        if (!known) {
            poll_metadata_once();
        }

        std::string url = cfg_.control_plane_url + "/internal/v1/tenants/" + tenant_id + "/route";
        if (!http_route_request("GET", url, route_out, err_out)) {
            return false;
        }
        if (!cfg_.backend_host.empty()) {
            route_out.host = cfg_.backend_host;
        }
        if (route_out.state == "SUSPENDED") {
            if (!wake_tenant(tenant_id, route_out, err_out)) {
                return false;
            }
        }
        upsert_cache(route_out);
        return true;
    }

    bool wake_tenant(const std::string& tenant_id, TenantRoute& route_out, std::string& err_out) {
        std::string url = cfg_.control_plane_url + "/internal/v1/tenants/" + tenant_id + "/wake";
        if (!http_route_request("POST", url, route_out, err_out)) {
            return false;
        }
        if (!cfg_.backend_host.empty()) {
            route_out.host = cfg_.backend_host;
        }
        if (route_out.state.empty()) {
            route_out.state = "ACTIVE";
        }
        upsert_cache(route_out);
        return true;
    }

    void touch_activity(const std::string& tenant_id) {
        if (tenant_id.empty()) return;
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lk(touch_mu_);
            auto it = last_touch_.find(tenant_id);
            if (it != last_touch_.end() &&
                std::chrono::duration_cast<std::chrono::seconds>(now - it->second).count() < 5) {
                return;
            }
            last_touch_[tenant_id] = now;
        }

        std::string url = cfg_.control_plane_url + "/internal/v1/tenants/" + tenant_id + "/activity";
        std::string token = cfg_.internal_token;
        std::thread([url, token]() {
            CURL* curl = curl_easy_init();
            if (!curl) return;
            struct curl_slist* hdrs = nullptr;
            if (!token.empty()) {
                std::string h = "X-Air-Internal-Token: " + token;
                hdrs = curl_slist_append(hdrs, h.c_str());
            }
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_POST, 1L);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "");
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2L);
            if (hdrs) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);
            curl_easy_perform(curl);
            if (hdrs) curl_slist_free_all(hdrs);
            curl_easy_cleanup(curl);
        }).detach();
    }

private:
    Config cfg_;
    std::atomic<bool> running_{false};
    std::thread worker_;
    std::mutex cv_mu_;
    std::condition_variable cv_;

    std::mutex pq_mu_;
    PGconn* pg_conn_ = nullptr;

    mutable std::mutex cache_mu_;
    std::unordered_map<std::string, TenantRoute> cache_;

    std::mutex cb_mu_;
    TenantCallback on_upsert_;
    TenantRemovedCallback on_remove_;

    std::mutex touch_mu_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_touch_;

    void discovery_loop() {
        while (running_.load()) {
            poll_metadata_once();
            std::unique_lock<std::mutex> lk(cv_mu_);
            cv_.wait_for(lk, std::chrono::milliseconds(cfg_.discovery_interval_ms),
                         [this]() { return !running_.load(); });
        }
    }

    void upsert_cache(const TenantRoute& r) {
        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            auto& cur = cache_[r.tenant_id];
            if (r.app_password.empty() && !cur.app_password.empty()) {
                TenantRoute merged = r;
                merged.app_password = cur.app_password;
                cur = merged;
            } else {
                cur = r;
            }
        }
        TenantCallback cb;
        {
            std::lock_guard<std::mutex> lk(cb_mu_);
            cb = on_upsert_;
        }
        if (cb) cb(r);
    }

    void poll_metadata_once() {
        std::vector<TenantRoute> discovered;
        {
            std::lock_guard<std::mutex> lk(pq_mu_);
            if (!pg_conn_ || PQstatus(pg_conn_) != CONNECTION_OK) {
                if (pg_conn_) {
                    PQfinish(pg_conn_);
                    pg_conn_ = nullptr;
                }
                pg_conn_ = PQconnectdb(cfg_.meta_dsn.c_str());
                if (!pg_conn_ || PQstatus(pg_conn_) != CONNECTION_OK) {
                    if (pg_conn_) {
                        PQfinish(pg_conn_);
                        pg_conn_ = nullptr;
                    }
                    return;
                }
            }

            PGresult* res = PQexec(pg_conn_, "SELECT tenant_id, state, direct_port FROM clusters");
            if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
                if (res) PQclear(res);
                PQfinish(pg_conn_);
                pg_conn_ = nullptr;
                return;
            }
            int rows = PQntuples(res);
            discovered.reserve(static_cast<size_t>(rows));
            for (int i = 0; i < rows; ++i) {
                TenantRoute r;
                r.tenant_id = PQgetvalue(res, i, 0);
                r.state = PQgetvalue(res, i, 1);
                r.port = std::atoi(PQgetvalue(res, i, 2));
                if (!cfg_.backend_host.empty()) {
                    r.host = cfg_.backend_host;
                }
                discovered.push_back(std::move(r));
            }
            PQclear(res);
        }

        std::unordered_set<std::string> seen;
        std::vector<TenantRoute> changed;
        std::vector<std::string> removed;

        {
            std::lock_guard<std::mutex> lk(cache_mu_);
            for (const auto& d : discovered) {
                seen.insert(d.tenant_id);
                auto it = cache_.find(d.tenant_id);
                if (it == cache_.end()) {
                    cache_[d.tenant_id] = d;
                    changed.push_back(d);
                    std::cout << "[air-gateway] discovered tenant db=" << d.tenant_id
                              << " state=" << d.state << " port=" << d.port << std::endl;
                } else if (it->second.state != d.state || it->second.port != d.port) {
                    it->second.state = d.state;
                    it->second.port = d.port;
                    if (!d.host.empty()) it->second.host = d.host;
                    changed.push_back(it->second);
                }
            }
            for (auto it = cache_.begin(); it != cache_.end();) {
                if (seen.find(it->first) == seen.end()) {
                    removed.push_back(it->first);
                    std::cout << "[air-gateway] removed deleted tenant db=" << it->first << std::endl;
                    it = cache_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        TenantCallback upsert_cb;
        TenantRemovedCallback remove_cb;
        {
            std::lock_guard<std::mutex> lk(cb_mu_);
            upsert_cb = on_upsert_;
            remove_cb = on_remove_;
        }
        if (upsert_cb) {
            for (const auto& r : changed) upsert_cb(r);
        }
        if (remove_cb) {
            for (const auto& tid : removed) remove_cb(tid);
        }
    }

    bool http_route_request(const std::string& method, const std::string& url,
                            TenantRoute& route_out, std::string& err_out) {
        CURL* curl = curl_easy_init();
        if (!curl) {
            err_out = "curl_easy_init failed";
            return false;
        }
        std::string resp_body;
        struct curl_slist* hdrs = nullptr;
        if (!cfg_.internal_token.empty()) {
            std::string h = "X-Air-Internal-Token: " + cfg_.internal_token;
            hdrs = curl_slist_append(hdrs, h.c_str());
        }
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        if (method == "POST") {
            curl_easy_setopt(curl, CURLOPT_POST, 1L);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "");
        }
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 35L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, detail::curl_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp_body);
        if (hdrs) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, hdrs);

        CURLcode rc = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        if (hdrs) curl_slist_free_all(hdrs);
        curl_easy_cleanup(curl);

        if (rc != CURLE_OK) {
            err_out = std::string("control plane request failed: ") + curl_easy_strerror(rc);
            return false;
        }
        if (http_code != 200) {
            err_out = "control plane returned HTTP " + std::to_string(http_code) + ": " + resp_body;
            return false;
        }
        if (!detail::parse_route_json(resp_body, route_out)) {
            err_out = "invalid route JSON from control plane";
            return false;
        }
        return true;
    }
};

} // namespace air::gw
