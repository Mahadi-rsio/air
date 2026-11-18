#pragma once

#include <regex>
#include <string>

namespace air::gw {

// Cached regexes for detecting session-level SQL commands in transaction pooling mode.

inline const std::regex& re_set_stmt() {
    static const std::regex r(
        R"(^\s*SET\s+([A-Za-z_][A-Za-z0-9_\.]*)\s*(?:=|TO)\s*([\s\S]+?)\s*;?\s*$)",
        std::regex::icase);
    return r;
}

inline const std::regex& re_reset_stmt() {
    static const std::regex r(R"(^\s*RESET\s+([A-Za-z_][A-Za-z0-9_\.]*|ALL)\s*;?\s*$)",
                              std::regex::icase);
    return r;
}

inline const std::regex& re_prepare_stmt() {
    static const std::regex r(
        R"(^\s*PREPARE\s+([A-Za-z_][A-Za-z0-9_]*)\s*(\([^)]*\))?\s+AS\s+([\s\S]+?)\s*;?\s*$)",
        std::regex::icase);
    return r;
}

inline const std::regex& re_dealloc_stmt() {
    static const std::regex r(
        R"(^\s*DEALLOCATE\s+(?:PREPARE\s+)?(ALL|[A-Za-z_][A-Za-z0-9_]*)\s*;?\s*$)",
        std::regex::icase);
    return r;
}

inline const std::regex& re_execute_stmt() {
    static const std::regex r(
        R"(^(\s*EXECUTE\s+)([A-Za-z_][A-Za-z0-9_]*)(\s*(?:\([\s\S]*\))?\s*;?\s*)$)",
        std::regex::icase);
    return r;
}

inline const std::regex& re_listen_stmt() {
    static const std::regex r(R"(^\s*(LISTEN|UNLISTEN)\b)", std::regex::icase);
    return r;
}

inline const std::regex& re_temp_table_stmt() {
    static const std::regex r(R"(^\s*CREATE\s+(TEMP|TEMPORARY)\s+TABLE\b)", std::regex::icase);
    return r;
}

inline const std::regex& re_adv_lock_stmt() {
    static const std::regex r(R"(pg_advisory_lock)", std::regex::icase);
    return r;
}

/// Return true if `sql` creates session-scoped state (LISTEN, TEMP TABLE, advisory locks)
/// that requires pinning the backend connection to this client until disconnect.
inline bool is_session_pinning_sql(const std::string& sql) {
    return std::regex_search(sql, re_listen_stmt()) ||
           std::regex_search(sql, re_temp_table_stmt()) ||
           std::regex_search(sql, re_adv_lock_stmt());
}

} // namespace air::gw
