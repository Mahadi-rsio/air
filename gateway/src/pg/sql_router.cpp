#include "pg/sql_router.hpp"

#include <algorithm>
#include <cctype>
#include <vector>

namespace air::gw {
namespace {

enum class TokKind {
    Word,
    Symbol,
    Semicolon,
};

struct Token {
    TokKind kind;
    std::string upper; // uppercased for Word tokens
};

bool is_ident_start(char c) {
    unsigned char u = static_cast<unsigned char>(c);
    return std::isalpha(u) || c == '_' || c == '$';
}

bool is_ident_char(char c) {
    unsigned char u = static_cast<unsigned char>(c);
    return std::isalnum(u) || c == '_' || c == '$';
}

std::string to_upper_ascii(std::string_view sv) {
    std::string out(sv);
    for (char& c : out) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return out;
}

std::vector<std::vector<Token>> tokenize_statements(std::string_view sql) {
    std::vector<std::vector<Token>> stmts(1);
    size_t i = 0;
    const size_t n = sql.size();

    while (i < n) {
        char c = sql[i];

        if (std::isspace(static_cast<unsigned char>(c))) {
            ++i;
            continue;
        }

        // Line comment -- ...
        if (c == '-' && i + 1 < n && sql[i + 1] == '-') {
            i += 2;
            while (i < n && sql[i] != '\n') ++i;
            continue;
        }

        // Block comment /* ... */ (supports nesting)
        if (c == '/' && i + 1 < n && sql[i + 1] == '*') {
            i += 2;
            int depth = 1;
            while (i < n && depth > 0) {
                if (sql[i] == '/' && i + 1 < n && sql[i + 1] == '*') {
                    ++depth;
                    i += 2;
                } else if (sql[i] == '*' && i + 1 < n && sql[i + 1] == '/') {
                    --depth;
                    i += 2;
                } else {
                    ++i;
                }
            }
            continue;
        }

        // Single-quoted string literal
        if (c == '\'') {
            ++i;
            while (i < n) {
                if (sql[i] == '\'') {
                    if (i + 1 < n && sql[i + 1] == '\'') {
                        i += 2;
                        continue;
                    }
                    ++i;
                    break;
                }
                ++i;
            }
            stmts.back().push_back({TokKind::Symbol, "'"});
            continue;
        }

        // Double-quoted identifier
        if (c == '"') {
            ++i;
            std::string ident;
            while (i < n) {
                if (sql[i] == '"') {
                    if (i + 1 < n && sql[i + 1] == '"') {
                        ident.push_back('"');
                        i += 2;
                        continue;
                    }
                    ++i;
                    break;
                }
                ident.push_back(sql[i]);
                ++i;
            }
            stmts.back().push_back({TokKind::Word, to_upper_ascii(ident)});
            continue;
        }

        // Dollar-quoted string literal $tag$...$tag$
        if (c == '$') {
            size_t tag_end = i + 1;
            while (tag_end < n && (std::isalnum(static_cast<unsigned char>(sql[tag_end])) || sql[tag_end] == '_')) {
                ++tag_end;
            }
            if (tag_end < n && sql[tag_end] == '$') {
                std::string_view tag = sql.substr(i, tag_end - i + 1);
                size_t close_pos = sql.find(tag, tag_end + 1);
                if (close_pos != std::string_view::npos) {
                    i = close_pos + tag.size();
                } else {
                    i = n;
                }
                stmts.back().push_back({TokKind::Symbol, "$$"});
                continue;
            }
        }

        if (c == ';') {
            if (!stmts.back().empty()) {
                stmts.emplace_back();
            }
            ++i;
            continue;
        }

        if (is_ident_start(c)) {
            size_t start = i;
            ++i;
            while (i < n && is_ident_char(sql[i])) ++i;
            stmts.back().push_back({TokKind::Word, to_upper_ascii(sql.substr(start, i - start))});
            continue;
        }

        stmts.back().push_back({TokKind::Symbol, std::string(1, c)});
        ++i;
    }

    if (stmts.size() > 1 && stmts.back().empty()) {
        stmts.pop_back();
    }
    return stmts;
}

bool is_write_keyword(const std::string& w) {
    return w == "INSERT" || w == "UPDATE" || w == "DELETE" || w == "MERGE";
}

bool is_ddl_or_write_head(const std::string& w) {
    return is_write_keyword(w) ||
           w == "CREATE" || w == "ALTER" || w == "DROP" || w == "TRUNCATE" ||
           w == "GRANT" || w == "REVOKE" || w == "VACUUM" || w == "ANALYZE" ||
           w == "CLUSTER" || w == "REINDEX" || w == "COMMENT" || w == "REFRESH" ||
           w == "DO" || w == "CALL" || w == "LOCK" || w == "CHECKPOINT" ||
           w == "IMPORT" || w == "REASSIGN" || w == "SECURITY" || w == "NOTIFY" ||
           w == "LISTEN" || w == "UNLISTEN";
}

bool is_mutating_function(const std::string& w) {
    return w == "NEXTVAL" || w == "SETVAL" ||
           w == "PG_ADVISORY_LOCK" || w == "PG_ADVISORY_LOCK_SHARED" ||
           w == "PG_TRY_ADVISORY_LOCK" || w == "PG_TRY_ADVISORY_LOCK_SHARED" ||
           w == "PG_ADVISORY_XACT_LOCK" || w == "PG_ADVISORY_XACT_LOCK_SHARED" ||
           w == "PG_ADVISORY_UNLOCK" || w == "PG_ADVISORY_UNLOCK_ALL" ||
           w == "LO_CREATE" || w == "LO_IMPORT" || w == "LO_UNLINK" ||
           w == "PG_RELOAD_CONF" || w == "PG_ROTATE_LOGFILE" ||
           w == "PG_TERMINATE_BACKEND" || w == "PG_CANCEL_BACKEND";
}

RouteDecision classify_single_statement(const std::vector<Token>& toks) {
    if (toks.empty()) {
        return {RouteTarget::ReadEngine, RouteReason::ReadOnlyUtility};
    }

    const std::string& head = toks[0].upper;

    if (is_write_keyword(head)) {
        return {RouteTarget::WriteEngine, RouteReason::WriteDML};
    }
    if (is_ddl_or_write_head(head)) {
        return {RouteTarget::WriteEngine, RouteReason::WriteDDL};
    }

    // COPY ... FROM -> WriteEngine; COPY (...) TO -> ReadEngine (checking top-level paren_depth == 0)
    if (head == "COPY") {
        int paren_depth = 0;
        for (size_t i = 1; i < toks.size(); ++i) {
            const std::string& w = toks[i].upper;
            if (w == "(") {
                ++paren_depth;
                continue;
            }
            if (w == ")") {
                if (paren_depth > 0) --paren_depth;
                continue;
            }
            if (paren_depth == 0) {
                if (w == "FROM") {
                    return {RouteTarget::WriteEngine, RouteReason::WriteDML};
                }
                if (w == "TO") {
                    return {RouteTarget::ReadEngine, RouteReason::ReadOnlyUtility};
                }
            } else {
                if (is_write_keyword(w)) {
                    return {RouteTarget::WriteEngine, RouteReason::ModifyingCTE};
                }
                if (is_mutating_function(w) && i + 1 < toks.size() && toks[i + 1].upper == "(") {
                    return {RouteTarget::WriteEngine, RouteReason::MutatingFunction};
                }
            }
        }
        return {RouteTarget::WriteEngine, RouteReason::WriteDML};
    }

    // BEGIN / START TRANSACTION READ WRITE -> WriteEngine; READ ONLY or plain BEGIN -> ReadEngine
    if (head == "BEGIN" || head == "START") {
        for (size_t i = 1; i + 1 < toks.size(); ++i) {
            if (toks[i].upper == "READ" && toks[i + 1].upper == "WRITE") {
                return {RouteTarget::WriteEngine, RouteReason::TransactionWriteState};
            }
        }
        return {RouteTarget::ReadEngine, RouteReason::ReadOnlyUtility};
    }

    if (head == "COMMIT" || head == "END" || head == "ROLLBACK" ||
        head == "SHOW" || head == "DISCARD" || head == "DEALLOCATE") {
        return {RouteTarget::ReadEngine, RouteReason::ReadOnlyUtility};
    }

    if (head == "SET" || head == "RESET") {
        // Attempting to disable read-only mode routes to WriteEngine (or rejected by air_ro).
        for (size_t i = 1; i < toks.size(); ++i) {
            if (toks[i].upper == "TRANSACTION_READ_ONLY" ||
                toks[i].upper == "DEFAULT_TRANSACTION_READ_ONLY") {
                return {RouteTarget::WriteEngine, RouteReason::SessionPinned};
            }
        }
        return {RouteTarget::ReadEngine, RouteReason::ReadOnlyUtility};
    }

    // Scan remaining tokens for modifying CTEs, SELECT INTO, FOR UPDATE/SHARE, or mutating functions.
    for (size_t i = 0; i < toks.size(); ++i) {
        const std::string& w = toks[i].upper;
        if (i > 0 && is_write_keyword(w)) {
            return {RouteTarget::WriteEngine, RouteReason::ModifyingCTE};
        }
        if (w == "INTO" && head == "SELECT") {
            return {RouteTarget::WriteEngine, RouteReason::SelectInto};
        }
        if (w == "FOR" && i + 1 < toks.size()) {
            const std::string& next = toks[i + 1].upper;
            if (next == "UPDATE" || next == "SHARE" || next == "NO" || next == "KEY") {
                return {RouteTarget::WriteEngine, RouteReason::RowLocking};
            }
        }
        if (is_mutating_function(w) && i + 1 < toks.size() && toks[i + 1].upper == "(") {
            return {RouteTarget::WriteEngine, RouteReason::MutatingFunction};
        }
    }

    if (head == "SELECT" || head == "WITH" || head == "VALUES" ||
        head == "TABLE" || head == "EXPLAIN") {
        return {RouteTarget::ReadEngine, RouteReason::ReadOnlySelect};
    }

    // Safe default for unknown statements: WriteEngine.
    return {RouteTarget::WriteEngine, RouteReason::WriteDDL};
}

} // namespace

RouteDecision classify_sql_for_routing(std::string_view sql) {
    auto stmts = tokenize_statements(sql);
    RouteDecision overall{RouteTarget::ReadEngine, RouteReason::ReadOnlySelect};
    for (const auto& toks : stmts) {
        if (toks.empty()) continue;
        RouteDecision d = classify_single_statement(toks);
        if (d.target == RouteTarget::WriteEngine) {
            return d;
        }
        overall = d;
    }
    return overall;
}

RouteDecision route_query(std::string_view sql,
                          const SessionRouteContext& ctx,
                          RoutingMetrics* metrics) {
    RouteDecision d = classify_sql_for_routing(sql);

    if (d.target == RouteTarget::ReadEngine) {
        if (ctx.session_pinned) {
            d = {RouteTarget::WriteEngine, RouteReason::SessionPinned};
        } else if (ctx.in_transaction && ctx.tx_has_writes) {
            d = {RouteTarget::WriteEngine, RouteReason::TransactionWriteState};
        } else if (!ctx.read_engine_healthy) {
            d = {RouteTarget::WriteEngine, RouteReason::ReadEngineUnavailableFallback};
            if (metrics) metrics->fallbacks_to_write.fetch_add(1, std::memory_order_relaxed);
        } else if (ctx.session_write_lsn > 0 && ctx.read_engine_lsn < ctx.session_write_lsn) {
            d = {RouteTarget::WriteEngine, RouteReason::CausalLagFallback};
            if (metrics) metrics->fallbacks_to_write.fetch_add(1, std::memory_order_relaxed);
        }
    }

    if (metrics) {
        if (d.target == RouteTarget::ReadEngine) {
            metrics->read_routed.fetch_add(1, std::memory_order_relaxed);
        } else {
            metrics->write_routed.fetch_add(1, std::memory_order_relaxed);
        }
    }
    return d;
}

void record_read_fallback(RoutingMetrics* metrics) {
    if (metrics) {
        metrics->fallbacks_to_write.fetch_add(1, std::memory_order_relaxed);
        metrics->read_errors_retried.fetch_add(1, std::memory_order_relaxed);
    }
}

} // namespace air::gw
