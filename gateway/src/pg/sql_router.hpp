#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>

namespace air::gw {

enum class RouteTarget {
    ReadEngine,
    WriteEngine,
};

enum class RouteReason {
    ReadOnlySelect,
    ReadOnlyUtility,
    WriteDML,
    WriteDDL,
    ModifyingCTE,
    RowLocking,
    SelectInto,
    MutatingFunction,
    TransactionWriteState,
    SessionPinned,
    ReadEngineUnavailableFallback,
    CausalLagFallback,
    ReadErrorFallback,
};

struct RouteDecision {
    RouteTarget target = RouteTarget::WriteEngine;
    RouteReason reason = RouteReason::WriteDML;
};

struct SessionRouteContext {
    bool in_transaction = false;
    bool tx_has_writes = false;
    bool session_pinned = false;
    bool read_engine_healthy = true;
    uint64_t session_write_lsn = 0;
    uint64_t read_engine_lsn = 0;
};

struct RoutingMetrics {
    std::atomic<uint64_t> read_routed{0};
    std::atomic<uint64_t> write_routed{0};
    std::atomic<uint64_t> fallbacks_to_write{0};
    std::atomic<uint64_t> read_errors_retried{0};
};

/// Classify a SQL string (single or multi-statement) for ReadEngine vs WriteEngine routing
/// using lexical scanning that respects comments, single quotes, double quotes, and dollar-quoting.
RouteDecision classify_sql_for_routing(std::string_view sql);

/// Decide the final route for a query given session transaction/pinning/LSN state,
/// automatically falling back to WriteEngine when ReadEngine is unavailable or lagging.
RouteDecision route_query(std::string_view sql,
                          const SessionRouteContext& ctx,
                          RoutingMetrics* metrics = nullptr);

/// Record a fallback from ReadEngine to WriteEngine after a transient/read-only error.
void record_read_fallback(RoutingMetrics* metrics);

} // namespace air::gw
