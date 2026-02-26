#include "net/serverTcp.h"

#include "query/sql.h"

#include "util/json.h"

namespace xeondb {

std::string ServerTcp::cmdShowMetrics(const SqlShowMetrics& showMetrics, const AuthedUser& u) {
    const std::string& ks = showMetrics.keyspace;
    if (authEnabled_ && !db_->canAccessKeyspace(u, ks)) {
        throw runtimeError("forbidden");
    }

    auto m = db_->keyspaceMetrics(ks);
    std::string out = std::string("{\"ok\":true,\"keyspace\":\"") + jsonEscape(ks) + "\"";
    out += std::string(",\"connections_active\":") + std::to_string(m.connectionsActive);

    out += ",\"connections_last24h_peak_4h\":[";
    for (usize i = 0; i < m.connectionsLast24hPeak4h.size(); i++) {
        if (i) {
            out += ",";
        }
        out += std::to_string(m.connectionsLast24hPeak4h[i]);
    }
    out += "]";

    out += ",\"queries_last24h_4h\":[";
    for (usize i = 0; i < m.queriesLast24h4h.size(); i++) {
        if (i) {
            out += ",";
        }
        out += std::to_string(m.queriesLast24h4h[i]);
    }
    out += "]";

    out += std::string(",\"queries_last24h_total\":") + std::to_string(m.queriesLast24hTotal);

    const u64 bytesUsed = bytesUsedForKeyspaceCached(ks);
    out += std::string(",\"bytes_used\":") + std::to_string(bytesUsed);

    if (auto quota = quotaBytesForKeyspace(ks); quota.has_value() && *quota > 0) {
        out += std::string(",\"quota_bytes\":") + std::to_string(*quota);
        out += std::string(",\"over_quota\":") + ((bytesUsed >= *quota) ? std::string("true") : std::string("false"));
    }

    out += ",\"labels_last24h_4h\":[";
    for (usize i = 0; i < m.labelsLast24h4h.size(); i++) {
        if (i) {
            out += ",";
        }
        out += std::string("\"") + jsonEscape(m.labelsLast24h4h[i]) + "\"";
    }
    out += "]";
    out += "}";
    return out;
}

}
