#include "net/serverTcp.h"

#include "net/detail/serverTcpInternal.h"

#include "query/sql.h"

#include "util/json.h"

namespace xeondb {

std::string ServerTcp::cmdInsert(const SqlInsert& insert, const std::string& currentKeyspace, const AuthedUser& u) {
    using server_tcp_detail::isSystemKeyspaceName;

    auto keyspace = insert.keyspace.empty() ? currentKeyspace : insert.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    auto retTable = db_->openTable(keyspace, insert.table);
    auto pkIndex = retTable->schema().primaryKeyIndex;
    auto pkName = retTable->schema().columns[pkIndex].name;

    std::optional<usize> pkPos;
    for (usize i = 0; i < insert.columns.size(); i++) {
        if (insert.columns[i] == pkName) {
            pkPos = i;
        }
    }
    if (!pkPos.has_value()) {
        throw runtimeError("Missing pk");
    }

    std::vector<std::pair<byteVec, byteVec>> prepared;
    prepared.reserve(insert.rows.size());
    u64 estimatedWriteBytes = 0;
    for (const auto& row : insert.rows) {
        auto pkLit = row[*pkPos];
        byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, pkLit);
        byteVec rowBytesBuf = rowBytes(retTable->schema(), insert.columns, row, pkBytes);
        estimatedWriteBytes += static_cast<u64>(pkBytes.size());
        estimatedWriteBytes += static_cast<u64>(rowBytesBuf.size());
        estimatedWriteBytes += 64;
        prepared.push_back({std::move(pkBytes), std::move(rowBytesBuf)});
    }

    if (auto quota = quotaBytesForKeyspace(keyspace); quota.has_value() && *quota > 0) {
        if (!quotaWouldAllowAndReserve(keyspace, *quota, estimatedWriteBytes)) {
            throw runtimeError("quota_exceeded");
        }
    }

    for (const auto& r : prepared) {
        retTable->putRow(r.first, r.second);
    }

    if (authEnabled_ && isSystemKeyspaceName(keyspace)) {
        if (insert.table == "USERS") {
            for (const auto& row : insert.rows) {
                std::optional<std::string> username;
                std::optional<std::string> password;
                std::optional<i32> level;
                std::optional<bool> enabled;
                for (usize i = 0; i < insert.columns.size(); i++) {
                    const auto& c = insert.columns[i];
                    const auto& v = row[i];
                    if (c == "username" && v.kind == SqlLiteral::Kind::Quoted) {
                        username = v.text;
                    } else if (c == "password" && v.kind == SqlLiteral::Kind::Quoted) {
                        password = v.text;
                    } else if (c == "level" && v.kind == SqlLiteral::Kind::Number) {
                        level = static_cast<i32>(std::stoll(v.text));
                    } else if (c == "enabled" && v.kind == SqlLiteral::Kind::Bool) {
                        enabled = (v.text == "true");
                    }
                }
                if (username.has_value() && password.has_value() && level.has_value() && enabled.has_value()) {
                    db_->onSystemUsersPut(*username, *password, *level, *enabled);
                }
            }
        } else if (insert.table == "KEYSPACE_OWNERS") {
            for (const auto& row : insert.rows) {
                std::optional<std::string> ks;
                std::optional<std::string> owner;
                for (usize i = 0; i < insert.columns.size(); i++) {
                    const auto& c = insert.columns[i];
                    const auto& v = row[i];
                    if (c == "keyspace" && v.kind == SqlLiteral::Kind::Quoted) {
                        ks = v.text;
                    } else if (c == "owner_username" && v.kind == SqlLiteral::Kind::Quoted) {
                        owner = v.text;
                    }
                }
                if (ks.has_value() && owner.has_value()) {
                    db_->onSystemKeyspaceOwnersPut(*ks, *owner);
                }
            }
        } else if (insert.table == "KEYSPACE_GRANTS") {
            for (const auto& row : insert.rows) {
                for (usize i = 0; i < insert.columns.size(); i++) {
                    if (insert.columns[i] != "keyspace_username") {
                        continue;
                    }
                    const auto& v = row[i];
                    if (v.kind != SqlLiteral::Kind::Quoted) {
                        continue;
                    }
                    auto pos = v.text.find('#');
                    if (pos == std::string::npos || pos == 0 || pos + 1 >= v.text.size()) {
                        continue;
                    }
                    db_->onSystemKeyspaceGrantsPut(v.text.substr(0, pos), v.text.substr(pos + 1));
                }
            }
        } else if (insert.table == "KEYSPACE_QUOTAS") {
            for (const auto& row : insert.rows) {
                std::optional<std::string> ks;
                std::optional<i64> quotaBytes;
                for (usize i = 0; i < insert.columns.size(); i++) {
                    const auto& c = insert.columns[i];
                    const auto& v = row[i];
                    if (c == "keyspace" && v.kind == SqlLiteral::Kind::Quoted) {
                        ks = v.text;
                    } else if (c == "quota_bytes" && v.kind == SqlLiteral::Kind::Number) {
                        quotaBytes = std::stoll(v.text);
                    }
                }
                if (ks.has_value() && quotaBytes.has_value() && *quotaBytes > 0) {
                    db_->onSystemKeyspaceQuotasPut(*ks, static_cast<u64>(*quotaBytes));
                }
            }
        }
    }

    return jsonOk();
}

}
