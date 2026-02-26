#include "net/serverTcp.h"

#include "net/detail/serverTcpInternal.h"

#include "query/sql.h"

#include "util/json.h"

namespace xeondb {

std::string ServerTcp::cmdUpdate(const SqlUpdate& upd, const std::string& currentKeyspace, const AuthedUser& u) {
    using server_tcp_detail::isSystemKeyspaceName;

    auto keyspace = upd.keyspace.empty() ? currentKeyspace : upd.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    auto retTable = db_->openTable(keyspace, upd.table);
    auto pkIndex = retTable->schema().primaryKeyIndex;
    auto pkName = retTable->schema().columns[pkIndex].name;
    if (upd.whereColumn != pkName) {
        throw runtimeError("Where must use primary key");
    }

    if (upd.setColumns.size() != upd.setValues.size()) {
        throw runtimeError("set column/value count");
    }

    for (const auto& c : upd.setColumns) {
        if (c == pkName) {
            throw runtimeError("cannot update pk");
        }
    }

    byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, upd.whereValue);
    auto existing = retTable->getRow(pkBytes);
    byteVec newRowBytes = mergeRowBytesForUpdate(retTable->schema(), existing, upd.setColumns, upd.setValues);

    if (auto quota = quotaBytesForKeyspace(keyspace); quota.has_value() && *quota > 0) {
        u64 est = static_cast<u64>(pkBytes.size()) + static_cast<u64>(newRowBytes.size()) + 64;
        if (!quotaWouldAllowAndReserve(keyspace, *quota, est)) {
            throw runtimeError("quota_exceeded");
        }
    }
    retTable->putRow(pkBytes, newRowBytes);

    if (authEnabled_ && isSystemKeyspaceName(keyspace)) {
        if (upd.whereValue.kind == SqlLiteral::Kind::Quoted && upd.table == "USERS") {
            std::optional<std::string> password;
            std::optional<i32> level;
            std::optional<bool> enabled;
            for (usize i = 0; i < upd.setColumns.size(); i++) {
                const auto& c = upd.setColumns[i];
                const auto& v = upd.setValues[i];
                if (c == "password" && v.kind == SqlLiteral::Kind::Quoted) {
                    password = v.text;
                } else if (c == "level" && v.kind == SqlLiteral::Kind::Number) {
                    level = static_cast<i32>(std::stoll(v.text));
                } else if (c == "enabled" && v.kind == SqlLiteral::Kind::Bool) {
                    enabled = (v.text == "true");
                }
            }
            if (password.has_value() && level.has_value() && enabled.has_value()) {
                db_->onSystemUsersPut(upd.whereValue.text, *password, *level, *enabled);
            }
        } else if (upd.whereValue.kind == SqlLiteral::Kind::Quoted && upd.table == "KEYSPACE_OWNERS") {
            for (usize i = 0; i < upd.setColumns.size(); i++) {
                if (upd.setColumns[i] == "owner_username" && upd.setValues[i].kind == SqlLiteral::Kind::Quoted) {
                    db_->onSystemKeyspaceOwnersPut(upd.whereValue.text, upd.setValues[i].text);
                }
            }
        } else if (upd.whereValue.kind == SqlLiteral::Kind::Quoted && upd.table == "KEYSPACE_QUOTAS") {
            for (usize i = 0; i < upd.setColumns.size(); i++) {
                if (upd.setColumns[i] == "quota_bytes" && upd.setValues[i].kind == SqlLiteral::Kind::Number) {
                    i64 q = std::stoll(upd.setValues[i].text);
                    if (q > 0) {
                        db_->onSystemKeyspaceQuotasPut(upd.whereValue.text, static_cast<u64>(q));
                    } else {
                        db_->onSystemKeyspaceQuotasDelete(upd.whereValue.text);
                    }
                }
            }
        }
    }

    return jsonOk();
}

}
