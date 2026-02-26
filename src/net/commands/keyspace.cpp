#include "net/serverTcp.h"

#include "net/detail/serverTcpInternal.h"

#include "core/paths.h"

#include "query/sql.h"

#include "util/json.h"

#include <filesystem>
#include <system_error>

namespace xeondb {

std::string ServerTcp::cmdCreateKeyspace(const SqlCreateKeyspace& createKeyspace, const AuthedUser& u) {
    using server_tcp_detail::isSystemKeyspaceName;
    using server_tcp_detail::litNumber;
    using server_tcp_detail::litQuoted;

    if (authEnabled_ && !db_->canCreateOrDropKeyspace(u)) {
        throw runtimeError("forbidden");
    }
    if (isSystemKeyspaceName(createKeyspace.keyspace)) {
        throw runtimeError("cannot create SYSTEM");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(createKeyspace.keyspace);
    }

    std::error_code ec;
    const bool existed = std::filesystem::exists(keyspaceDir(db_->dataDir(), createKeyspace.keyspace), ec) && !ec;
    db_->createKeyspace(createKeyspace.keyspace);

    if (authEnabled_) {
        db_->onKeyspaceCreated(createKeyspace.keyspace);
        if (!existed) {
            auto ownersTable = db_->openTable("SYSTEM", "KEYSPACE_OWNERS");
            const i64 createdAt = server_tcp_detail::nowMs();
            auto ksLit = litQuoted(createKeyspace.keyspace);
            byteVec pkBytes = partitionKeyBytes(ColumnType::Text, ksLit);
            std::vector<std::string> cols = {"keyspace", "owner_username", "created_at"};
            std::vector<SqlLiteral> vals = {ksLit, litQuoted(u.username), litNumber(createdAt)};
            TableSchema schema;
            schema.columns = {{"keyspace", ColumnType::Text}, {"owner_username", ColumnType::Text}, {"created_at", ColumnType::Timestamp}};
            schema.primaryKeyIndex = 0;
            byteVec rb = rowBytes(schema, cols, vals, pkBytes);
            ownersTable->putRow(pkBytes, rb);
            db_->onSystemKeyspaceOwnersPut(createKeyspace.keyspace, u.username);
        }
    }

    return jsonOk();
}

std::string ServerTcp::cmdDropKeyspace(const SqlDropKeyspace& dropKeyspace, std::string& currentKeyspace, const AuthedUser& u) {
    using server_tcp_detail::isSystemKeyspaceName;

    if (authEnabled_ && !db_->canCreateOrDropKeyspace(u)) {
        throw runtimeError("forbidden");
    }
    if (isSystemKeyspaceName(dropKeyspace.keyspace)) {
        throw runtimeError("cannot drop SYSTEM");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(dropKeyspace.keyspace);
    }

    db_->dropKeyspace(dropKeyspace.keyspace, dropKeyspace.ifExists);
    invalidateBytesUsedCache(dropKeyspace.keyspace);
    if (authEnabled_) {
        db_->cleanupKeyspaceSecurityMetadata(dropKeyspace.keyspace);
        db_->onKeyspaceDropped(dropKeyspace.keyspace);
    }
    if (currentKeyspace == dropKeyspace.keyspace) {
        currentKeyspace.clear();
    }

    return jsonOk();
}

std::string ServerTcp::cmdShowKeyspaces(const std::optional<AuthedUser>& currentUser) {
    std::vector<std::string> keyspaces;
    if (authEnabled_) {
        const AuthedUser& u = *currentUser;
        keyspaces = db_->listKeyspacesForUser(u);
    } else {
        keyspaces = db_->listKeyspaces();
    }

    std::string out = "{\"ok\":true,\"keyspaces\":[";
    for (usize i = 0; i < keyspaces.size(); i++) {
        if (i) {
            out += ",";
        }
        out += "\"" + jsonEscape(keyspaces[i]) + "\"";
    }
    out += "]}";
    return out;
}

}
