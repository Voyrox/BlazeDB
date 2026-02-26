#include "net/serverTcp.h"

#include "net/detail/serverTcpInternal.h"

#include "query/sql.h"

#include "util/json.h"

namespace xeondb {

std::string ServerTcp::cmdCreateTable(const SqlCreateTable& createTable, const std::string& currentKeyspace, const AuthedUser& u) {
    using server_tcp_detail::schemaEquals;

    auto keyspace = createTable.keyspace.empty() ? currentKeyspace : createTable.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    if (auto quota = quotaBytesForKeyspace(keyspace); quota.has_value() && *quota > 0) {
        constexpr u64 estCreateTableBytes = 16ull * 1024ull;
        if (!quotaWouldAllowAndReserve(keyspace, *quota, estCreateTableBytes)) {
            throw runtimeError("quota_exceeded");
        }
    }

    if (!createTable.ifNotExists) {
        (void)db_->createTable(keyspace, createTable.table, createTable.schema);
    } else {
        try {
            (void)db_->createTable(keyspace, createTable.table, createTable.schema);
        } catch (const std::exception& e) {
            if (std::string(e.what()) == "Table exists") {
                auto t = db_->openTable(keyspace, createTable.table);
                if (!schemaEquals(t->schema(), createTable.schema)) {
                    throw runtimeError("Schema mismatch");
                }
            } else {
                throw;
            }
        }
    }

    return jsonOk();
}

std::string ServerTcp::cmdDropTable(const SqlDropTable& dropTable, const std::string& currentKeyspace, const AuthedUser& u) {
    auto keyspace = dropTable.keyspace.empty() ? currentKeyspace : dropTable.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    db_->dropTable(keyspace, dropTable.table, dropTable.ifExists);
    invalidateBytesUsedCache(keyspace);
    return jsonOk();
}

std::string ServerTcp::cmdShowTables(const SqlShowTables& showTables, const std::string& currentKeyspace, const AuthedUser& u) {
    std::string keyspace;
    if (showTables.inKeyspace.has_value()) {
        keyspace = *showTables.inKeyspace;
    } else {
        keyspace = currentKeyspace;
    }
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    auto tables = db_->listTables(keyspace);
    std::string out = "{\"ok\":true,\"tables\":[";
    for (usize i = 0; i < tables.size(); i++) {
        if (i) {
            out += ",";
        }
        out += "\"" + jsonEscape(tables[i]) + "\"";
    }
    out += "]}";
    return out;
}

std::string ServerTcp::cmdDescribeTable(const SqlDescribeTable& describe, const std::string& currentKeyspace, const AuthedUser& u) {
    auto keyspace = describe.keyspace.empty() ? currentKeyspace : describe.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    auto t = db_->openTable(keyspace, describe.table);
    const auto& schema = t->schema();
    std::string out = "{\"ok\":true,\"keyspace\":\"" + jsonEscape(keyspace) + "\",\"table\":\"" + jsonEscape(describe.table) + "\",";
    auto pkName = schema.columns[schema.primaryKeyIndex].name;
    out += "\"primaryKey\":\"" + jsonEscape(pkName) + "\",\"columns\":[";
    for (usize c = 0; c < schema.columns.size(); c++) {
        if (c) {
            out += ",";
        }
        out += "{\"name\":\"" + jsonEscape(schema.columns[c].name) + "\",\"type\":\"" + jsonEscape(columnTypeName(schema.columns[c].type)) + "\"}";
    }
    out += "]}";
    return out;
}

std::string ServerTcp::cmdShowCreateTable(const SqlShowCreateTable& showCreate, const std::string& currentKeyspace, const AuthedUser& u) {
    auto keyspace = showCreate.keyspace.empty() ? currentKeyspace : showCreate.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    auto t = db_->openTable(keyspace, showCreate.table);
    const auto& schema = t->schema();
    auto pkName = schema.columns[schema.primaryKeyIndex].name;
    std::string stmt = "CREATE TABLE " + keyspace + "." + showCreate.table + " (";
    for (usize c = 0; c < schema.columns.size(); c++) {
        if (c) {
            stmt += ", ";
        }
        stmt += schema.columns[c].name + " " + columnTypeName(schema.columns[c].type);
    }
    stmt += ", PRIMARY KEY (" + pkName + "));";
    return std::string("{\"ok\":true,\"create\":\"") + jsonEscape(stmt) + "\"}";
}

std::string ServerTcp::cmdTruncateTable(const SqlTruncateTable& trunc, const std::string& currentKeyspace, const AuthedUser& u) {
    auto keyspace = trunc.keyspace.empty() ? currentKeyspace : trunc.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }
    db_->truncateTable(keyspace, trunc.table);
    invalidateBytesUsedCache(keyspace);
    return jsonOk();
}

std::string ServerTcp::cmdFlush(const SqlFlush& flush, const std::string& currentKeyspace, const AuthedUser& u) {
    auto keyspace = flush.keyspace.empty() ? currentKeyspace : flush.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    if (auto quota = quotaBytesForKeyspace(keyspace); quota.has_value() && *quota > 0) {
        const u64 used = bytesUsedForKeyspaceCached(keyspace);
        if (used >= *quota) {
            throw runtimeError("quota_exceeded");
        }
    }

    auto retTable = db_->openTable(keyspace, flush.table);
    retTable->flush();
    invalidateBytesUsedCache(keyspace);
    return jsonOk();
}

}
