#include "net/serverTcp.h"

#include "net/detail/serverTcpInternal.h"

#include "query/sql.h"

#include "util/json.h"

namespace xeondb {

std::string ServerTcp::cmdDelete(const SqlDelete& del, const std::string& currentKeyspace, const AuthedUser& u) {
    using server_tcp_detail::isSystemKeyspaceName;

    auto keyspace = del.keyspace.empty() ? currentKeyspace : del.keyspace;
    if (keyspace.empty()) {
        throw runtimeError("No keyspace selected");
    }
    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnCommand(keyspace);
    }

    auto retTable = db_->openTable(keyspace, del.table);
    auto pkIndex = retTable->schema().primaryKeyIndex;
    auto pkName = retTable->schema().columns[pkIndex].name;
    if (del.whereColumn != pkName) {
        throw runtimeError("Where must use primary key");
    }
    byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, del.whereValue);
    retTable->deleteRow(pkBytes);

    if (authEnabled_ && isSystemKeyspaceName(keyspace)) {
        if (del.table == "USERS" && del.whereValue.kind == SqlLiteral::Kind::Quoted) {
            db_->onSystemUsersDelete(del.whereValue.text);
        } else if (del.table == "KEYSPACE_OWNERS" && del.whereValue.kind == SqlLiteral::Kind::Quoted) {
            db_->onSystemKeyspaceOwnersDelete(del.whereValue.text);
        } else if (del.table == "KEYSPACE_GRANTS" && del.whereValue.kind == SqlLiteral::Kind::Quoted) {
            auto pos = del.whereValue.text.find('#');
            if (pos != std::string::npos) {
                db_->onSystemKeyspaceGrantsDelete(del.whereValue.text.substr(0, pos), del.whereValue.text.substr(pos + 1));
            }
        } else if (del.table == "KEYSPACE_QUOTAS" && del.whereValue.kind == SqlLiteral::Kind::Quoted) {
            db_->onSystemKeyspaceQuotasDelete(del.whereValue.text);
        }
    }

    return jsonOk();
}

}
