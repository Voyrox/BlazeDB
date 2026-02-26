#include "net/serverTcp.h"

#include "query/sql.h"

#include "util/json.h"

namespace xeondb {

std::string ServerTcp::cmdUse(const SqlUse& use, std::string& currentKeyspace, const AuthedUser& u) {
    if (authEnabled_ && !db_->canAccessKeyspace(u, use.keyspace)) {
        throw runtimeError("forbidden");
    }

    if (db_ != nullptr) {
        db_->metricsOnUse(currentKeyspace, use.keyspace);
    }

    currentKeyspace = use.keyspace;
    return jsonOk();
}

}
