#include "net/serverTcp.h"

#include "query/sql.h"

#include "util/json.h"

namespace xeondb {

std::string ServerTcp::cmdAuth(const SqlAuth& auth, std::optional<AuthedUser>& currentUser) {
    if (!authEnabled_) {
        return jsonOk();
    }

    auto u = db_->authenticate(auth.username, auth.password);
    if (!u.has_value()) {
        return jsonError("bad_auth");
    }

    currentUser = *u;
    return jsonOk();
}

}
