#include "net/serverTcp.h"

#include "util/json.h"

namespace xeondb {

std::string ServerTcp::cmdPing() {
    return jsonString("result", "PONG");
}

}
