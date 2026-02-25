#include "net/serverTcp.h"

#include "net/detail/serverTcpInternal.h"

#include "util/json.h"
#include "util/log.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <thread>

namespace xeondb {

ServerTcp::ServerTcp(
        std::shared_ptr<Db> db, std::string host, u16 port, usize maxLineBytes, usize maxConnections, std::string authUsername, std::string authPassword)
    : db_(std::move(db))
    , host_(std::move(host))
    , port_(port)
    , maxLineBytes_(maxLineBytes)
    , maxConnections_(maxConnections)
    , authUsername_(std::move(authUsername))
    , authPassword_(std::move(authPassword))
    , authEnabled_(db_ != nullptr ? db_->authEnabled() : (!authUsername_.empty() && !authPassword_.empty()))
    , connectionCount_(0) {
}

ServerTcp::~ServerTcp() = default;

void ServerTcp::run() {
    using server_tcp_detail::errnoError;
    using server_tcp_detail::sendAll;

    int socketFileDesc = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socketFileDesc < 0)
        throw errnoError("socket failed");
    int addrFlag = 1;
    ::setsockopt(socketFileDesc, SOL_SOCKET, SO_REUSEADDR, &addrFlag, sizeof(addrFlag));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) {
        ::close(socketFileDesc);
        throw runtimeError("bad host");
    }

    if (::bind(socketFileDesc, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(socketFileDesc);
        throw errnoError("bind failed");
    }
    if (::listen(socketFileDesc, 128) != 0) {
        ::close(socketFileDesc);
        throw errnoError("listen failed");
    }

    xeondb::log(xeondb::LogLevel::INFO, std::string("Listening host=") + host_ + " port=" + std::to_string(port_) +
                                                " maxLineBytes=" + std::to_string(maxLineBytes_) + " maxConnections=" + std::to_string(maxConnections_) +
                                                " auth=" + (authEnabled_ ? "enabled" : "disabled"));

    if (db_ != nullptr) {
        auto db = db_;
        std::thread sampler([db]() {
            using namespace std::chrono_literals;
            for (;;) {
                std::this_thread::sleep_for(30s);
                try {
                    db->metricsSampleAll();
                } catch (...) {
                    // ignore
                }
            }
        });
        sampler.detach();
    }

    for (;;) {
        int clientSocketDesc = ::accept(socketFileDesc, nullptr, nullptr);
        if (clientSocketDesc < 0) {
            if (errno == EINTR)
                continue;
            continue;
        }
        if (connectionCount_.load() >= maxConnections_) {
            sendAll(clientSocketDesc, jsonError("too_many_connections") + "\n");
            ::close(clientSocketDesc);
            continue;
        }
        connectionCount_++;
        std::thread t([this, clientSocketDesc]() {
            handleClient(clientSocketDesc);
            ::close(clientSocketDesc);
            connectionCount_--;
        });
        t.detach();
    }
}

}
