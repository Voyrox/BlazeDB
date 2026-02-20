#pragma once

#include <atomic>
#include <memory>
#include <string>

#include "core/db.h"
#include "prelude.h"

namespace xeondb {

class ServerTcp {
public:
    ServerTcp(std::shared_ptr<Db> db, std::string host, u16 port, usize maxLineBytes, usize maxConnections, std::string authUsername, std::string authPassword);
    ~ServerTcp();

    ServerTcp(const ServerTcp&) = delete;
    ServerTcp& operator=(const ServerTcp&) = delete;

    void run();

private:
    void handleClient(int clientFd);

    std::shared_ptr<Db> db_;
    std::string host_;
    u16 port_;
    usize maxLineBytes_;
    usize maxConnections_;
    std::string authUsername_;
    std::string authPassword_;
    bool authEnabled_;
    std::atomic<usize> connectionCount_;
};

}
