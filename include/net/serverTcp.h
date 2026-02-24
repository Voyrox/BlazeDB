#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <optional>

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

    std::optional<u64> quotaBytesForKeyspace(const std::string& keyspace) const;
    bool quotaWouldAllowAndReserve(const std::string& keyspace, u64 quotaBytes, u64 estimatedWriteBytes);
    u64 bytesUsedForKeyspaceCached(const std::string& keyspace);
    void invalidateBytesUsedCache(const std::string& keyspace);

    std::shared_ptr<Db> db_;
    std::string host_;
    u16 port_;
    usize maxLineBytes_;
    usize maxConnections_;
    std::string authUsername_;
    std::string authPassword_;
    bool authEnabled_;
    std::atomic<usize> connectionCount_;

    struct BytesUsedCacheEntry {
        u64 bytesUsed = 0;
        i64 computedAtMs = 0;
    };
    mutable std::mutex bytesUsedCacheMutex_;
    std::unordered_map<std::string, BytesUsedCacheEntry> bytesUsedCache_;
};

}
