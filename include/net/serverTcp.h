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

struct SqlAuth;
struct SqlUse;
struct SqlCreateKeyspace;
struct SqlCreateTable;
struct SqlInsert;
struct SqlSelect;
struct SqlFlush;
struct SqlDropTable;
struct SqlDropKeyspace;
struct SqlShowKeyspaces;
struct SqlShowTables;
struct SqlDescribeTable;
struct SqlShowCreateTable;
struct SqlShowMetrics;
struct SqlTruncateTable;
struct SqlDelete;
struct SqlUpdate;

class ServerTcp {
public:
    ServerTcp(std::shared_ptr<Db> db, std::string host, u16 port, usize maxLineBytes, usize maxConnections, std::string authUsername, std::string authPassword);
    ~ServerTcp();

    ServerTcp(const ServerTcp&) = delete;
    ServerTcp& operator=(const ServerTcp&) = delete;

    void run();

private:
    void handleClient(int clientFd);

    std::string cmdAuth(const SqlAuth& auth, std::optional<AuthedUser>& currentUser);
    std::string cmdPing();
    std::string cmdUse(const SqlUse& use, std::string& currentKeyspace, const AuthedUser& u);

    std::string cmdCreateKeyspace(const SqlCreateKeyspace& createKeyspace, const AuthedUser& u);
    std::string cmdDropKeyspace(const SqlDropKeyspace& dropKeyspace, std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdShowKeyspaces(const std::optional<AuthedUser>& currentUser);

    std::string cmdCreateTable(const SqlCreateTable& createTable, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdDropTable(const SqlDropTable& dropTable, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdShowTables(const SqlShowTables& showTables, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdDescribeTable(const SqlDescribeTable& describe, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdShowCreateTable(const SqlShowCreateTable& showCreate, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdShowMetrics(const SqlShowMetrics& showMetrics, const AuthedUser& u);
    std::string cmdTruncateTable(const SqlTruncateTable& trunc, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdInsert(const SqlInsert& insert, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdFlush(const SqlFlush& flush, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdDelete(const SqlDelete& del, const std::string& currentKeyspace, const AuthedUser& u);
    std::string cmdUpdate(const SqlUpdate& upd, const std::string& currentKeyspace, const AuthedUser& u);

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
