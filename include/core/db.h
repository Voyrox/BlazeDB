#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <array>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/config.h"
#include "storage/table.h"

using std::filesystem::path;
using std::shared_ptr;
using std::string;

namespace xeondb {

struct AuthedUser {
    string username;
    i32 level;
};

class Db {
public:
    explicit Db(Settings settings);

    const path& dataDir() const;
    const Settings& settings() const;

    bool authEnabled() const;
    void bootstrapAuthSystem();
    std::optional<AuthedUser> authenticate(const string& username, const string& password) const;

    bool canCreateOrDropKeyspace(const AuthedUser& user) const;
    bool canAccessKeyspace(const AuthedUser& user, const string& keyspace) const;
    std::vector<string> listKeyspacesForUser(const AuthedUser& user) const;

    void onKeyspaceCreated(const string& keyspace);
    void onKeyspaceDropped(const string& keyspace);
    void cleanupKeyspaceSecurityMetadata(const string& keyspace);

    void onSystemUsersPut(const string& username, const string& password, i32 level, bool enabled);
    void onSystemUsersDelete(const string& username);
    void onSystemKeyspaceOwnersPut(const string& keyspace, const string& ownerUsername);
    void onSystemKeyspaceOwnersDelete(const string& keyspace);
    void onSystemKeyspaceGrantsPut(const string& keyspace, const string& username);
    void onSystemKeyspaceGrantsDelete(const string& keyspace, const string& username);

    void createKeyspace(const string& keyspace);
    path createTable(const string& keyspace, const string& table, const TableSchema& schema);

    shared_ptr<Table> openTable(const string& keyspace, const string& table);

    std::vector<string> listKeyspaces() const;
    std::vector<string> listTables(const string& keyspace) const;

    void dropTable(const string& keyspace, const string& table, bool ifExists);
    void dropKeyspace(const string& keyspace, bool ifExists);
    void truncateTable(const string& keyspace, const string& table);

    struct KeyspaceMetrics {
        i64 connectionsActive = 0;
        std::array<i64, 6> connectionsLast24hPeak4h{};
        std::array<i64, 6> queriesLast24h4h{};
        i64 queriesLast24hTotal = 0;
        std::array<string, 6> labelsLast24h4h{};
    };

    void metricsOnUse(const string& oldKeyspace, const string& newKeyspace);
    void metricsOnDisconnect(const string& keyspace);
    void metricsOnCommand(const string& keyspace);
    void metricsSampleAll();
    KeyspaceMetrics keyspaceMetrics(const string& keyspace) const;

private:
    shared_ptr<Table> openTableUnlocked(const string& keyspace, const string& table);

    static bool isSystemKeyspace(const string& keyspace);
    static string grantKey(const string& keyspace, const string& username);
    void keyspacesInsertSortedUnlocked(const string& keyspace);
    void keyspacesEraseUnlocked(const string& keyspace);

    Settings settings_;
    path effectiveDataDir_;

    std::mutex mutex_;
    std::unordered_map<string, shared_ptr<Table>> tables_;

    struct MetricsSeries {
        static constexpr i64 bucketMs = 5 * 60 * 1000;
        static constexpr usize bucketCount = 288;

        i64 connectionsActive = 0;
        std::array<u64, bucketCount> bucketId{};
        std::array<i64, bucketCount> connPeak{};
        std::array<i64, bucketCount> queries{};
    };

    void metricsTouchBucketLocked(MetricsSeries& m, u64 absBucket);
    void metricsObserveConnPeakLocked(MetricsSeries& m, u64 absBucket);
    KeyspaceMetrics keyspaceMetricsLocked(const string& keyspace, u64 nowBucket) const;

    mutable std::mutex metricsMutex_;
    std::unordered_map<string, MetricsSeries> metricsByKeyspace_;

    mutable std::shared_mutex authMutex_;
    bool authBootstrapped_ = false;
    std::unordered_map<string, string> usersPassword_;
    std::unordered_map<string, i32> usersLevel_;
    std::unordered_map<string, bool> usersEnabled_;
    std::unordered_map<string, string> keyspaceOwner_;
    std::unordered_set<string> keyspaceGrants_;
    std::vector<string> keyspacesCache_;
};

}
