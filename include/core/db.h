#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
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
