#pragma once

#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "storage/table.h"

using std::filesystem::path;
using std::shared_ptr;
using std::string;

namespace xeondb {

class Db {
public:
    explicit Db(Settings settings);

    const path& dataDir() const;
    const Settings& settings() const;

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

    Settings settings_;
    path effectiveDataDir_;

    std::mutex mutex_;
    std::unordered_map<string, shared_ptr<Table>> tables_;
};

}
