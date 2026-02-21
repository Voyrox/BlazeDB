#include "core/db.h"

#include "core/paths.h"
#include "util/uuid.h"

#include <filesystem>
#include <algorithm>
#include <set>
#include <cctype>

using std::string;
using std::shared_ptr;
using std::filesystem::path;

namespace xeondb {

static string tableKey(const string& keyspace, const string& table) {
    return keyspace + "." + table;
}

Db::Db(Settings settings)
    : settings_(std::move(settings)) {
    settings_.dataDir = resolveDataDir(settings_.dataDir);
    effectiveDataDir_ = settings_.dataDir;
    std::filesystem::create_directories(effectiveDataDir_);
}

const path& Db::dataDir() const {
    return effectiveDataDir_;
}

const Settings& Db::settings() const {
    return settings_;
}

void Db::createKeyspace(const string& keyspace) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::filesystem::create_directories(keyspaceDir(effectiveDataDir_, keyspace));
}

path Db::createTable(const string& keyspace, const string& table, const TableSchema& schema) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::filesystem::create_directories(keyspaceDir(effectiveDataDir_, keyspace));
    auto schemaFile = schemaPath(effectiveDataDir_, keyspace);
    auto existing = findTableUuidFromSchema(schemaFile, table);
    if (existing.has_value()) {
        throw runtimeError("Table exists");
    }
    auto uuid = newUuidHex();
    upsertTableUuidToSchema(schemaFile, table, uuid);
    auto dirPath = tableDir(effectiveDataDir_, keyspace, table, uuid);
    std::filesystem::create_directories(dirPath / "tmp");

    TableSettings ts;
    ts.walFsync = settings_.walFsync;
    ts.walFsyncIntervalMs = settings_.walFsyncIntervalMs;
    ts.walFsyncBytes = settings_.walFsyncBytes;
    ts.memtableMaxBytes = settings_.memtableMaxBytes;
    ts.sstableIndexStride = settings_.sstableIndexStride;
    auto t = std::make_shared<Table>(dirPath, keyspace, table, uuid, schema, ts);
    t->openOrCreateFiles(true);
    t->recover();
    tables_[tableKey(keyspace, table)] = t;
    return dirPath;
}

shared_ptr<Table> Db::openTable(const string& keyspace, const string& table) {
    std::lock_guard<std::mutex> lock(mutex_);
    return openTableUnlocked(keyspace, table);
}

shared_ptr<Table> Db::openTableUnlocked(const string& keyspace, const string& table) {
    auto key = tableKey(keyspace, table);
    auto data = tables_.find(key);
    if (data != tables_.end()) {
        return data->second;
    }

    auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
    std::filesystem::create_directories(ksDir);
    auto schemaFile = schemaPath(effectiveDataDir_, keyspace);

    auto uuidOpt = findTableUuidFromSchema(schemaFile, table);
    if (!uuidOpt.has_value()) {
        uuidOpt = findTableUuidByScan(ksDir, table);
        if (uuidOpt.has_value()) {
            upsertTableUuidToSchema(schemaFile, table, *uuidOpt);
        }
    }
    if (!uuidOpt.has_value()) {
        throw runtimeError("Table not found");
    }

    auto dirPath = tableDir(effectiveDataDir_, keyspace, table, *uuidOpt);
    auto schema = readSchemaFromMetadata(dirPath);
    TableSettings ts;
    ts.walFsync = settings_.walFsync;
    ts.walFsyncIntervalMs = settings_.walFsyncIntervalMs;
    ts.walFsyncBytes = settings_.walFsyncBytes;
    ts.memtableMaxBytes = settings_.memtableMaxBytes;
    ts.sstableIndexStride = settings_.sstableIndexStride;
    auto tablePtr = std::make_shared<Table>(dirPath, keyspace, table, *uuidOpt, schema, ts);
    tablePtr->openOrCreateFiles(false);
    tablePtr->recover();
    tables_[key] = tablePtr;
    return tablePtr;
}

static bool isValidIdent(const string& s) {
    if (s.empty())
        return false;
    auto isIdentChar = [](char c) {
        return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
    };
    if (!isIdentChar(s[0]) || std::isdigit(static_cast<unsigned char>(s[0])))
        return false;
    for (char c : s) {
        if (!isIdentChar(c))
            return false;
    }
    return true;
}

std::vector<string> Db::listKeyspaces() const {
    std::vector<string> out;
    std::error_code ec;
    if (!std::filesystem::exists(effectiveDataDir_, ec))
        return out;

    for (const auto& entry : std::filesystem::directory_iterator(effectiveDataDir_, ec)) {
        if (ec)
            break;
        if (!entry.is_directory())
            continue;
        auto name = entry.path().filename().string();
        if (!isValidIdent(name))
            continue;
        out.push_back(name);
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<string> Db::listTables(const string& keyspace) const {
    std::vector<string> out;
    if (!isValidIdent(keyspace))
        return out;
    std::error_code ec;
    auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
    if (!std::filesystem::exists(ksDir, ec))
        return out;

    std::set<string> uniq;
    for (const auto& entry : std::filesystem::directory_iterator(ksDir, ec)) {
        if (ec)
            break;
        if (!entry.is_directory())
            continue;
        auto name = entry.path().filename().string();
        auto pos = name.rfind('-');
        if (pos == string::npos || pos == 0)
            continue;
        auto table = name.substr(0, pos);
        if (!isValidIdent(table))
            continue;
        uniq.insert(table);
    }

    out.assign(uniq.begin(), uniq.end());
    return out;
}

void Db::dropTable(const string& keyspace, const string& table, bool ifExists) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto schemaFile = schemaPath(effectiveDataDir_, keyspace);
    auto uuidOpt = findTableUuidFromSchema(schemaFile, table);
    if (!uuidOpt.has_value()) {
        auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
        uuidOpt = findTableUuidByScan(ksDir, table);
    }
    if (!uuidOpt.has_value()) {
        if (ifExists)
            return;
        throw runtimeError("Table not found");
    }

    auto key = tableKey(keyspace, table);
    auto it = tables_.find(key);
    if (it != tables_.end()) {
        it->second->shutdown();
        tables_.erase(it);
    }

    (void)removeTableFromSchema(schemaFile, table);

    std::error_code ec;
    auto dirPath = tableDir(effectiveDataDir_, keyspace, table, *uuidOpt);
    std::filesystem::remove_all(dirPath, ec);
    if (ec && !ifExists) {
        throw runtimeError("drop failed");
    }
}

void Db::dropKeyspace(const string& keyspace, bool ifExists) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::error_code ec;
    auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
    if (!std::filesystem::exists(ksDir, ec)) {
        if (ifExists)
            return;
        throw runtimeError("Keyspace not found");
    }

    auto prefix = keyspace + ".";
    std::vector<string> toErase;
    toErase.reserve(tables_.size());
    for (auto& kv : tables_) {
        if (kv.first.rfind(prefix, 0) == 0) {
            kv.second->shutdown();
            toErase.push_back(kv.first);
        }
    }
    for (const auto& k : toErase) {
        tables_.erase(k);
    }

    std::filesystem::remove_all(ksDir, ec);
    if (ec && !ifExists) {
        throw runtimeError("drop failed");
    }
}

void Db::truncateTable(const string& keyspace, const string& table) {
    auto t = openTable(keyspace, table);
    t->truncate();
}

}
