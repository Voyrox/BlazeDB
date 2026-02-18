#include "core/db.h"

#include "core/paths.h"
#include "util/uuid.h"

#include <filesystem>

using std::string ;
using std::shared_ptr;
using std::filesystem::path;

namespace blazeDb
{

    static string tableKey(const string &keyspace, const string &table)
    {
        return keyspace + "." + table;
    }

    Db::Db(Settings settings) : settings_(std::move(settings))
    {
        settings_.dataDir = resolveDataDir(settings_.dataDir);
        effectiveDataDir_ = settings_.dataDir;
        std::filesystem::create_directories(effectiveDataDir_);
    }

    const path &Db::dataDir() const
    {
        return effectiveDataDir_;
    }

    const Settings &Db::settings() const
    {
        return settings_;
    }

    void Db::createKeyspace(const string &keyspace)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::filesystem::create_directories(keyspaceDir(effectiveDataDir_, keyspace));
    }

    path Db::createTable(const string &keyspace, const string &table, const TableSchema &schema)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::filesystem::create_directories(keyspaceDir(effectiveDataDir_, keyspace));
        auto schemaFile = schemaPath(effectiveDataDir_, keyspace);
        auto existing = findTableUuidFromSchema(schemaFile, table);
        if (existing.has_value())
        {
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

    shared_ptr<Table> Db::openTable(const string &keyspace, const string &table)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return openTableUnlocked(keyspace, table);
    }

    shared_ptr<Table> Db::openTableUnlocked(const string &keyspace, const string &table)
    {
        auto key = tableKey(keyspace, table);
        auto data = tables_.find(key);
        if (data != tables_.end())
        {
            return data->second;
        }

        auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
        std::filesystem::create_directories(ksDir);
        auto schemaFile = schemaPath(effectiveDataDir_, keyspace);

        auto uuidOpt = findTableUuidFromSchema(schemaFile, table);
        if (!uuidOpt.has_value())
        {
            uuidOpt = findTableUuidByScan(ksDir, table);
            if (uuidOpt.has_value())
            {
                upsertTableUuidToSchema(schemaFile, table, *uuidOpt);
            }
        }
        if (!uuidOpt.has_value())
        {
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

}
