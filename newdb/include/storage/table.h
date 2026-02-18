#pragma once

#include <atomic>
#include "prelude.h"

#include <filesystem>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "storage/commitLog.h"
#include "storage/memTable.h"
#include "storage/manifest.h"
#include "util/murmur3.h"
#include "query/schema.h"
#include "storage/ssTable.h"

using std::string;
using std::vector;

namespace blazeDb
{

    TableSchema readSchemaFromMetadata(const path& tableDirPath);

struct TableSettings
{
    string walFsync;
    u64 walFsyncIntervalMs;
    usize walFsyncBytes;
    usize memtableMaxBytes;
    usize sstableIndexStride;
};

class Table
{
public:
    Table(path tableDirPath, string keyspace, string table, string uuid, TableSchema schema, TableSettings settings);
        ~Table();

        Table(const Table &) = delete;
        Table &operator=(const Table &) = delete;

    const path &dir() const;
        const string &keyspace() const;
        const string &table() const;
        const string &uuid() const;
        const TableSchema &schema() const;

        void openOrCreateFiles(bool createNew);
        void recover();

    void putRow(const byteVec &pkBytes, const byteVec &rowBytes);
    std::optional<byteVec> getRow(const byteVec &pkBytes);
        void flush();

    private:
        void startWalThread();
        void stopWalThread();
        void walThreadMain();

        void writeMetadata();
        void loadMetadata();

    path tableDirPath_;
        string keyspace_;
        string table_;
        string uuid_;
        TableSchema schema_;
        TableSettings settings_;

        mutable std::mutex mutex_;
    u64 nextSeq_;

        CommitLog commitLog_;
        MemTable memTable_;
        Manifest manifest_;
    std::vector<SsTableFile> ssTables_;

        std::atomic<bool> walStop_;
        std::thread walThread_;
    };

}
