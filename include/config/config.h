#pragma once

#include "prelude.h"

#include <string>

using std::string;

namespace blazeDb
{

    struct Settings
    {
        string host;
        u16 port;
        string dataDir;
        usize maxLineBytes;
        usize maxConnections;
        string walFsync;
        u64 walFsyncIntervalMs;
        usize walFsyncBytes;
        usize memtableMaxBytes;
        usize sstableIndexStride;
    };

    Settings loadSettings(const string &filePath);
    string resolveDataDir(const string &preferredDataDir);

}
