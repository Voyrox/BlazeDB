#pragma once

#include "prelude.h"

#include <string>

namespace blazeDb
{

    struct Settings
    {
        std::string host;
        u16 port;
        std::string dataDir;
        usize maxLineBytes;
        usize maxConnections;
        std::string walFsync;
        u64 walFsyncIntervalMs;
        usize walFsyncBytes;
        usize memtableMaxBytes;
        usize sstableIndexStride;
    };

    Settings loadSettings(const std::string &filePath);
    std::string resolveDataDir(const std::string &preferredDataDir);

}
