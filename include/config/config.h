#pragma once

#include "prelude.h"

#include <string>

using std::string;

namespace xeondb {

struct Settings {
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
    string authUsername;
    string authPassword;
};

Settings loadSettings(const string& filePath);
string resolveDataDir(const string& preferredDataDir);

}
