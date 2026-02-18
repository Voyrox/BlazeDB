#pragma once

#include "prelude.h"

#include <optional>
#include <utility>

namespace blazeDb
{

    struct SsEntry
    {
        byteVec key;
        u64 seq;
        byteVec value;
    };

    struct SsIndexEntry
    {
        byteVec key;
        u64 offset;
    };

    struct SsTableFile
    {
        path filePath;
        std::vector<SsIndexEntry> index;
    };

    void writeSsTable(const path &path, const std::vector<SsEntry> &entries, usize indexStride);
    SsTableFile loadSsTableIndex(const path &path);
    std::optional<byteVec> ssTableGet(const SsTableFile &file, const byteVec &key);

}
