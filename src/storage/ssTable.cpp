#include "storage/ssTable.h"

#include "util/binIo.h"

#include <algorithm>
#include <fstream>

namespace blazeDb
{

static constexpr const char* ssMagic = "BZST001";
static constexpr const char* ixMagic = "BZIX001";
static constexpr const char* endMagic = "BZEND001";
static constexpr u32 ssVersion = 1;

static bool bytesLess(const byteVec& a, const byteVec& b)
{
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

void writeSsTable(const path& path, const std::vector<SsEntry>& entries, usize indexStride)
{
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out.is_open())
        throw runtimeError("cannot write sstable");

    out.write(ssMagic, 7);
    char pad = 0;
    out.write(&pad, 1);
    writeU32(out, ssVersion);
    writeU64(out, static_cast<u64>(entries.size()));

    if (indexStride == 0)
        indexStride = 16;

    std::vector<SsIndexEntry> indexEntry;
    indexEntry.reserve((entries.size() / indexStride) + 2);

    for (usize i = 0; i < entries.size(); i++)
    {
        u64 offset = static_cast<u64>(out.tellp());
        const auto& e = entries[i];
        if (i % indexStride == 0)
            indexEntry.push_back(SsIndexEntry{e.key, offset});
        writeBytes(out, e.key);
        writeU64(out, e.seq);
        writeBytes(out, e.value);
    }

    u64 indexStart = static_cast<u64>(out.tellp());
    out.write(ixMagic, 7);
    out.write(&pad, 1);
    writeU64(out, static_cast<u64>(indexEntry.size()));
    for (const auto& it : indexEntry)
    {
        writeBytes(out, it.key);
        writeU64(out, it.offset);
    }

    out.write(endMagic, 7);
    out.write(&pad, 1);
    writeU64(out, indexStart);
    out.flush();
    out.close();
}

SsTableFile loadSsTableIndex(const path& path)
{
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open())
        throw runtimeError("cannot open sstable");

    in.seekg(0, std::ios::end);
    auto size = static_cast<u64>(in.tellg());
    if (size < 16)
        throw runtimeError("sstable too small");
    in.seekg(static_cast<i64>(size - 16));
    char end[8]{};
    in.read(end, 8);
    if (std::string(end, 7) != std::string(endMagic, 7))
        throw runtimeError("bad sstable footer");
    u64 indexStart = readU64(in);

    in.seekg(static_cast<i64>(indexStart));
    char ix[8]{};
    in.read(ix, 8);
    if (std::string(ix, 7) != std::string(ixMagic, 7))
        throw runtimeError("bad index");
    u64 count = readU64(in);

    SsTableFile tableFile;
    tableFile.filePath = path;
    tableFile.index.reserve(static_cast<usize>(count));
    for (u64 i = 0; i < count; i++)
    {
        byteVec k = readBytes(in);
        u64 off = readU64(in);
        tableFile.index.push_back(SsIndexEntry{std::move(k), off});
    }
    return tableFile;
}

static std::optional<usize> findIndexFloor(const std::vector<SsIndexEntry>& index, const byteVec& key)
{
    if (index.empty())
        return std::nullopt;

    auto upper = std::upper_bound(index.begin(), index.end(), key, [](const byteVec& k, const SsIndexEntry& e) {
        return bytesLess(k, e.key);
    });
    if (upper == index.begin())
        return 0;
    return static_cast<usize>((upper - index.begin()) - 1);
}

std::optional<byteVec> ssTableGet(const SsTableFile& file, const byteVec& key)
{
    std::ifstream fileStream(file.filePath, std::ios::binary);
    if (!fileStream.is_open())
        return std::nullopt;

    auto floor = findIndexFloor(file.index, key);
    if (floor.has_value())
        fileStream.seekg(static_cast<i64>(file.index[*floor].offset));
    else
        fileStream.seekg(0);

    if (fileStream.tellg() == 0)
    {
        char header[8]{};
        fileStream.read(header, 8);
        if (std::string(header, 7) != std::string(ssMagic, 7))
            return std::nullopt;
        u32 ver = readU32(fileStream);
        if (ver != ssVersion)
            return std::nullopt;
        (void)readU64(fileStream);
    }

    while (fileStream)
    {
        byteVec entryKey = readBytes(fileStream);
        if (!fileStream)
            break;
        u64 seq = readU64(fileStream);
        (void)seq;
        byteVec value = readBytes(fileStream);
        if (!fileStream)
            break;
        if (entryKey == key)
            return value;
        if (bytesLess(key, entryKey))
            return std::nullopt;
    }
    return std::nullopt;
}

}
