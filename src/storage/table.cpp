#include "storage/table.h"

#include "util/crc32.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <cstdio>
#include <fstream>
#include <unordered_map>

using std::ifstream;
using std::ofstream;
using std::string;

namespace xeondb {

static constexpr const char* metaMagic = "BZMD002";
static constexpr u32 metaVersion = 2;

static void metaWriteU32(ofstream& out, u32 v) {
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

static void metaWriteU64(ofstream& out, u64 v) {
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

static u32 metaReadU32(ifstream& in) {
    u32 value = 0;
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

static u64 metaReadU64(ifstream& in) {
    u64 value = 0;
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

static void metaWriteString(ofstream& out, const string& s) {
    metaWriteU32(out, static_cast<u32>(s.size()));
    out.write(s.data(), s.size());
}

static string metaReadString(ifstream& stream) {
    auto len = metaReadU32(stream);
    string s;
    s.resize(len);
    stream.read(s.data(), len);
    return s;
}

static path metadataPath(const path& dir) {
    return dir / "metadata.bin";
}

static path manifestPath(const path& dir) {
    return dir / "manifest.bin";
}

static path commitLogPath(const path& dir) {
    return dir / "commitlog.bin";
}

static byteVec decoratedKeyBytes(const byteVec& pkBytes) {
    i64 token = murmur3Token(pkBytes);
    u64 flipped = static_cast<u64>(token) ^ 0x8000000000000000ULL;
    byteVec out;
    out.reserve(8 + pkBytes.size());
    out.push_back(static_cast<u8>((flipped >> 56) & 0xFF));
    out.push_back(static_cast<u8>((flipped >> 48) & 0xFF));
    out.push_back(static_cast<u8>((flipped >> 40) & 0xFF));
    out.push_back(static_cast<u8>((flipped >> 32) & 0xFF));
    out.push_back(static_cast<u8>((flipped >> 24) & 0xFF));
    out.push_back(static_cast<u8>((flipped >> 16) & 0xFF));
    out.push_back(static_cast<u8>((flipped >> 8) & 0xFF));
    out.push_back(static_cast<u8>((flipped >> 0) & 0xFF));
    out.insert(out.end(), pkBytes.begin(), pkBytes.end());
    return out;
}

static byteVec pkBytesFromDecoratedKeyString(const string& decorated) {
    if (decorated.size() < 8)
        return {};
    const u8* p = reinterpret_cast<const u8*>(decorated.data());
    return byteVec(p + 8, p + decorated.size());
}

static int comparePkBytes(ColumnType type, const byteVec& a, const byteVec& b) {
    auto readBe32Local = [](const byteVec& v) -> i32 {
        u32 val = 0;
        val |= static_cast<u32>(v[0]) << 24;
        val |= static_cast<u32>(v[1]) << 16;
        val |= static_cast<u32>(v[2]) << 8;
        val |= static_cast<u32>(v[3]) << 0;
        return static_cast<i32>(val);
    };
    auto readBe64Local = [](const byteVec& v) -> i64 {
        u64 val = 0;
        for (int i = 0; i < 8; i++)
            val = (val << 8) | static_cast<u64>(v[static_cast<usize>(i)]);
        return static_cast<i64>(val);
    };

    if (type == ColumnType::Text || type == ColumnType::Char || type == ColumnType::Blob) {
        if (std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()))
            return -1;
        if (std::lexicographical_compare(b.begin(), b.end(), a.begin(), a.end()))
            return 1;
        return 0;
    }

    if (type == ColumnType::Boolean) {
        u8 av = a.empty() ? 0 : a[0];
        u8 bv = b.empty() ? 0 : b[0];
        if (av < bv)
            return -1;
        if (av > bv)
            return 1;
        return 0;
    }

    if (type == ColumnType::Int32 || type == ColumnType::Date) {
        if (a.size() != 4 || b.size() != 4)
            return (a.size() < b.size()) ? -1 : (a.size() > b.size() ? 1 : 0);
        i32 av = readBe32Local(a);
        i32 bv = readBe32Local(b);
        if (av < bv)
            return -1;
        if (av > bv)
            return 1;
        return 0;
    }

    if (type == ColumnType::Int64 || type == ColumnType::Timestamp) {
        if (a.size() != 8 || b.size() != 8)
            return (a.size() < b.size()) ? -1 : (a.size() > b.size() ? 1 : 0);
        i64 av = readBe64Local(a);
        i64 bv = readBe64Local(b);
        if (av < bv)
            return -1;
        if (av > bv)
            return 1;
        return 0;
    }

    if (type == ColumnType::Float32) {
        if (a.size() != 4 || b.size() != 4)
            return (a.size() < b.size()) ? -1 : (a.size() > b.size() ? 1 : 0);
        u32 au = 0;
        au |= static_cast<u32>(a[0]) << 24;
        au |= static_cast<u32>(a[1]) << 16;
        au |= static_cast<u32>(a[2]) << 8;
        au |= static_cast<u32>(a[3]) << 0;
        u32 bu = 0;
        bu |= static_cast<u32>(b[0]) << 24;
        bu |= static_cast<u32>(b[1]) << 16;
        bu |= static_cast<u32>(b[2]) << 8;
        bu |= static_cast<u32>(b[3]) << 0;
        float af;
        float bf;
        std::memcpy(&af, &au, 4);
        std::memcpy(&bf, &bu, 4);
        bool aNan = std::isnan(af);
        bool bNan = std::isnan(bf);
        if (aNan && bNan)
            return 0;
        if (aNan)
            return -1;
        if (bNan)
            return 1;
        if (af < bf)
            return -1;
        if (af > bf)
            return 1;
        return 0;
    }

    if (std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()))
        return -1;
    if (std::lexicographical_compare(b.begin(), b.end(), a.begin(), a.end()))
        return 1;
    return 0;
}

static string decoratedKeyString(const byteVec& pkBytes) {
    auto bytes = decoratedKeyBytes(pkBytes);
    return string(reinterpret_cast<const char*>(bytes.data()), reinterpret_cast<const char*>(bytes.data() + bytes.size()));
}

TableSchema readSchemaFromMetadata(const path& tableDirPath) {
    ifstream stream(metadataPath(tableDirPath), std::ios::binary);
    if (!stream.is_open())
        throw runtimeError("Missing metadata");
    char magic[7]{};
    stream.read(magic, 7);
    if (!stream || string(magic, 7) != string(metaMagic, 7))
        throw runtimeError("Bad metadata");
    char pad = 0;
    stream.read(&pad, 1);
    auto version = metaReadU32(stream);
    if (version != metaVersion)
        throw runtimeError("Bad metadata");
    (void)metaReadString(stream);
    (void)metaReadString(stream);
    (void)metaReadString(stream);
    (void)metaReadU64(stream);
    auto pkIndex = metaReadU32(stream);
    auto colCount = metaReadU32(stream);
    TableSchema schema;
    schema.columns.reserve(colCount);
    for (u32 i = 0; i < colCount; i++) {
        auto name = metaReadString(stream);
        u8 typeId = 0;
        stream.read(reinterpret_cast<char*>(&typeId), 1);
        schema.columns.push_back(ColumnDef{name, static_cast<ColumnType>(typeId)});
    }
    schema.primaryKeyIndex = pkIndex;
    return schema;
}

Table::Table(path tableDirPath, string keyspace, string table, string uuid, TableSchema schema, TableSettings settings)
    : tableDirPath_(std::move(tableDirPath))
    , keyspace_(std::move(keyspace))
    , table_(std::move(table))
    , uuid_(std::move(uuid))
    , schema_(std::move(schema))
    , settings_(settings)
    , nextSeq_(1)
    , walStop_(false) {
    manifest_.lastFlushedSeq = 0;
    manifest_.nextSstableGen = 1;
}

Table::~Table() {
    stopWalThread();
}

void Table::shutdown() {
    stopWalThread();
    std::lock_guard<std::mutex> lock(mutex_);
    commitLog_.close();
}

void Table::truncate() {
    stopWalThread();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        commitLog_.close();
    }

    std::error_code ec;
    if (std::filesystem::exists(tableDirPath_, ec)) {
        for (const auto& entry : std::filesystem::directory_iterator(tableDirPath_, ec)) {
            if (ec)
                break;
            if (!entry.is_regular_file())
                continue;
            auto name = entry.path().filename().string();
            if (name.rfind("sstable-", 0) == 0) {
                std::filesystem::remove(entry.path(), ec);
                ec.clear();
            }
            if (name == "manifest.bin" || name == "commitlog.bin") {
                std::filesystem::remove(entry.path(), ec);
                ec.clear();
            }
        }

        auto tmpDir = tableDirPath_ / "tmp";
        std::filesystem::remove_all(tmpDir, ec);
        ec.clear();
        std::filesystem::create_directories(tmpDir, ec);
        ec.clear();
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        memTable_.clear();
        ssTables_.clear();
        manifest_.lastFlushedSeq = 0;
        manifest_.nextSstableGen = 1;
        manifest_.sstableFiles.clear();
        nextSeq_ = 1;
        writeManifestAtomic(manifestPath(tableDirPath_), manifest_);
        commitLog_.openOrCreate(commitLogPath(tableDirPath_), true);
    }

    startWalThread();
}

const std::filesystem::path& Table::dir() const {
    return tableDirPath_;
}

const string& Table::keyspace() const {
    return keyspace_;
}

const string& Table::table() const {
    return table_;
}

const string& Table::uuid() const {
    return uuid_;
}

const TableSchema& Table::schema() const {
    return schema_;
}

void Table::writeMetadata() {
    ofstream stream(metadataPath(tableDirPath_), std::ios::binary | std::ios::trunc);
    if (!stream.is_open())
        throw runtimeError("Cannot write metadata");
    stream.write(metaMagic, 7);
    char pad = 0;
    stream.write(&pad, 1);
    metaWriteU32(stream, metaVersion);
    metaWriteString(stream, uuid_);
    metaWriteString(stream, keyspace_);
    metaWriteString(stream, table_);
    auto now = static_cast<u64>(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    metaWriteU64(stream, now);
    metaWriteU32(stream, static_cast<u32>(schema_.primaryKeyIndex));
    metaWriteU32(stream, static_cast<u32>(schema_.columns.size()));
    for (const auto& cols : schema_.columns) {
        metaWriteString(stream, cols.name);
        u8 typeId = static_cast<u8>(cols.type);
        stream.write(reinterpret_cast<const char*>(&typeId), 1);
    }
    stream.flush();
    stream.close();
}

void Table::loadMetadata() {
    auto schema = readSchemaFromMetadata(tableDirPath_);
    schema_ = schema;
}

void Table::openOrCreateFiles(bool createNew) {
    std::filesystem::create_directories(tableDirPath_ / "tmp");
    if (createNew) {
        writeMetadata();
        manifest_.lastFlushedSeq = 0;
        manifest_.nextSstableGen = 1;
        manifest_.sstableFiles.clear();
        writeManifestAtomic(manifestPath(tableDirPath_), manifest_);
        commitLog_.openOrCreate(commitLogPath(tableDirPath_), true);
    } else {
        loadMetadata();
        manifest_ = readManifest(manifestPath(tableDirPath_));
        commitLog_.openOrCreate(commitLogPath(tableDirPath_), false);
    }
}

static bool readExact(ifstream& in, void* p, usize n) {
    in.read(reinterpret_cast<char*>(p), static_cast<std::streamsize>(n));
    return static_cast<usize>(in.gcount()) == n;
}

void Table::recover() {
    std::lock_guard<std::mutex> lock(mutex_);
    ssTables_.clear();
    for (const auto& tableFiles : manifest_.sstableFiles) {
        ssTables_.push_back(loadSsTableIndex(tableDirPath_ / tableFiles));
    }

    ifstream stream(commitLogPath(tableDirPath_), std::ios::binary);
    if (stream.is_open()) {
        char magic[8]{};
        if (readExact(stream, magic, 8)) {
            u32 ver = 0;
            if (readExact(stream, &ver, sizeof(ver))) {
                if (string(magic, 7) == string("BZWAL001", 7) && ver == 1) {
                    while (stream) {
                        u64 seq = 0;
                        u32 keyLen = 0;
                        u32 valLen = 0;
                        if (!readExact(stream, &seq, sizeof(seq)))
                            break;
                        if (!readExact(stream, &keyLen, sizeof(keyLen)))
                            break;
                        if (!readExact(stream, &valLen, sizeof(valLen)))
                            break;
                        string key;
                        key.resize(keyLen);
                        if (keyLen > 0) {
                            if (!readExact(stream, key.data(), keyLen))
                                break;
                        }
                        byteVec val;
                        val.resize(valLen);
                        if (valLen > 0) {
                            if (!readExact(stream, val.data(), valLen))
                                break;
                        }
                        u32 c = 0;
                        if (!readExact(stream, &c, sizeof(c)))
                            break;

                        byteVec buf;
                        buf.reserve(sizeof(seq) + sizeof(keyLen) + sizeof(valLen) + keyLen + valLen);
                        auto add = [&](const void* p, usize n) {
                            const u8* bytePtr = static_cast<const u8*>(p);
                            buf.insert(buf.end(), bytePtr, bytePtr + n);
                        };
                        add(&seq, sizeof(seq));
                        add(&keyLen, sizeof(keyLen));
                        add(&valLen, sizeof(valLen));
                        if (keyLen > 0)
                            add(key.data(), key.size());
                        if (valLen > 0)
                            add(val.data(), val.size());
                        if (crc32(buf.data(), buf.size()) != c)
                            break;

                        memTable_.put(key, seq, val);
                        if (seq >= nextSeq_)
                            nextSeq_ = seq + 1;
                    }
                }
            }
        }
    }

    startWalThread();
}

void Table::putRow(const byteVec& pkBytes, const byteVec& rowBytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    u64 seq = nextSeq_++;
    string dkey = decoratedKeyString(pkBytes);
    commitLog_.append(seq, std::string_view(dkey.data(), dkey.size()), rowBytes);
    if (settings_.walFsync == "always")
        commitLog_.fsyncNow();
    memTable_.put(dkey, seq, rowBytes);
}

void Table::deleteRow(const byteVec& pkBytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    u64 seq = nextSeq_++;
    string dkey = decoratedKeyString(pkBytes);
    byteVec tombstone;
    commitLog_.append(seq, std::string_view(dkey.data(), dkey.size()), tombstone);
    if (settings_.walFsync == "always")
        commitLog_.fsyncNow();
    memTable_.put(dkey, seq, tombstone);
}

std::optional<byteVec> Table::getRow(const byteVec& pkBytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    string dkey = decoratedKeyString(pkBytes);
    auto memory = memTable_.get(dkey);
    if (memory.has_value()) {
        if (memory->value.empty())
            return std::nullopt;
        return memory->value;
    }
    auto dkeyBytes = decoratedKeyBytes(pkBytes);
    for (usize i = ssTables_.size(); i-- > 0;) {
        auto table = ssTableGet(ssTables_[i], dkeyBytes);
        if (table.has_value()) {
            if (table->empty())
                return std::nullopt;
            return table;
        }
    }
    return std::nullopt;
}

std::vector<Table::ScanRow> Table::scanAllRowsByPk(bool desc) {
    TableSchema schemaSnap;
    std::vector<std::pair<string, MemValue>> memSnap;
    std::vector<SsTableFile> ssSnap;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        schemaSnap = schema_;
        memSnap = memTable_.snapshot();
        ssSnap = ssTables_;
    }

    std::unordered_map<string, std::pair<u64, byteVec>> latest;
    latest.reserve(memSnap.size() + 32);

    for (const auto& kv : memSnap) {
        const auto& key = kv.first;
        const auto& mv = kv.second;
        auto it = latest.find(key);
        if (it == latest.end() || mv.seq > it->second.first)
            latest[key] = {mv.seq, mv.value};
    }

    for (const auto& ss : ssSnap) {
        auto entries = ssTableScanAll(ss);
        for (const auto& e : entries) {
            string key(reinterpret_cast<const char*>(e.key.data()), reinterpret_cast<const char*>(e.key.data() + e.key.size()));
            auto it = latest.find(key);
            if (it == latest.end() || e.seq > it->second.first)
                latest[key] = {e.seq, e.value};
        }
    }

    std::vector<Table::ScanRow> out;
    out.reserve(latest.size());
    for (auto& kv : latest) {
        const string& dkey = kv.first;
        const byteVec& rowBytes = kv.second.second;
        if (rowBytes.empty())
            continue;

        Table::ScanRow r;
        r.pkBytes = pkBytesFromDecoratedKeyString(dkey);
        r.rowBytes = rowBytes;
        out.push_back(std::move(r));
    }

    ColumnType pkType = schemaSnap.columns[schemaSnap.primaryKeyIndex].type;
    std::sort(out.begin(), out.end(), [&](const Table::ScanRow& a, const Table::ScanRow& b) {
        int cmp = comparePkBytes(pkType, a.pkBytes, b.pkBytes);
        if (cmp == 0) {
            if (std::lexicographical_compare(a.pkBytes.begin(), a.pkBytes.end(), b.pkBytes.begin(), b.pkBytes.end()))
                cmp = -1;
            else if (std::lexicographical_compare(b.pkBytes.begin(), b.pkBytes.end(), a.pkBytes.begin(), a.pkBytes.end()))
                cmp = 1;
            else
                cmp = 0;
        }
        return desc ? (cmp > 0) : (cmp < 0);
    });

    return out;
}

void Table::flush() {
    std::vector<std::pair<string, MemValue>> snap;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (memTable_.size() == 0)
            return;
        snap = memTable_.snapshot();
    }

    std::vector<SsEntry> entries;
    entries.reserve(snap.size());
    u64 maxSeq = 0;
    for (auto& kv : snap) {
        byteVec key;
        key.insert(key.end(), reinterpret_cast<const u8*>(kv.first.data()), reinterpret_cast<const u8*>(kv.first.data() + kv.first.size()));
        entries.push_back(SsEntry{std::move(key), kv.second.seq, kv.second.value});
        if (kv.second.seq > maxSeq)
            maxSeq = kv.second.seq;
    }

    std::sort(entries.begin(), entries.end(), [](const SsEntry& lhs, const SsEntry& rhs) {
        usize minSize = std::min(lhs.key.size(), rhs.key.size());
        for (usize i = 0; i < minSize; i++) {
            if (lhs.key[i] < rhs.key[i])
                return true;
            if (lhs.key[i] > rhs.key[i])
                return false;
        }
        return lhs.key.size() < rhs.key.size();
    });

    string fileName;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        char buf[64];
        std::snprintf(buf, sizeof(buf), "sstable-%06llu.bin", static_cast<unsigned long long>(manifest_.nextSstableGen));
        fileName = buf;
    }

    auto tmpPath = tableDirPath_ / "tmp" / (fileName + ".tmp");
    auto finalPath = tableDirPath_ / fileName;
    writeSsTable(tmpPath, entries, settings_.sstableIndexStride);
    std::filesystem::rename(tmpPath, finalPath);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        manifest_.sstableFiles.push_back(fileName);
        manifest_.nextSstableGen += 1;
        manifest_.lastFlushedSeq = maxSeq;
        writeManifestAtomic(manifestPath(tableDirPath_), manifest_);
        ssTables_.push_back(loadSsTableIndex(finalPath));
        memTable_.clear();
        commitLog_.openOrCreate(commitLogPath(tableDirPath_), true);
    }
}

void Table::startWalThread() {
    if (settings_.walFsync != "periodic")
        return;
    if (walThread_.joinable())
        return;
    walStop_ = false;
    walThread_ = std::thread([this]() {
        walThreadMain();
    });
}

void Table::stopWalThread() {
    walStop_ = true;
    if (walThread_.joinable())
        walThread_.join();
}

void Table::walThreadMain() {
    using namespace std::chrono;
    auto interval = milliseconds(settings_.walFsyncIntervalMs == 0 ? 50 : settings_.walFsyncIntervalMs);
    while (!walStop_.load()) {
        std::this_thread::sleep_for(interval);
        bool doFsync = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (commitLog_.isDirty())
                doFsync = true;
        }
        if (doFsync) {
            try {
                std::lock_guard<std::mutex> lock(mutex_);
                commitLog_.fsyncNow();
            } catch (...) {
            }
        }
    }
}

}
