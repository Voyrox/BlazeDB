#include "core/paths.h"

#include "util/binIo.h"

#include <filesystem>
#include <fstream>
#include <unordered_map>

using std::ifstream;
using std::ofstream;
using std::string;

namespace xeondb {

static constexpr const char* schemaMagic = "BZSC001";
static constexpr u32 schemaVersion = 1;

static std::unordered_map<string, string> readSchemaMap(const path& schemaFile) {
    std::unordered_map<string, string> map;

    ifstream in(schemaFile, std::ios::binary);
    if (!in.is_open()) {
        return map;
    }

    char magicBuf[8]{};
    in.read(magicBuf, 7);
    if (string(magicBuf, 7) != string(schemaMagic, 7)) {
        return map;
    }

    char pad = 0;
    in.read(&pad, 1);
    if (!in) {
        return map;
    }

    try {
        auto version = readU32(in);
        if (version != schemaVersion) {
            return map;
        }
        auto count = readU64(in);
        for (u64 i = 0; i < count; i++) {
            auto name = readString(in);
            auto uuid = readString(in);
            map[name] = uuid;
        }
        return map;
    } catch (...) {
        return std::unordered_map<string, string>{};
    }
}

static void writeSchemaMapAtomic(const path& schemaFile, const std::unordered_map<string, string>& map) {
    auto tmp = schemaFile;
    tmp += ".tmp";

    ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        throw runtimeError("Cannot write schema");
    }

    out.write(schemaMagic, 7);
    char pad = 0;
    out.write(&pad, 1);
    writeU32(out, schemaVersion);
    writeU64(out, static_cast<u64>(map.size()));
    for (const auto& kv : map) {
        writeString(out, kv.first);
        writeString(out, kv.second);
    }
    out.flush();
    out.close();
    std::filesystem::rename(tmp, schemaFile);
}

path keyspaceDir(const path& dataDir, const string& keyspace) {
    return dataDir / keyspace;
}

path schemaPath(const path& dataDir, const string& keyspace) {
    return keyspaceDir(dataDir, keyspace) / "schema.bin";
}

std::optional<string> findTableUuidFromSchema(const path& schemaFile, const string& table) {
    auto map = readSchemaMap(schemaFile);
    auto it = map.find(table);
    if (it == map.end()) {
        return std::nullopt;
    }
    return it->second;
}

void upsertTableUuidToSchema(const path& schemaFile, const string& table, const string& uuid) {
    auto map = readSchemaMap(schemaFile);
    map[table] = uuid;
    std::filesystem::create_directories(schemaFile.parent_path());
    writeSchemaMapAtomic(schemaFile, map);
}

bool removeTableFromSchema(const path& schemaFile, const string& table) {
    auto map = readSchemaMap(schemaFile);
    auto it = map.find(table);
    if (it == map.end()) {
        return false;
    }
    map.erase(it);
    std::filesystem::create_directories(schemaFile.parent_path());
    writeSchemaMapAtomic(schemaFile, map);
    return true;
}

std::optional<string> findTableUuidByScan(const path& keyspaceDirPath, const string& table) {
    std::error_code errorCode;
    if (!std::filesystem::exists(keyspaceDirPath, errorCode)) {
        return std::nullopt;
    }
    for (const auto& dirEntry : std::filesystem::directory_iterator(keyspaceDirPath, errorCode)) {
        if (errorCode) {
            break;
        }
        if (!dirEntry.is_directory()) {
            continue;
        }
        auto name = dirEntry.path().filename().string();
        auto prefix = table + "-";
        if (name.rfind(prefix, 0) == 0 && name.size() > prefix.size()) {
            return name.substr(prefix.size());
        }
    }
    return std::nullopt;
}

path tableDir(const path& dataDir, const string& keyspace, const string& table, const string& uuid) {
    return keyspaceDir(dataDir, keyspace) / (table + "-" + uuid);
}

}
