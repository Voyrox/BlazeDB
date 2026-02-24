#include "config/config.h"

#include "util/log.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <string>

using std::string;
using std::ifstream;
using std::ofstream;

namespace xeondb {

static string trim(const string& s) {
    usize i = 0;
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) {
        i++;
    }
    usize j = s.size();
    while (j > i && std::isspace(static_cast<unsigned char>(s[j - 1]))) {
        j--;
    }
    return s.substr(i, j - i);
}

static string stripQuotes(const string& s) {
    if (s.size() >= 2) {
        char a = s.front();
        char b = s.back();
        if ((a == '"' && b == '"') || (a == '\'' && b == '\'')) {
            return s.substr(1, s.size() - 2);
        }
    }
    return s;
}

static string toLower(string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

static bool parseBool(const string& s, const string& key) {
    string v = toLower(s);
    if (v == "1" || v == "true" || v == "yes" || v == "y" || v == "on")
        return true;
    if (v == "0" || v == "false" || v == "no" || v == "n" || v == "off")
        return false;
    throw runtimeError("Invalid value for " + key);
}

static u64 parseU64(const string& s, const string& key) {
    try {
        usize i = 0;
        auto v = std::stoull(s, &i, 10);
        if (i != s.size()) {
            throw std::invalid_argument("bad");
        }
        return static_cast<u64>(v);
    } catch (...) {
        throw runtimeError("Invalid value for " + key);
    }
}

static usize parseSize(const string& s, const string& key) {
    auto temp = parseU64(s, key);
    return static_cast<usize>(temp);
}

Settings loadSettings(const string& filePath) {
    Settings s;
    s.host = "0.0.0.0";
    s.port = 9876;
    s.dataDir = "/var/lib/xeondb/data";
    s.maxLineBytes = 1024 * 1024;
    s.maxConnections = 1024;
    s.walFsync = "periodic";
    s.walFsyncIntervalMs = 50;
    s.walFsyncBytes = 1024 * 1024;
    s.memtableMaxBytes = 32ull * 1024ull * 1024ull;
    s.sstableIndexStride = 16;
    s.quotaEnforcementEnabled = false;
    s.quotaBytesUsedCacheTtlMs = 2000;
    s.authUsername.clear();
    s.authPassword.clear();

    ifstream in(filePath);
    if (!in.is_open()) {
        throw runtimeError("Cannot open config: " + filePath);
    }

    string currentSection;
    string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty()) {
            continue;
        }
        if (line[0] == '#') {
            continue;
        }
        auto pos = line.find(':');
        if (pos == string::npos) {
            continue;
        }
        string key = trim(line.substr(0, pos));
        string value = trim(line.substr(pos + 1));

        if (value.empty()) {
            currentSection = toLower(key);
            continue;
        }

        const string section = currentSection;
        value = stripQuotes(value);

        if (key == "host") {
            s.host = value;
        } else if (key == "port") {
            s.port = static_cast<u16>(parseU64(value, key));
        } else if (key == "dataDir") {
            s.dataDir = value;
        } else if (key == "maxLineBytes") {
            s.maxLineBytes = parseSize(value, key);
        } else if (key == "maxConnections") {
            s.maxConnections = parseSize(value, key);
        } else if (key == "quotaEnforcementEnabled") {
            s.quotaEnforcementEnabled = parseBool(value, key);
        } else if (key == "quotaBytesUsedCacheTtlMs") {
            s.quotaBytesUsedCacheTtlMs = parseU64(value, key);
        } else if (key == "walFsync") {
            s.walFsync = toLower(value);
        } else if (key == "walFsyncIntervalMs") {
            s.walFsyncIntervalMs = parseU64(value, key);
        } else if (key == "walFsyncBytes") {
            s.walFsyncBytes = parseSize(value, key);
        } else if (key == "memtableMaxBytes") {
            s.memtableMaxBytes = parseSize(value, key);
        } else if (key == "sstableIndexStride") {
            s.sstableIndexStride = parseSize(value, key);
        } else if (section == "auth" && key == "username") {
            s.authUsername = value;
        } else if (section == "auth" && key == "password") {
            s.authPassword = value;
        }
    }

    return s;
}

string resolveDataDir(const string& preferredDataDir) {
    namespace fs = std::filesystem;

    auto canUse = [](const fs::path& p, bool* createdDirs, string* reason) -> bool {
        std::error_code ec;
        const bool created = fs::create_directories(p, ec);
        if (createdDirs != nullptr) {
            *createdDirs = created;
        }
        if (ec) {
            if (reason != nullptr) {
                *reason = "Create Directories: " + ec.message();
            }
            return false;
        }
        fs::path testFile = p / ".xeondbWriteTest";
        std::ofstream out(testFile, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            if (reason != nullptr) {
                *reason = "cannot open write test file";
            }
            return false;
        }
        out.write("x", 1);
        out.close();
        fs::remove(testFile, ec);
        if (ec && reason != nullptr) {
            *reason = "remove write test file: " + ec.message();
        }
        return true;
    };

    fs::path preferred = preferredDataDir.empty() ? fs::path("/var/lib/xeondb/data") : fs::path(preferredDataDir);

    bool preferredCreated = false;
    string preferredReason;
    if (canUse(preferred, &preferredCreated, &preferredReason)) {
        return preferred.string();
    }

    fs::path fallback = fs::path(".") / "var" / "lib" / "xeondb" / "data";

    bool fallbackCreated = false;
    string fallbackReason;
    if (canUse(fallback, &fallbackCreated, &fallbackReason)) {
        string msg = "Cannot use dataDir=" + preferred.string();
        if (!preferredReason.empty()) {
            msg += " (" + preferredReason + ")";
        }
        msg += ". Using local dataDir=" + fallback.string();
        if (fallbackCreated) {
            msg += " (created)";
        }
        xeondb::log(xeondb::LogLevel::WARN, msg);
        return fallback.string();
    }

    string err = "Cannot create usable dataDir. preferred=" + preferred.string();
    if (!preferredReason.empty()) {
        err += " (" + preferredReason + ")";
    }
    err += " fallback=" + fallback.string();
    if (!fallbackReason.empty()) {
        err += " (" + fallbackReason + ")";
    }
    throw runtimeError(err);
}

}
