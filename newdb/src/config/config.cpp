#include "config/config.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <string>

using std::string;
using std::ifstream;
using std::ofstream;

namespace blazeDb
{

    static string trim(const string &s)
    {
        usize i = 0;
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i])))
        {
            i++;
        }
        usize j = s.size();
        while (j > i && std::isspace(static_cast<unsigned char>(s[j - 1])))
        {
            j--;
        }
        return s.substr(i, j - i);
    }

    static string toLower(string s)
    {
        for (char &c : s)
        {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        return s;
    }

    static u64 parseU64(const string &s, const string &key)
    {
        try
        {
            usize i = 0;
            auto v = std::stoull(s, &i, 10);
            if (i != s.size())
            {
                throw std::invalid_argument("bad");
            }
            return static_cast<u64>(v);
        }
        catch (...)
        {
            throw runtimeError("Invalid value for " + key);
        }
    }

    static usize parseSize(const string &s, const string &key)
    {
        auto temp = parseU64(s, key);
        return static_cast<usize>(temp);
    }

    Settings loadSettings(const string &filePath)
    {
        Settings s;
        s.host = "0.0.0.0";
        s.port = 9876;
        s.dataDir = "/var/lib/blazedb/data";
        s.maxLineBytes = 1024 * 1024;
        s.maxConnections = 1024;
        s.walFsync = "periodic";
        s.walFsyncIntervalMs = 50;
        s.walFsyncBytes = 1024 * 1024;
        s.memtableMaxBytes = 32ull * 1024ull * 1024ull;
        s.sstableIndexStride = 16;

        ifstream in(filePath);
        if (!in.is_open())
        {
            throw runtimeError("Cannot open config: " + filePath);
        }

        string line;
        while (std::getline(in, line))
        {
            line = trim(line);
            if (line.empty())
            {
                continue;
            }
            auto pos = line.find(':');
            if (pos == string::npos)
            {
                continue;
            }
            string key = trim(line.substr(0, pos));
            string value = trim(line.substr(pos + 1));

            if (key == "host")
            {
                s.host = value;
            }
            else if (key == "port")
            {
                s.port = static_cast<u16>(parseU64(value, key));
            }
            else if (key == "dataDir")
            {
                s.dataDir = value;
            }
            else if (key == "maxLineBytes")
            {
                s.maxLineBytes = parseSize(value, key);
            }
            else if (key == "maxConnections")
            {
                s.maxConnections = parseSize(value, key);
            }
            else if (key == "walFsync")
            {
                s.walFsync = toLower(value);
            }
            else if (key == "walFsyncIntervalMs")
            {
                s.walFsyncIntervalMs = parseU64(value, key);
            }
            else if (key == "walFsyncBytes")
            {
                s.walFsyncBytes = parseSize(value, key);
            }
            else if (key == "memtableMaxBytes")
            {
                s.memtableMaxBytes = parseSize(value, key);
            }
            else if (key == "sstableIndexStride")
            {
                s.sstableIndexStride = parseSize(value, key);
            }
        }

        return s;
    }

    string resolveDataDir(const string &preferredDataDir)
    {
        namespace fs = std::filesystem;

        auto canUse = [](const fs::path &p) -> bool
        {
            std::error_code ec;
            fs::create_directories(p, ec);
            if (ec)
            {
                return false;
            }
            fs::path testFile = p / ".blazeDbWriteTest";
            std::ofstream out(testFile, std::ios::binary | std::ios::trunc);
            if (!out.is_open())
            {
                return false;
            }
            out.write("x", 1);
            out.close();
            fs::remove(testFile, ec);
            return true;
        };

        fs::path preferred(preferredDataDir);
        if (canUse(preferred))
        {
            return preferred.string();
        }

        fs::path fallback = fs::path(".") / "var" / "lib" / "blazedb" / "data";
        if (canUse(fallback))
        {
            return fallback.string();
        }

        throw runtimeError("Cannot create usable dataDir");
    }

}
