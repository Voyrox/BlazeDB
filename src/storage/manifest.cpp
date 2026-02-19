#include "storage/manifest.h"

#include <fstream>

using std::ifstream;
using std::ofstream;
using std::string;

namespace blazeDb
{

    static constexpr const char *manifestMagic = "BZMF001";
    static constexpr u32 manifestVersion = 1;

    static void writeU32(ofstream &out, u32 v)
    {
        out.write(reinterpret_cast<const char *>(&v), sizeof(v));
    }

    static void writeU64(ofstream &out, u64 v)
    {
        out.write(reinterpret_cast<const char *>(&v), sizeof(v));
    }

    static u32 readU32(ifstream &in)
    {
        u32 value = 0;
        in.read(reinterpret_cast<char *>(&value), sizeof(value));
        return value;
    }

    static u64 readU64(ifstream &in)
    {
        u64 value = 0;
        in.read(reinterpret_cast<char *>(&value), sizeof(value));
        return value;
    }

    static void writeString(ofstream &out, const string &s)
    {
        u32 len = static_cast<u32>(s.size());
        writeU32(out, len);
        out.write(s.data(), s.size());
    }

    static string readString(ifstream &in)
    {
        u32 len = readU32(in);
        string s;
        s.resize(len);
        in.read(s.data(), len);
        return s;
    }

    Manifest readManifest(const std::filesystem::path &path)
    {
        Manifest m;
        m.lastFlushedSeq = 0;
        m.nextSstableGen = 1;

        ifstream in(path, std::ios::binary);
        if (!in.is_open())
        {
            return m;
        }
        char magic[7]{};
        in.read(magic, 7);
        if (!in || string(magic, 7) != string(manifestMagic, 7))
        {
            return m;
        }
        char pad = 0;
        in.read(&pad, 1);
        if (!in)
        {
            return m;
        }
        u32 version = readU32(in);
        if (version != manifestVersion)
        {
            return m;
        }
        m.lastFlushedSeq = readU64(in);
        m.nextSstableGen = readU64(in);
        u64 count = readU64(in);
        for (u64 i = 0; i < count; i++)
        {
            m.sstableFiles.push_back(readString(in));
        }
        return m;
    }

    void writeManifestAtomic(const std::filesystem::path &path, const Manifest &manifest)
    {
        auto tmp = path;
        tmp += ".tmp";

        ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out.is_open())
        {
            throw runtimeError("Cannot write manifest");
        }
        out.write(manifestMagic, 7);
        char pad = 0;
        out.write(&pad, 1);
        writeU32(out, manifestVersion);
        writeU64(out, manifest.lastFlushedSeq);
        writeU64(out, manifest.nextSstableGen);
        writeU64(out, static_cast<u64>(manifest.sstableFiles.size()));
        for (const auto &f : manifest.sstableFiles)
        {
            writeString(out, f);
        }
        out.flush();
        out.close();
        std::filesystem::rename(tmp, path);
    }

}
