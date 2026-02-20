#include "storage/manifest.h"

#include "util/binIo.h"

#include <fstream>

using std::ifstream;
using std::ofstream;
using std::string;

namespace xeondb {

static constexpr const char* manifestMagic = "BZMF001";
static constexpr u32 manifestVersion = 1;

Manifest readManifest(const std::filesystem::path& path) {
    Manifest m;
    m.lastFlushedSeq = 0;
    m.nextSstableGen = 1;

    ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        return m;
    }
    char magic[7]{};
    in.read(magic, 7);
    if (!in || string(magic, 7) != string(manifestMagic, 7)) {
        return m;
    }
    char pad = 0;
    in.read(&pad, 1);
    if (!in) {
        return m;
    }

    try {
        u32 version = readU32(in);
        if (version != manifestVersion) {
            return m;
        }
        m.lastFlushedSeq = readU64(in);
        m.nextSstableGen = readU64(in);
        u64 count = readU64(in);
        for (u64 i = 0; i < count; i++) {
            m.sstableFiles.push_back(readString(in));
        }
        return m;
    } catch (...) {
        return m;
    }
}

void writeManifestAtomic(const std::filesystem::path& path, const Manifest& manifest) {
    auto tmp = path;
    tmp += ".tmp";

    ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        throw runtimeError("Cannot write manifest");
    }
    out.write(manifestMagic, 7);
    char pad = 0;
    out.write(&pad, 1);
    writeU32(out, manifestVersion);
    writeU64(out, manifest.lastFlushedSeq);
    writeU64(out, manifest.nextSstableGen);
    writeU64(out, static_cast<u64>(manifest.sstableFiles.size()));
    for (const auto& f : manifest.sstableFiles) {
        writeString(out, f);
    }
    out.flush();
    out.close();
    std::filesystem::rename(tmp, path);
}

}
