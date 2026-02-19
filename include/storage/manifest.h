#pragma once

#include "prelude.h"

#include <filesystem>
#include <string>
#include <vector>

using std::filesystem::path;

namespace xeondb
{

    struct Manifest
    {
        u64 lastFlushedSeq;
        u64 nextSstableGen;
        std::vector<std::string> sstableFiles;
    };

    Manifest readManifest(const path &path);
    void writeManifestAtomic(const path &path, const Manifest &manifest);

}
