#pragma once

#include <atomic>
#include "prelude.h"

#include <filesystem>
#include <string>

namespace xeondb {

inline constexpr const char* commitLogMagic = "BZWAL002";
inline constexpr usize commitLogMagicLen = 7;
inline constexpr u32 commitLogVersion = 2;

class CommitLog {
public:
    CommitLog();
    ~CommitLog();

    CommitLog(const CommitLog&) = delete;
    CommitLog& operator=(const CommitLog&) = delete;

    void openOrCreate(const std::filesystem::path& path, bool truncate);
    void append(u64 seq, stringView key, const byteVec& value);
    void fsyncNow();
    void close();

    usize bytesSinceFsync() const;
    void resetBytesSinceFsync();
    bool isDirty() const;
    void clearDirty();

    const std::filesystem::path& path() const;

private:
    int fileDesc;
    std::filesystem::path path_;
    std::atomic<usize> bytesSinceFsync_;
    std::atomic<bool> logDirty;
};

}
