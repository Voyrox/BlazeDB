#include "storage/commitLog.h"

#include "util/crc32.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace xeondb {
static void writeAll(int fd, const void* p, usize n) {
    const u8* buffer = static_cast<const u8*>(p);
    usize offset = 0;
    while (offset < n) {
        ssize_t wrote = ::write(fd, buffer + offset, n - offset);
        if (wrote < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw runtimeError("write failed");
        }
        offset += static_cast<usize>(wrote);
    }
}

static bool readAllExact(int fd, void* p, usize n) {
    u8* buffer = static_cast<u8*>(p);
    usize offset = 0;
    while (offset < n) {
        ssize_t got = ::read(fd, buffer + offset, n - offset);
        if (got == 0) {
            return false;
        }
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        offset += static_cast<usize>(got);
    }
    return true;
}

static void writeHeader(int fd) {
    writeAll(fd, commitLogMagic, commitLogMagicLen);
    u8 pad = 0;
    writeAll(fd, &pad, 1);
    u32 ver = commitLogVersion;
    writeAll(fd, &ver, sizeof(ver));
}

static bool validateHeader(int fd) {
    if (::lseek(fd, 0, SEEK_SET) < 0) {
        return false;
    }
    char magic[commitLogMagicLen]{};
    u8 pad = 0;
    u32 ver = 0;
    if (!readAllExact(fd, magic, commitLogMagicLen))
        return false;
    if (!readAllExact(fd, &pad, 1))
        return false;
    if (!readAllExact(fd, &ver, sizeof(ver)))
        return false;
    if (std::memcmp(magic, commitLogMagic, commitLogMagicLen) != 0)
        return false;
    if (pad != 0)
        return false;
    return ver == commitLogVersion;
}

CommitLog::CommitLog()
    : fileDesc(-1)
    , bytesSinceFsync_(0)
    , logDirty(false) {
}

CommitLog::~CommitLog() {
    close();
}

void CommitLog::openOrCreate(const std::filesystem::path& path, bool truncate) {
    close();
    path_ = path;
    int flags = O_CREAT | O_RDWR | O_APPEND;
    if (truncate)
        flags = O_CREAT | O_RDWR | O_TRUNC | O_APPEND;
    fileDesc = ::open(path_.c_str(), flags, 0644);
    if (fileDesc < 0) {
        throw runtimeError("cannot open commitlog");
    }
    if (truncate) {
        writeHeader(fileDesc);
    } else {
        struct stat st{};
        if (::fstat(fileDesc, &st) == 0) {
            if (st.st_size == 0) {
                writeHeader(fileDesc);
            } else {
                if (!validateHeader(fileDesc)) {
                    ::close(fileDesc);
                    fileDesc = ::open(path_.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_APPEND, 0644);
                    if (fileDesc < 0) {
                        throw runtimeError("cannot open commitlog");
                    }
                    writeHeader(fileDesc);
                }
            }
        }
    }
    bytesSinceFsync_ = 0;
    logDirty = false;
}

void CommitLog::append(u64 seq, stringView key, const byteVec& value) {
    if (fileDesc < 0) {
        throw runtimeError("commitlog not open");
    }

    u32 keyLen = static_cast<u32>(key.size());
    u32 valLen = static_cast<u32>(value.size());

    byteVec buf;
    buf.reserve(sizeof(seq) + sizeof(keyLen) + sizeof(valLen) + key.size() + value.size());

    auto appendBytes = [&](const void* p, usize n) {
        const u8* bPtr = static_cast<const u8*>(p);
        buf.insert(buf.end(), bPtr, bPtr + n);
    };

    appendBytes(&seq, sizeof(seq));
    appendBytes(&keyLen, sizeof(keyLen));
    appendBytes(&valLen, sizeof(valLen));
    appendBytes(key.data(), key.size());
    if (!value.empty()) {
        appendBytes(value.data(), value.size());
    }

    u32 checksum = crc32(buf.data(), buf.size());
    appendBytes(&checksum, sizeof(checksum));

    writeAll(fileDesc, buf.data(), buf.size());
    bytesSinceFsync_ += buf.size();
    logDirty = true;
}

void CommitLog::fsyncNow() {
    if (fileDesc < 0) {
        return;
    }
    if (::fsync(fileDesc) != 0) {
        throw runtimeError("fsync failed");
    }
    bytesSinceFsync_ = 0;
    logDirty = false;
}

void CommitLog::close() {
    if (fileDesc >= 0) {
        ::close(fileDesc);
        fileDesc = -1;
    }
}

usize CommitLog::bytesSinceFsync() const {
    return bytesSinceFsync_.load();
}

void CommitLog::resetBytesSinceFsync() {
    bytesSinceFsync_ = 0;
}

bool CommitLog::isDirty() const {
    return logDirty.load();
}

void CommitLog::clearDirty() {
    logDirty = false;
}

const std::filesystem::path& CommitLog::path() const {
    return path_;
}

}
