#include "net/serverTcp.h"

#include "net/detail/serverTcpInternal.h"

#include "core/paths.h"

#include <filesystem>
#include <system_error>

namespace xeondb {

static u64 dirBytesUsed(const path& root) {
    u64 total = 0;
    std::error_code ec;

    if (root.empty()) {
        return 0;
    }
    if (!std::filesystem::exists(root, ec) || ec) {
        return 0;
    }

    std::filesystem::recursive_directory_iterator it(root, std::filesystem::directory_options::skip_permission_denied, ec);
    const std::filesystem::recursive_directory_iterator end;

    for (; !ec && it != end; it.increment(ec)) {
        std::error_code ec2;
        if (it->is_regular_file(ec2) && !ec2) {
            const auto sz = it->file_size(ec2);
            if (!ec2) {
                total += static_cast<u64>(sz);
            }
        }
    }

    return total;
}

std::optional<u64> ServerTcp::quotaBytesForKeyspace(const std::string& keyspace) const {
    if (db_ == nullptr) {
        return std::nullopt;
    }
    const auto& s = db_->settings();
    if (!s.quotaEnforcementEnabled) {
        return std::nullopt;
    }
    return db_->keyspaceQuotaBytes(keyspace);
}

u64 ServerTcp::bytesUsedForKeyspaceCached(const std::string& keyspace) {
    using server_tcp_detail::nowMs;

    if (db_ == nullptr) {
        return 0;
    }

    const u64 ttlMs = db_->settings().quotaBytesUsedCacheTtlMs == 0 ? 2000 : db_->settings().quotaBytesUsedCacheTtlMs;
    const i64 now = nowMs();

    {
        std::lock_guard<std::mutex> lock(bytesUsedCacheMutex_);
        auto it = bytesUsedCache_.find(keyspace);
        if (it != bytesUsedCache_.end() && it->second.computedAtMs > 0) {
            const i64 age = now - it->second.computedAtMs;
            if (age >= 0 && static_cast<u64>(age) < ttlMs) {
                return it->second.bytesUsed;
            }
        }
    }

    const u64 fresh = dirBytesUsed(keyspaceDir(db_->dataDir(), keyspace));

    {
        std::lock_guard<std::mutex> lock(bytesUsedCacheMutex_);
        auto& e = bytesUsedCache_[keyspace];
        e.bytesUsed = fresh;
        e.computedAtMs = now;
    }
    return fresh;
}

void ServerTcp::invalidateBytesUsedCache(const std::string& keyspace) {
    std::lock_guard<std::mutex> lock(bytesUsedCacheMutex_);
    auto it = bytesUsedCache_.find(keyspace);
    if (it != bytesUsedCache_.end()) {
        it->second.computedAtMs = 0;
    }
}

bool ServerTcp::quotaWouldAllowAndReserve(const std::string& keyspace, u64 quotaBytes, u64 estimatedWriteBytes) {
    using server_tcp_detail::nowMs;

    if (db_ == nullptr) {
        return true;
    }
    if (quotaBytes == 0) {
        return true;
    }
    if (estimatedWriteBytes == 0) {
        return true;
    }

    const u64 ttlMs = db_->settings().quotaBytesUsedCacheTtlMs == 0 ? 2000 : db_->settings().quotaBytesUsedCacheTtlMs;
    const i64 now = nowMs();

    bool needScan = false;
    {
        std::lock_guard<std::mutex> lock(bytesUsedCacheMutex_);
        auto& e = bytesUsedCache_[keyspace];
        if (e.computedAtMs <= 0) {
            needScan = true;
        } else {
            const i64 age = now - e.computedAtMs;
            if (age < 0 || static_cast<u64>(age) >= ttlMs) {
                needScan = true;
            }
        }
    }

    if (needScan) {
        const u64 fresh = dirBytesUsed(keyspaceDir(db_->dataDir(), keyspace));
        std::lock_guard<std::mutex> lock(bytesUsedCacheMutex_);
        auto& e = bytesUsedCache_[keyspace];
        e.bytesUsed = fresh;
        e.computedAtMs = now;
    }

    std::lock_guard<std::mutex> lock(bytesUsedCacheMutex_);
    auto& e = bytesUsedCache_[keyspace];
    if (e.bytesUsed + estimatedWriteBytes > quotaBytes) {
        return false;
    }

    e.bytesUsed += estimatedWriteBytes;
    return true;
}

}
