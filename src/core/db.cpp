#include "core/db.h"

#include "core/paths.h"
#include "util/uuid.h"

#include "query/schema.h"
#include "util/binIo.h"

#include <filesystem>
#include <algorithm>
#include <chrono>
#include <shared_mutex>
#include <set>
#include <cctype>

using std::string;
using std::shared_ptr;
using std::filesystem::path;

namespace xeondb {

static i64 nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static u64 nowBucket5m() {
    const i64 ms = nowMs();
    if (ms <= 0)
        return 0;
    constexpr i64 bucketMs = 5 * 60 * 1000;
    return static_cast<u64>(ms / bucketMs);
}

static string tableKey(const string& keyspace, const string& table) {
    return keyspace + "." + table;
}

Db::Db(Settings settings)
    : settings_(std::move(settings)) {
    settings_.dataDir = resolveDataDir(settings_.dataDir);
    effectiveDataDir_ = settings_.dataDir;
    std::filesystem::create_directories(effectiveDataDir_);
}

void Db::metricsTouchBucketLocked(MetricsSeries& m, u64 absBucket) {
    const usize idx = static_cast<usize>(absBucket % MetricsSeries::bucketCount);
    if (m.bucketId[idx] != absBucket) {
        m.bucketId[idx] = absBucket;
        m.connPeak[idx] = 0;
        m.queries[idx] = 0;
    }
}

void Db::metricsObserveConnPeakLocked(MetricsSeries& m, u64 absBucket) {
    metricsTouchBucketLocked(m, absBucket);
    const usize idx = static_cast<usize>(absBucket % MetricsSeries::bucketCount);
    if (m.connectionsActive > m.connPeak[idx])
        m.connPeak[idx] = m.connectionsActive;
}

Db::KeyspaceMetrics Db::keyspaceMetricsLocked(const string& keyspace, u64 nowBucket) const {
    KeyspaceMetrics out;
    out.labelsLast24h4h = {"-24h", "-20h", "-16h", "-12h", "-8h", "-4h"};

    auto it = metricsByKeyspace_.find(keyspace);
    if (it == metricsByKeyspace_.end()) {
        return out;
    }
    const MetricsSeries& m = it->second;
    out.connectionsActive = m.connectionsActive;

    auto bucketConnPeak = [&m](u64 absBucket) -> i64 {
        const usize idx = static_cast<usize>(absBucket % MetricsSeries::bucketCount);
        return (m.bucketId[idx] == absBucket) ? m.connPeak[idx] : 0;
    };
    auto bucketQueries = [&m](u64 absBucket) -> i64 {
        const usize idx = static_cast<usize>(absBucket % MetricsSeries::bucketCount);
        return (m.bucketId[idx] == absBucket) ? m.queries[idx] : 0;
    };

    const u64 firstBucket = (nowBucket >= MetricsSeries::bucketCount) ? (nowBucket - MetricsSeries::bucketCount + 1) : 0;
    (void)firstBucket;

    i64 totalQ = 0;
    for (u64 b = (nowBucket >= MetricsSeries::bucketCount - 1 ? (nowBucket - (MetricsSeries::bucketCount - 1)) : 0); b <= nowBucket; b++) {
        totalQ += bucketQueries(b);
    }
    out.queriesLast24hTotal = totalQ;

    for (usize w = 0; w < 6; w++) {
        // Window boundaries in buckets: 4h = 48 buckets.
        const u64 endExclusive = nowBucket + 1 - static_cast<u64>((5 - w) * 48);
        const u64 start = endExclusive - 48;

        i64 peak = 0;
        i64 sum = 0;
        for (u64 b = start; b < endExclusive; b++) {
            const i64 cp = bucketConnPeak(b);
            if (cp > peak)
                peak = cp;
            sum += bucketQueries(b);
        }
        out.connectionsLast24hPeak4h[w] = peak;
        out.queriesLast24h4h[w] = sum;
    }

    return out;
}

void Db::metricsOnUse(const string& oldKeyspace, const string& newKeyspace) {
    if (oldKeyspace == newKeyspace)
        return;
    const u64 b = nowBucket5m();
    std::lock_guard<std::mutex> lock(metricsMutex_);

    if (!oldKeyspace.empty()) {
        auto& m = metricsByKeyspace_[oldKeyspace];
        if (m.connectionsActive > 0)
            m.connectionsActive--;
        metricsObserveConnPeakLocked(m, b);
    }
    if (!newKeyspace.empty()) {
        auto& m = metricsByKeyspace_[newKeyspace];
        m.connectionsActive++;
        metricsObserveConnPeakLocked(m, b);
    }
}

void Db::metricsOnDisconnect(const string& keyspace) {
    if (keyspace.empty())
        return;
    const u64 b = nowBucket5m();
    std::lock_guard<std::mutex> lock(metricsMutex_);
    auto& m = metricsByKeyspace_[keyspace];
    if (m.connectionsActive > 0)
        m.connectionsActive--;
    metricsObserveConnPeakLocked(m, b);
}

void Db::metricsOnCommand(const string& keyspace) {
    if (keyspace.empty())
        return;
    const u64 b = nowBucket5m();
    std::lock_guard<std::mutex> lock(metricsMutex_);
    auto& m = metricsByKeyspace_[keyspace];
    metricsTouchBucketLocked(m, b);
    const usize idx = static_cast<usize>(b % MetricsSeries::bucketCount);
    m.queries[idx]++;
    metricsObserveConnPeakLocked(m, b);
}

void Db::metricsSampleAll() {
    const u64 b = nowBucket5m();
    std::lock_guard<std::mutex> lock(metricsMutex_);
    for (auto& it : metricsByKeyspace_) {
        metricsObserveConnPeakLocked(it.second, b);
    }
}

Db::KeyspaceMetrics Db::keyspaceMetrics(const string& keyspace) const {
    const u64 b = nowBucket5m();
    std::lock_guard<std::mutex> lock(metricsMutex_);
    return keyspaceMetricsLocked(keyspace, b);
}

bool Db::authEnabled() const {
    return !settings_.authUsername.empty() && !settings_.authPassword.empty();
}

bool Db::isSystemKeyspace(const string& keyspace) {
    if (keyspace.size() != 6)
        return false;
    return (std::tolower(static_cast<unsigned char>(keyspace[0])) == 's') && (std::tolower(static_cast<unsigned char>(keyspace[1])) == 'y') &&
           (std::tolower(static_cast<unsigned char>(keyspace[2])) == 's') && (std::tolower(static_cast<unsigned char>(keyspace[3])) == 't') &&
           (std::tolower(static_cast<unsigned char>(keyspace[4])) == 'e') && (std::tolower(static_cast<unsigned char>(keyspace[5])) == 'm');
}

string Db::grantKey(const string& keyspace, const string& username) {
    return keyspace + "#" + username;
}

void Db::keyspacesInsertSortedUnlocked(const string& keyspace) {
    auto it = std::lower_bound(keyspacesCache_.begin(), keyspacesCache_.end(), keyspace);
    if (it != keyspacesCache_.end() && *it == keyspace)
        return;
    keyspacesCache_.insert(it, keyspace);
}

void Db::keyspacesEraseUnlocked(const string& keyspace) {
    auto it = std::lower_bound(keyspacesCache_.begin(), keyspacesCache_.end(), keyspace);
    if (it != keyspacesCache_.end() && *it == keyspace)
        keyspacesCache_.erase(it);
}

std::optional<AuthedUser> Db::authenticate(const string& username, const string& password) const {
    std::shared_lock<std::shared_mutex> lock(authMutex_);
    auto itPass = usersPassword_.find(username);
    if (itPass == usersPassword_.end())
        return std::nullopt;
    auto itEnabled = usersEnabled_.find(username);
    if (itEnabled == usersEnabled_.end() || !itEnabled->second)
        return std::nullopt;
    if (itPass->second != password)
        return std::nullopt;
    auto itLevel = usersLevel_.find(username);
    if (itLevel == usersLevel_.end())
        return std::nullopt;
    return AuthedUser{username, itLevel->second};
}

bool Db::canCreateOrDropKeyspace(const AuthedUser& user) const {
    return user.level == 0;
}

bool Db::canAccessKeyspace(const AuthedUser& user, const string& keyspace) const {
    if (user.level == 0)
        return true;
    if (isSystemKeyspace(keyspace))
        return false;
    std::shared_lock<std::shared_mutex> lock(authMutex_);
    auto it = keyspaceOwner_.find(keyspace);
    if (it != keyspaceOwner_.end() && it->second == user.username)
        return true;
    return keyspaceGrants_.find(grantKey(keyspace, user.username)) != keyspaceGrants_.end();
}

std::vector<string> Db::listKeyspacesForUser(const AuthedUser& user) const {
    std::vector<string> out;
    std::shared_lock<std::shared_mutex> lock(authMutex_);
    if (user.level == 0) {
        out = keyspacesCache_;
        return out;
    }
    for (const auto& ks : keyspacesCache_) {
        if (isSystemKeyspace(ks))
            continue;
        auto it = keyspaceOwner_.find(ks);
        bool ok = (it != keyspaceOwner_.end() && it->second == user.username);
        if (!ok) {
            ok = keyspaceGrants_.find(grantKey(ks, user.username)) != keyspaceGrants_.end();
        }
        if (ok)
            out.push_back(ks);
    }
    return out;
}

void Db::onKeyspaceCreated(const string& keyspace) {
    if (!authEnabled())
        return;
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    keyspacesInsertSortedUnlocked(keyspace);
}

void Db::onKeyspaceDropped(const string& keyspace) {
    if (!authEnabled())
        return;
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    keyspacesEraseUnlocked(keyspace);
    keyspaceOwner_.erase(keyspace);
    for (auto it = keyspaceGrants_.begin(); it != keyspaceGrants_.end();) {
        if (it->rfind(keyspace + "#", 0) == 0) {
            it = keyspaceGrants_.erase(it);
        } else {
            ++it;
        }
    }
}

void Db::onSystemUsersPut(const string& username, const string& password, i32 level, bool enabled) {
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    usersPassword_[username] = password;
    usersLevel_[username] = level;
    usersEnabled_[username] = enabled;
}

void Db::onSystemUsersDelete(const string& username) {
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    usersPassword_.erase(username);
    usersLevel_.erase(username);
    usersEnabled_.erase(username);
}

void Db::onSystemKeyspaceOwnersPut(const string& keyspace, const string& ownerUsername) {
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    keyspaceOwner_[keyspace] = ownerUsername;
}

void Db::onSystemKeyspaceOwnersDelete(const string& keyspace) {
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    keyspaceOwner_.erase(keyspace);
}

void Db::onSystemKeyspaceGrantsPut(const string& keyspace, const string& username) {
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    keyspaceGrants_.insert(grantKey(keyspace, username));
}

void Db::onSystemKeyspaceGrantsDelete(const string& keyspace, const string& username) {
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    keyspaceGrants_.erase(grantKey(keyspace, username));
}

void Db::onSystemKeyspaceQuotasPut(const string& keyspace, u64 quotaBytes) {
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    keyspaceQuotaBytes_[keyspace] = quotaBytes;
}

void Db::onSystemKeyspaceQuotasDelete(const string& keyspace) {
    std::unique_lock<std::shared_mutex> lock(authMutex_);
    keyspaceQuotaBytes_.erase(keyspace);
}

std::optional<u64> Db::keyspaceQuotaBytes(const string& keyspace) const {
    std::shared_lock<std::shared_mutex> lock(authMutex_);
    auto it = keyspaceQuotaBytes_.find(keyspace);
    if (it == keyspaceQuotaBytes_.end()) {
        return std::nullopt;
    }
    return it->second;
}

static SqlLiteral litQuoted(const string& s) {
    SqlLiteral l;
    l.kind = SqlLiteral::Kind::Quoted;
    l.text = s;
    return l;
}

static SqlLiteral litNumber(i64 v) {
    SqlLiteral l;
    l.kind = SqlLiteral::Kind::Number;
    l.text = std::to_string(v);
    return l;
}

static SqlLiteral litBool(bool v) {
    SqlLiteral l;
    l.kind = SqlLiteral::Kind::Bool;
    l.text = v ? "true" : "false";
    return l;
}

static string pkText(const byteVec& pkBytes) {
    return string(reinterpret_cast<const char*>(pkBytes.data()), reinterpret_cast<const char*>(pkBytes.data() + pkBytes.size()));
}

static std::optional<string> readTextOrNull(const byteVec& rowBytes, usize& o) {
    if (o >= rowBytes.size())
        throw runtimeError("bad row");
    u8 isNull = rowBytes[o++];
    if (isNull != 0)
        return std::nullopt;
    u32 len = readBeU32(rowBytes, o);
    if (o + len > rowBytes.size())
        throw runtimeError("bad row");
    string s(reinterpret_cast<const char*>(rowBytes.data() + o), reinterpret_cast<const char*>(rowBytes.data() + o + len));
    o += len;
    return s;
}

static std::optional<i32> readI32OrNull(const byteVec& rowBytes, usize& o) {
    if (o >= rowBytes.size())
        throw runtimeError("bad row");
    u8 isNull = rowBytes[o++];
    if (isNull != 0)
        return std::nullopt;
    return readBe32(rowBytes, o);
}

static std::optional<i64> readI64OrNull(const byteVec& rowBytes, usize& o) {
    if (o >= rowBytes.size())
        throw runtimeError("bad row");
    u8 isNull = rowBytes[o++];
    if (isNull != 0)
        return std::nullopt;
    return readBe64(rowBytes, o);
}

static std::optional<bool> readBoolOrNull(const byteVec& rowBytes, usize& o) {
    if (o >= rowBytes.size())
        throw runtimeError("bad row");
    u8 isNull = rowBytes[o++];
    if (isNull != 0)
        return std::nullopt;
    if (o >= rowBytes.size())
        throw runtimeError("bad row");
    return rowBytes[o++] != 0;
}

void Db::bootstrapAuthSystem() {
    if (!authEnabled())
        return;

    {
        std::unique_lock<std::shared_mutex> lock(authMutex_);
        if (authBootstrapped_)
            return;
    }

    createKeyspace("SYSTEM");

    auto makeUsersSchema = []() {
        TableSchema s;
        s.columns = {
                {"username", ColumnType::Text},
                {"password", ColumnType::Text},
                {"level", ColumnType::Int32},
                {"enabled", ColumnType::Boolean},
                {"created_at", ColumnType::Timestamp},
        };
        s.primaryKeyIndex = 0;
        return s;
    };
    auto makeOwnersSchema = []() {
        TableSchema s;
        s.columns = {
                {"keyspace", ColumnType::Text},
                {"owner_username", ColumnType::Text},
                {"created_at", ColumnType::Timestamp},
        };
        s.primaryKeyIndex = 0;
        return s;
    };
    auto makeGrantsSchema = []() {
        TableSchema s;
        s.columns = {
                {"keyspace_username", ColumnType::Text},
                {"created_at", ColumnType::Timestamp},
        };
        s.primaryKeyIndex = 0;
        return s;
    };

    auto makeQuotasSchema = []() {
        TableSchema s;
        s.columns = {
                {"keyspace", ColumnType::Text},
                {"quota_bytes", ColumnType::Int64},
                {"updated_at", ColumnType::Timestamp},
        };
        s.primaryKeyIndex = 0;
        return s;
    };

    auto ensureTable = [this](const string& keyspace, const string& table, const TableSchema& schema) {
        try {
            (void)createTable(keyspace, table, schema);
        } catch (const std::exception& e) {
            if (string(e.what()) != "Table exists")
                throw;
        }
    };

    ensureTable("SYSTEM", "USERS", makeUsersSchema());
    ensureTable("SYSTEM", "KEYSPACE_OWNERS", makeOwnersSchema());
    ensureTable("SYSTEM", "KEYSPACE_GRANTS", makeGrantsSchema());

    if (settings_.quotaEnforcementEnabled) {
        ensureTable("SYSTEM", "KEYSPACE_QUOTAS", makeQuotasSchema());
    }

    auto usersTable = openTable("SYSTEM", "USERS");
    auto ownersTable = openTable("SYSTEM", "KEYSPACE_OWNERS");
    auto grantsTable = openTable("SYSTEM", "KEYSPACE_GRANTS");
    shared_ptr<Table> quotasTable;
    if (settings_.quotaEnforcementEnabled) {
        quotasTable = openTable("SYSTEM", "KEYSPACE_QUOTAS");
    }

    std::vector<string> ksList = listKeyspaces();
    if (std::find(ksList.begin(), ksList.end(), string("SYSTEM")) == ksList.end()) {
        ksList.push_back("SYSTEM");
        std::sort(ksList.begin(), ksList.end());
    }

    std::unordered_map<string, string> usersPass;
    std::unordered_map<string, i32> usersLevel;
    std::unordered_map<string, bool> usersEnabled;
    std::unordered_map<string, string> owners;
    std::unordered_set<string> grants;
    std::unordered_map<string, u64> quotas;

    for (const auto& row : usersTable->scanAllRowsByPk(false)) {
        string username = pkText(row.pkBytes);
        usize o = 0;
        if (readBeU32(row.rowBytes, o) != 1)
            continue;
        auto password = readTextOrNull(row.rowBytes, o);
        auto level = readI32OrNull(row.rowBytes, o);
        auto enabled = readBoolOrNull(row.rowBytes, o);
        (void)readI64OrNull(row.rowBytes, o);
        if (!password.has_value() || !level.has_value() || !enabled.has_value())
            continue;
        usersPass[username] = *password;
        usersLevel[username] = *level;
        usersEnabled[username] = *enabled;
    }

    for (const auto& row : ownersTable->scanAllRowsByPk(false)) {
        string keyspace = pkText(row.pkBytes);
        usize o = 0;
        if (readBeU32(row.rowBytes, o) != 1)
            continue;
        auto owner = readTextOrNull(row.rowBytes, o);
        (void)readI64OrNull(row.rowBytes, o);
        if (!owner.has_value())
            continue;
        owners[keyspace] = *owner;
    }

    for (const auto& row : grantsTable->scanAllRowsByPk(false)) {
        string ksu = pkText(row.pkBytes);
        auto pos = ksu.find('#');
        if (pos == string::npos || pos == 0 || pos + 1 >= ksu.size())
            continue;
        grants.insert(ksu);
    }

    if (settings_.quotaEnforcementEnabled && quotasTable != nullptr) {
        for (const auto& row : quotasTable->scanAllRowsByPk(false)) {
            string keyspace = pkText(row.pkBytes);
            usize o = 0;
            if (readBeU32(row.rowBytes, o) != 1)
                continue;
            auto quota = readI64OrNull(row.rowBytes, o);
            (void)readI64OrNull(row.rowBytes, o);
            if (!quota.has_value())
                continue;
            if (*quota <= 0)
                continue;
            quotas[keyspace] = static_cast<u64>(*quota);
        }
    }

    {
        std::unique_lock<std::shared_mutex> lock(authMutex_);
        usersPassword_ = std::move(usersPass);
        usersLevel_ = std::move(usersLevel);
        usersEnabled_ = std::move(usersEnabled);
        keyspaceOwner_ = std::move(owners);
        keyspaceGrants_ = std::move(grants);
        keyspacesCache_ = ksList;
        keyspaceQuotaBytes_ = std::move(quotas);
    }

    {
        const i64 createdAt = nowMs();
        const string rootUser = settings_.authUsername;
        const string rootPass = settings_.authPassword;
        auto rootLit = litQuoted(rootUser);
        byteVec pkBytes = partitionKeyBytes(ColumnType::Text, rootLit);
        std::vector<string> cols = {"username", "password", "level", "enabled", "created_at"};
        std::vector<SqlLiteral> vals = {rootLit, litQuoted(rootPass), litNumber(0), litBool(true), litNumber(createdAt)};
        byteVec rb = rowBytes(makeUsersSchema(), cols, vals, pkBytes);
        usersTable->putRow(pkBytes, rb);
        onSystemUsersPut(rootUser, rootPass, 0, true);
    }

    {
        const string rootUser = settings_.authUsername;
        const i64 createdAt = nowMs();
        for (const auto& ks : ksList) {
            if (isSystemKeyspace(ks))
                continue;
            bool hasOwner = false;
            {
                std::shared_lock<std::shared_mutex> lock(authMutex_);
                hasOwner = keyspaceOwner_.find(ks) != keyspaceOwner_.end();
            }
            if (hasOwner)
                continue;
            auto ksLit = litQuoted(ks);
            byteVec pkBytes = partitionKeyBytes(ColumnType::Text, ksLit);
            std::vector<string> cols = {"keyspace", "owner_username", "created_at"};
            std::vector<SqlLiteral> vals = {ksLit, litQuoted(rootUser), litNumber(createdAt)};
            byteVec rb = rowBytes(makeOwnersSchema(), cols, vals, pkBytes);
            ownersTable->putRow(pkBytes, rb);
            onSystemKeyspaceOwnersPut(ks, rootUser);
        }
    }

    {
        std::unique_lock<std::shared_mutex> lock(authMutex_);
        authBootstrapped_ = true;
    }
}

void Db::cleanupKeyspaceSecurityMetadata(const string& keyspace) {
    if (!authEnabled())
        return;
    if (isSystemKeyspace(keyspace))
        return;

    auto ownersTable = openTable("SYSTEM", "KEYSPACE_OWNERS");
    auto grantsTable = openTable("SYSTEM", "KEYSPACE_GRANTS");
    shared_ptr<Table> quotasTable;
    if (settings_.quotaEnforcementEnabled) {
        quotasTable = openTable("SYSTEM", "KEYSPACE_QUOTAS");
    }

    {
        auto ksLit = litQuoted(keyspace);
        byteVec pkBytes = partitionKeyBytes(ColumnType::Text, ksLit);
        ownersTable->deleteRow(pkBytes);
        onSystemKeyspaceOwnersDelete(keyspace);
    }

    std::vector<string> toDelete;
    {
        std::shared_lock<std::shared_mutex> lock(authMutex_);
        for (const auto& k : keyspaceGrants_) {
            if (k.rfind(keyspace + "#", 0) == 0)
                toDelete.push_back(k);
        }
    }
    for (const auto& k : toDelete) {
        auto lit = litQuoted(k);
        byteVec pkBytes = partitionKeyBytes(ColumnType::Text, lit);
        grantsTable->deleteRow(pkBytes);
        auto pos = k.find('#');
        if (pos != string::npos) {
            onSystemKeyspaceGrantsDelete(keyspace, k.substr(pos + 1));
        }
    }

    if (quotasTable != nullptr) {
        auto ksLit = litQuoted(keyspace);
        byteVec pkBytes = partitionKeyBytes(ColumnType::Text, ksLit);
        quotasTable->deleteRow(pkBytes);
        onSystemKeyspaceQuotasDelete(keyspace);
    }
}

const path& Db::dataDir() const {
    return effectiveDataDir_;
}

const Settings& Db::settings() const {
    return settings_;
}

void Db::createKeyspace(const string& keyspace) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::filesystem::create_directories(keyspaceDir(effectiveDataDir_, keyspace));
}

path Db::createTable(const string& keyspace, const string& table, const TableSchema& schema) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
    if (authEnabled()) {
        std::error_code ec;
        if (!std::filesystem::exists(ksDir, ec) || ec) {
            throw runtimeError("Keyspace not found");
        }
    } else {
        std::filesystem::create_directories(ksDir);
    }
    auto schemaFile = schemaPath(effectiveDataDir_, keyspace);
    auto existing = findTableUuidFromSchema(schemaFile, table);
    if (existing.has_value()) {
        throw runtimeError("Table exists");
    }
    auto uuid = newUuidHex();
    upsertTableUuidToSchema(schemaFile, table, uuid);
    auto dirPath = tableDir(effectiveDataDir_, keyspace, table, uuid);
    std::filesystem::create_directories(dirPath / "tmp");

    TableSettings ts;
    ts.walFsync = settings_.walFsync;
    ts.walFsyncIntervalMs = settings_.walFsyncIntervalMs;
    ts.walFsyncBytes = settings_.walFsyncBytes;
    ts.memtableMaxBytes = settings_.memtableMaxBytes;
    ts.sstableIndexStride = settings_.sstableIndexStride;
    auto t = std::make_shared<Table>(dirPath, keyspace, table, uuid, schema, ts);
    t->openOrCreateFiles(true);
    t->recover();
    tables_[tableKey(keyspace, table)] = t;
    return dirPath;
}

shared_ptr<Table> Db::openTable(const string& keyspace, const string& table) {
    std::lock_guard<std::mutex> lock(mutex_);
    return openTableUnlocked(keyspace, table);
}

shared_ptr<Table> Db::openTableUnlocked(const string& keyspace, const string& table) {
    auto key = tableKey(keyspace, table);
    auto data = tables_.find(key);
    if (data != tables_.end()) {
        return data->second;
    }

    auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
    if (authEnabled()) {
        std::error_code ec;
        if (!std::filesystem::exists(ksDir, ec) || ec) {
            throw runtimeError("Keyspace not found");
        }
    } else {
        std::filesystem::create_directories(ksDir);
    }
    auto schemaFile = schemaPath(effectiveDataDir_, keyspace);

    auto uuidOpt = findTableUuidFromSchema(schemaFile, table);
    if (!uuidOpt.has_value()) {
        uuidOpt = findTableUuidByScan(ksDir, table);
        if (uuidOpt.has_value()) {
            upsertTableUuidToSchema(schemaFile, table, *uuidOpt);
        }
    }
    if (!uuidOpt.has_value()) {
        throw runtimeError("Table not found");
    }

    auto dirPath = tableDir(effectiveDataDir_, keyspace, table, *uuidOpt);
    auto schema = readSchemaFromMetadata(dirPath);
    TableSettings ts;
    ts.walFsync = settings_.walFsync;
    ts.walFsyncIntervalMs = settings_.walFsyncIntervalMs;
    ts.walFsyncBytes = settings_.walFsyncBytes;
    ts.memtableMaxBytes = settings_.memtableMaxBytes;
    ts.sstableIndexStride = settings_.sstableIndexStride;
    auto tablePtr = std::make_shared<Table>(dirPath, keyspace, table, *uuidOpt, schema, ts);
    tablePtr->openOrCreateFiles(false);
    tablePtr->recover();
    tables_[key] = tablePtr;
    return tablePtr;
}

static bool isValidIdent(const string& s) {
    if (s.empty())
        return false;
    auto isIdentChar = [](char c) {
        return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
    };
    if (!isIdentChar(s[0]) || std::isdigit(static_cast<unsigned char>(s[0])))
        return false;
    for (char c : s) {
        if (!isIdentChar(c))
            return false;
    }
    return true;
}

std::vector<string> Db::listKeyspaces() const {
    std::vector<string> out;
    std::error_code ec;
    if (!std::filesystem::exists(effectiveDataDir_, ec))
        return out;

    for (const auto& entry : std::filesystem::directory_iterator(effectiveDataDir_, ec)) {
        if (ec)
            break;
        if (!entry.is_directory())
            continue;
        auto name = entry.path().filename().string();
        if (!isValidIdent(name))
            continue;
        out.push_back(name);
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<string> Db::listTables(const string& keyspace) const {
    std::vector<string> out;
    if (!isValidIdent(keyspace))
        return out;
    std::error_code ec;
    auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
    if (!std::filesystem::exists(ksDir, ec))
        return out;

    std::set<string> uniq;
    for (const auto& entry : std::filesystem::directory_iterator(ksDir, ec)) {
        if (ec)
            break;
        if (!entry.is_directory())
            continue;
        auto name = entry.path().filename().string();
        auto pos = name.rfind('-');
        if (pos == string::npos || pos == 0)
            continue;
        auto table = name.substr(0, pos);
        if (!isValidIdent(table))
            continue;
        uniq.insert(table);
    }

    out.assign(uniq.begin(), uniq.end());
    return out;
}

void Db::dropTable(const string& keyspace, const string& table, bool ifExists) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto schemaFile = schemaPath(effectiveDataDir_, keyspace);
    auto uuidOpt = findTableUuidFromSchema(schemaFile, table);
    if (!uuidOpt.has_value()) {
        auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
        uuidOpt = findTableUuidByScan(ksDir, table);
    }
    if (!uuidOpt.has_value()) {
        if (ifExists)
            return;
        throw runtimeError("Table not found");
    }

    auto key = tableKey(keyspace, table);
    auto it = tables_.find(key);
    if (it != tables_.end()) {
        it->second->shutdown();
        tables_.erase(it);
    }

    (void)removeTableFromSchema(schemaFile, table);

    std::error_code ec;
    auto dirPath = tableDir(effectiveDataDir_, keyspace, table, *uuidOpt);
    std::filesystem::remove_all(dirPath, ec);
    if (ec && !ifExists) {
        throw runtimeError("drop failed");
    }
}

void Db::dropKeyspace(const string& keyspace, bool ifExists) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::error_code ec;
    auto ksDir = keyspaceDir(effectiveDataDir_, keyspace);
    if (!std::filesystem::exists(ksDir, ec)) {
        if (ifExists)
            return;
        throw runtimeError("Keyspace not found");
    }

    auto prefix = keyspace + ".";
    std::vector<string> toErase;
    toErase.reserve(tables_.size());
    for (auto& kv : tables_) {
        if (kv.first.rfind(prefix, 0) == 0) {
            kv.second->shutdown();
            toErase.push_back(kv.first);
        }
    }
    for (const auto& k : toErase) {
        tables_.erase(k);
    }

    std::filesystem::remove_all(ksDir, ec);
    if (ec && !ifExists) {
        throw runtimeError("drop failed");
    }
}

void Db::truncateTable(const string& keyspace, const string& table) {
    auto t = openTable(keyspace, table);
    t->truncate();
}

}
