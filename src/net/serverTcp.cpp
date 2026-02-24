#include "net/serverTcp.h"

#include "util/json.h"
#include "query/sql.h"

#include "core/paths.h"

#include "util/log.h"

#include <cerrno>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <chrono>
#include <system_error>
#include <string>
#include <thread>
#include <functional>
#include <mutex>
#include <unordered_map>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

using std::string;

namespace xeondb {

static bool isSystemKeyspaceName(const string& keyspace) {
    if (keyspace.size() != 6)
        return false;
    return (std::tolower(static_cast<unsigned char>(keyspace[0])) == 's') && (std::tolower(static_cast<unsigned char>(keyspace[1])) == 'y') &&
           (std::tolower(static_cast<unsigned char>(keyspace[2])) == 's') && (std::tolower(static_cast<unsigned char>(keyspace[3])) == 't') &&
           (std::tolower(static_cast<unsigned char>(keyspace[4])) == 'e') && (std::tolower(static_cast<unsigned char>(keyspace[5])) == 'm');
}

static i64 nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
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

static u64 dirBytesUsedBestEffort(const path& root) {
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

    const u64 fresh = dirBytesUsedBestEffort(keyspaceDir(db_->dataDir(), keyspace));

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
        const u64 fresh = dirBytesUsedBestEffort(keyspaceDir(db_->dataDir(), keyspace));
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

ServerTcp::ServerTcp(std::shared_ptr<Db> db, string host, u16 port, usize maxLineBytes, usize maxConnections, string authUsername, string authPassword)
    : db_(std::move(db))
    , host_(std::move(host))
    , port_(port)
    , maxLineBytes_(maxLineBytes)
    , maxConnections_(maxConnections)
    , authUsername_(std::move(authUsername))
    , authPassword_(std::move(authPassword))
    , authEnabled_(db_ != nullptr ? db_->authEnabled() : (!authUsername_.empty() && !authPassword_.empty()))
    , connectionCount_(0) {
}

ServerTcp::~ServerTcp() {
}

static bool sendAll(int fd, const string& s) {
    const char* p = s.data();
    usize i = s.size();
    usize j = 0;
    while (j < i) {
        ssize_t sentBytes = ::send(fd, p + j, i - j, 0);
        if (sentBytes < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        j += static_cast<usize>(sentBytes);
    }
    return true;
}

static runtimeError errnoError(const string& prefix) {
    return runtimeError(prefix + " errno=" + std::to_string(errno) + " err=" + string(::strerror(errno)));
}

static bool schemaEquals(const TableSchema& schema, const TableSchema& schemaB) {
    if (schema.primaryKeyIndex != schemaB.primaryKeyIndex)
        return false;
    if (schema.columns.size() != schemaB.columns.size())
        return false;
    for (usize i = 0; i < schema.columns.size(); i++) {
        if (schema.columns[i].name != schemaB.columns[i].name)
            return false;
        if (schema.columns[i].type != schemaB.columns[i].type)
            return false;
    }
    return true;
}

void ServerTcp::run() {
    int socketFileDesc = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socketFileDesc < 0)
        throw errnoError("socket failed");
    int addrFlag = 1;
    ::setsockopt(socketFileDesc, SOL_SOCKET, SO_REUSEADDR, &addrFlag, sizeof(addrFlag));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) {
        ::close(socketFileDesc);
        throw runtimeError("bad host");
    }

    if (::bind(socketFileDesc, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(socketFileDesc);
        throw errnoError("bind failed");
    }
    if (::listen(socketFileDesc, 128) != 0) {
        ::close(socketFileDesc);
        throw errnoError("listen failed");
    }

    xeondb::log(xeondb::LogLevel::INFO, std::string("Listening host=") + host_ + " port=" + std::to_string(port_) +
                                                " maxLineBytes=" + std::to_string(maxLineBytes_) + " maxConnections=" + std::to_string(maxConnections_) +
                                                " auth=" + (authEnabled_ ? "enabled" : "disabled"));

    if (db_ != nullptr) {
        auto db = db_;
        std::thread sampler([db]() {
            using namespace std::chrono_literals;
            for (;;) {
                std::this_thread::sleep_for(30s);
                try {
                    db->metricsSampleAll();
                } catch (...) {
                    // ignore
                }
            }
        });
        sampler.detach();
    }

    for (;;) {
        int clientSocketDesc = ::accept(socketFileDesc, nullptr, nullptr);
        if (clientSocketDesc < 0) {
            if (errno == EINTR)
                continue;
            continue;
        }
        if (connectionCount_.load() >= maxConnections_) {
            sendAll(clientSocketDesc, jsonError("too_many_connections") + "\n");
            ::close(clientSocketDesc);
            continue;
        }
        connectionCount_++;
        std::thread t([this, clientSocketDesc]() {
            handleClient(clientSocketDesc);
            ::close(clientSocketDesc);
            connectionCount_--;
        });
        t.detach();
    }
}

void ServerTcp::handleClient(int clientFd) {
    string buf;
    buf.reserve(4096);
    char tmp[4096];

    string currentKeyspace;
    std::optional<AuthedUser> currentUser;
    AuthedUser noAuthRoot{"", 0};

    struct ScopeExit {
        std::function<void()> fn;
        ~ScopeExit() {
            if (fn)
                fn();
        }
    } onExit;

    onExit.fn = [this, &currentKeyspace]() {
        try {
            if (db_ != nullptr && !currentKeyspace.empty()) {
                db_->metricsOnDisconnect(currentKeyspace);
            }
        } catch (...) {
            // ignore
        }
    };

    while (true) {
        ssize_t recieved = ::recv(clientFd, tmp, sizeof(tmp), 0);
        if (recieved == 0)
            return;
        if (recieved < 0) {
            if (errno == EINTR)
                continue;
            return;
        }
        buf.append(tmp, tmp + recieved);

        while (true) {
            auto newl = buf.find('\n');
            if (newl == string::npos) {
                if (buf.size() > maxLineBytes_) {
                    sendAll(clientFd, jsonError("line_too_large") + "\n");
                    return;
                }
                break;
            }
            string line = buf.substr(0, newl);
            buf.erase(0, newl + 1);
            if (!line.empty() && line.back() == '\r')
                line.pop_back();
            if (line.empty())
                continue;

            if (authEnabled_ && !currentUser.has_value()) {
                usize k = 0;
                while (k < line.size() && std::isspace(static_cast<unsigned char>(line[k])))
                    k++;
                auto eq = [](char a, char b) {
                    return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
                };
                const bool isAuth = (k + 4 <= line.size()) && eq(line[k + 0], 'a') && eq(line[k + 1], 'u') && eq(line[k + 2], 't') && eq(line[k + 3], 'h') &&
                                    (k + 4 == line.size() || std::isspace(static_cast<unsigned char>(line[k + 4])));
                if (!isAuth) {
                    sendAll(clientFd, jsonError("unauthorized") + "\n");
                    continue;
                }
            }

            string parseError;
            auto cmdOpt = sqlCommand(line, parseError);
            if (!cmdOpt.has_value()) {
                sendAll(clientFd, jsonError(parseError) + "\n");
                continue;
            }

            string response;
            try {
                auto& cmd = *cmdOpt;
                if (auto* auth = std::get_if<SqlAuth>(&cmd)) {
                    if (!authEnabled_) {
                        response = jsonOk();
                    } else {
                        auto u = db_->authenticate(auth->username, auth->password);
                        if (!u.has_value()) {
                            response = jsonError("bad_auth");
                        } else {
                            currentUser = *u;
                            response = jsonOk();
                        }
                    }
                } else if (authEnabled_ && !currentUser.has_value()) {
                    response = jsonError("unauthorized");
                } else if (std::holds_alternative<SqlPing>(cmd)) {
                    response = jsonString("result", "PONG");
                } else if (auto* use = std::get_if<SqlUse>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    if (authEnabled_ && !db_->canAccessKeyspace(u, use->keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnUse(currentKeyspace, use->keyspace);
                    }
                    currentKeyspace = use->keyspace;
                    response = jsonOk();
                } else if (auto* createKeyspace = std::get_if<SqlCreateKeyspace>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    if (authEnabled_ && !db_->canCreateOrDropKeyspace(u))
                        throw runtimeError("forbidden");
                    if (isSystemKeyspaceName(createKeyspace->keyspace))
                        throw runtimeError("cannot create SYSTEM");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(createKeyspace->keyspace);
                    }

                    std::error_code ec;
                    const bool existed = std::filesystem::exists(keyspaceDir(db_->dataDir(), createKeyspace->keyspace), ec) && !ec;
                    db_->createKeyspace(createKeyspace->keyspace);
                    if (authEnabled_) {
                        db_->onKeyspaceCreated(createKeyspace->keyspace);
                        if (!existed) {
                            auto ownersTable = db_->openTable("SYSTEM", "KEYSPACE_OWNERS");
                            const i64 createdAt = nowMs();
                            auto ksLit = litQuoted(createKeyspace->keyspace);
                            byteVec pkBytes = partitionKeyBytes(ColumnType::Text, ksLit);
                            std::vector<string> cols = {"keyspace", "owner_username", "created_at"};
                            std::vector<SqlLiteral> vals = {ksLit, litQuoted(u.username), litNumber(createdAt)};
                            TableSchema schema;
                            schema.columns = {{"keyspace", ColumnType::Text}, {"owner_username", ColumnType::Text}, {"created_at", ColumnType::Timestamp}};
                            schema.primaryKeyIndex = 0;
                            byteVec rb = rowBytes(schema, cols, vals, pkBytes);
                            ownersTable->putRow(pkBytes, rb);
                            db_->onSystemKeyspaceOwnersPut(createKeyspace->keyspace, u.username);
                        }
                    }
                    response = jsonOk();
                } else if (auto* createTable = std::get_if<SqlCreateTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = createTable->keyspace.empty() ? currentKeyspace : createTable->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }

                    if (auto quota = quotaBytesForKeyspace(keyspace); quota.has_value() && *quota > 0) {
                        constexpr u64 estCreateTableBytes = 16ull * 1024ull;
                        if (!quotaWouldAllowAndReserve(keyspace, *quota, estCreateTableBytes))
                            throw runtimeError("quota_exceeded");
                    }

                    if (!createTable->ifNotExists) {
                        (void)db_->createTable(keyspace, createTable->table, createTable->schema);
                    } else {
                        try {
                            (void)db_->createTable(keyspace, createTable->table, createTable->schema);
                        } catch (const std::exception& e) {
                            if (string(e.what()) == "Table exists") {
                                auto t = db_->openTable(keyspace, createTable->table);
                                if (!schemaEquals(t->schema(), createTable->schema))
                                    throw runtimeError("Schema mismatch");
                            } else {
                                throw;
                            }
                        }
                    }

                    response = jsonOk();
                } else if (auto* dropTable = std::get_if<SqlDropTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = dropTable->keyspace.empty() ? currentKeyspace : dropTable->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }
                    db_->dropTable(keyspace, dropTable->table, dropTable->ifExists);
                    invalidateBytesUsedCache(keyspace);
                    response = jsonOk();
                } else if (auto* dropKeyspace = std::get_if<SqlDropKeyspace>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    if (authEnabled_ && !db_->canCreateOrDropKeyspace(u))
                        throw runtimeError("forbidden");
                    if (isSystemKeyspaceName(dropKeyspace->keyspace))
                        throw runtimeError("cannot drop SYSTEM");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(dropKeyspace->keyspace);
                    }
                    db_->dropKeyspace(dropKeyspace->keyspace, dropKeyspace->ifExists);
                    invalidateBytesUsedCache(dropKeyspace->keyspace);
                    if (authEnabled_) {
                        db_->cleanupKeyspaceSecurityMetadata(dropKeyspace->keyspace);
                        db_->onKeyspaceDropped(dropKeyspace->keyspace);
                    }
                    if (currentKeyspace == dropKeyspace->keyspace)
                        currentKeyspace.clear();
                    response = jsonOk();
                } else if (std::holds_alternative<SqlShowKeyspaces>(cmd)) {
                    std::vector<string> keyspaces;
                    if (authEnabled_) {
                        const AuthedUser& u = *currentUser;
                        keyspaces = db_->listKeyspacesForUser(u);
                    } else {
                        keyspaces = db_->listKeyspaces();
                    }
                    string out = "{\"ok\":true,\"keyspaces\":[";
                    for (usize i = 0; i < keyspaces.size(); i++) {
                        if (i)
                            out += ",";
                        out += "\"" + jsonEscape(keyspaces[i]) + "\"";
                    }
                    out += "]}";
                    response = out;
                } else if (auto* showTables = std::get_if<SqlShowTables>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    string keyspace;
                    if (showTables->inKeyspace.has_value()) {
                        keyspace = *showTables->inKeyspace;
                    } else {
                        keyspace = currentKeyspace;
                    }
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }
                    auto tables = db_->listTables(keyspace);
                    string out = "{\"ok\":true,\"tables\":[";
                    for (usize i = 0; i < tables.size(); i++) {
                        if (i)
                            out += ",";
                        out += "\"" + jsonEscape(tables[i]) + "\"";
                    }
                    out += "]}";
                    response = out;
                } else if (auto* describe = std::get_if<SqlDescribeTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = describe->keyspace.empty() ? currentKeyspace : describe->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }
                    auto t = db_->openTable(keyspace, describe->table);
                    const auto& schema = t->schema();
                    string out = "{\"ok\":true,\"keyspace\":\"" + jsonEscape(keyspace) + "\",\"table\":\"" + jsonEscape(describe->table) + "\",";
                    auto pkName = schema.columns[schema.primaryKeyIndex].name;
                    out += "\"primaryKey\":\"" + jsonEscape(pkName) + "\",\"columns\":[";
                    for (usize c = 0; c < schema.columns.size(); c++) {
                        if (c)
                            out += ",";
                        out += "{\"name\":\"" + jsonEscape(schema.columns[c].name) + "\",\"type\":\"" + jsonEscape(columnTypeName(schema.columns[c].type)) +
                               "\"}";
                    }
                    out += "]}";
                    response = out;
                } else if (auto* showCreate = std::get_if<SqlShowCreateTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = showCreate->keyspace.empty() ? currentKeyspace : showCreate->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }
                    auto t = db_->openTable(keyspace, showCreate->table);
                    const auto& schema = t->schema();
                    auto pkName = schema.columns[schema.primaryKeyIndex].name;
                    string stmt = "CREATE TABLE " + keyspace + "." + showCreate->table + " (";
                    for (usize c = 0; c < schema.columns.size(); c++) {
                        if (c)
                            stmt += ", ";
                        stmt += schema.columns[c].name + " " + columnTypeName(schema.columns[c].type);
                    }
                    stmt += ", PRIMARY KEY (" + pkName + "));";
                    response = string("{\"ok\":true,\"create\":\"") + jsonEscape(stmt) + "\"}";
                } else if (auto* showMetrics = std::get_if<SqlShowMetrics>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    const string& ks = showMetrics->keyspace;
                    if (authEnabled_ && !db_->canAccessKeyspace(u, ks))
                        throw runtimeError("forbidden");

                    auto m = db_->keyspaceMetrics(ks);
                    string out = string("{\"ok\":true,\"keyspace\":\"") + jsonEscape(ks) + "\"";
                    out += string(",\"connections_active\":") + std::to_string(m.connectionsActive);

                    out += ",\"connections_last24h_peak_4h\":[";
                    for (usize i = 0; i < m.connectionsLast24hPeak4h.size(); i++) {
                        if (i)
                            out += ",";
                        out += std::to_string(m.connectionsLast24hPeak4h[i]);
                    }
                    out += "]";

                    out += ",\"queries_last24h_4h\":[";
                    for (usize i = 0; i < m.queriesLast24h4h.size(); i++) {
                        if (i)
                            out += ",";
                        out += std::to_string(m.queriesLast24h4h[i]);
                    }
                    out += "]";

                    out += string(",\"queries_last24h_total\":") + std::to_string(m.queriesLast24hTotal);

                    const u64 bytesUsed = bytesUsedForKeyspaceCached(ks);
                    out += string(",\"bytes_used\":") + std::to_string(bytesUsed);

                    if (auto quota = quotaBytesForKeyspace(ks); quota.has_value() && *quota > 0) {
                        out += string(",\"quota_bytes\":") + std::to_string(*quota);
                        out += string(",\"over_quota\":") + ((bytesUsed >= *quota) ? string("true") : string("false"));
                    }

                    out += ",\"labels_last24h_4h\":[";
                    for (usize i = 0; i < m.labelsLast24h4h.size(); i++) {
                        if (i)
                            out += ",";
                        out += string("\"") + jsonEscape(m.labelsLast24h4h[i]) + "\"";
                    }
                    out += "]";
                    out += "}";
                    response = out;
                } else if (auto* trunc = std::get_if<SqlTruncateTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = trunc->keyspace.empty() ? currentKeyspace : trunc->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }
                    db_->truncateTable(keyspace, trunc->table);
                    invalidateBytesUsedCache(keyspace);
                    response = jsonOk();
                } else if (auto* insert = std::get_if<SqlInsert>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = insert->keyspace.empty() ? currentKeyspace : insert->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }

                    auto retTable = db_->openTable(keyspace, insert->table);
                    auto pkIndex = retTable->schema().primaryKeyIndex;
                    auto pkName = retTable->schema().columns[pkIndex].name;

                    std::optional<usize> pkPos;
                    for (usize i = 0; i < insert->columns.size(); i++) {
                        if (insert->columns[i] == pkName)
                            pkPos = i;
                    }
                    if (!pkPos.has_value())
                        throw runtimeError("Missing pk");

                    std::vector<std::pair<byteVec, byteVec>> prepared;
                    prepared.reserve(insert->rows.size());
                    u64 estimatedWriteBytes = 0;
                    for (const auto& row : insert->rows) {
                        auto pkLit = row[*pkPos];
                        byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, pkLit);
                        byteVec rowBytesBuf = rowBytes(retTable->schema(), insert->columns, row, pkBytes);
                        estimatedWriteBytes += static_cast<u64>(pkBytes.size());
                        estimatedWriteBytes += static_cast<u64>(rowBytesBuf.size());
                        estimatedWriteBytes += 64; // small WAL/metadata overhead estimate
                        prepared.push_back({std::move(pkBytes), std::move(rowBytesBuf)});
                    }

                    if (auto quota = quotaBytesForKeyspace(keyspace); quota.has_value() && *quota > 0) {
                        if (!quotaWouldAllowAndReserve(keyspace, *quota, estimatedWriteBytes))
                            throw runtimeError("quota_exceeded");
                    }

                    for (const auto& r : prepared) {
                        retTable->putRow(r.first, r.second);
                    }

                    if (authEnabled_ && isSystemKeyspaceName(keyspace)) {
                        if (insert->table == "USERS") {
                            for (const auto& row : insert->rows) {
                                std::optional<string> username;
                                std::optional<string> password;
                                std::optional<i32> level;
                                std::optional<bool> enabled;
                                for (usize i = 0; i < insert->columns.size(); i++) {
                                    const auto& c = insert->columns[i];
                                    const auto& v = row[i];
                                    if (c == "username" && v.kind == SqlLiteral::Kind::Quoted)
                                        username = v.text;
                                    else if (c == "password" && v.kind == SqlLiteral::Kind::Quoted)
                                        password = v.text;
                                    else if (c == "level" && v.kind == SqlLiteral::Kind::Number)
                                        level = static_cast<i32>(std::stoll(v.text));
                                    else if (c == "enabled" && v.kind == SqlLiteral::Kind::Bool)
                                        enabled = (v.text == "true");
                                }
                                if (username.has_value() && password.has_value() && level.has_value() && enabled.has_value()) {
                                    db_->onSystemUsersPut(*username, *password, *level, *enabled);
                                }
                            }
                        } else if (insert->table == "KEYSPACE_OWNERS") {
                            for (const auto& row : insert->rows) {
                                std::optional<string> ks;
                                std::optional<string> owner;
                                for (usize i = 0; i < insert->columns.size(); i++) {
                                    const auto& c = insert->columns[i];
                                    const auto& v = row[i];
                                    if (c == "keyspace" && v.kind == SqlLiteral::Kind::Quoted)
                                        ks = v.text;
                                    else if (c == "owner_username" && v.kind == SqlLiteral::Kind::Quoted)
                                        owner = v.text;
                                }
                                if (ks.has_value() && owner.has_value()) {
                                    db_->onSystemKeyspaceOwnersPut(*ks, *owner);
                                }
                            }
                        } else if (insert->table == "KEYSPACE_GRANTS") {
                            for (const auto& row : insert->rows) {
                                for (usize i = 0; i < insert->columns.size(); i++) {
                                    if (insert->columns[i] != "keyspace_username")
                                        continue;
                                    const auto& v = row[i];
                                    if (v.kind != SqlLiteral::Kind::Quoted)
                                        continue;
                                    auto pos = v.text.find('#');
                                    if (pos == string::npos || pos == 0 || pos + 1 >= v.text.size())
                                        continue;
                                    db_->onSystemKeyspaceGrantsPut(v.text.substr(0, pos), v.text.substr(pos + 1));
                                }
                            }
                        } else if (insert->table == "KEYSPACE_QUOTAS") {
                            for (const auto& row : insert->rows) {
                                std::optional<string> ks;
                                std::optional<i64> quotaBytes;
                                for (usize i = 0; i < insert->columns.size(); i++) {
                                    const auto& c = insert->columns[i];
                                    const auto& v = row[i];
                                    if (c == "keyspace" && v.kind == SqlLiteral::Kind::Quoted)
                                        ks = v.text;
                                    else if (c == "quota_bytes" && v.kind == SqlLiteral::Kind::Number)
                                        quotaBytes = std::stoll(v.text);
                                }
                                if (ks.has_value() && quotaBytes.has_value() && *quotaBytes > 0) {
                                    db_->onSystemKeyspaceQuotasPut(*ks, static_cast<u64>(*quotaBytes));
                                }
                            }
                        }
                    }
                    response = jsonOk();
                } else if (auto* select = std::get_if<SqlSelect>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = select->keyspace.empty() ? currentKeyspace : select->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }

                    auto retTable = db_->openTable(keyspace, select->table);
                    auto pkIndex = retTable->schema().primaryKeyIndex;
                    auto pkName = retTable->schema().columns[pkIndex].name;

                    if (select->whereColumn.has_value()) {
                        if (!select->whereValue.has_value())
                            throw runtimeError("Expected where value");
                        if (*select->whereColumn != pkName)
                            throw runtimeError("Where must use primary key");
                        byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, *select->whereValue);
                        auto rowBytes = retTable->getRow(pkBytes);
                        if (!rowBytes.has_value()) {
                            response = string("{\"ok\":true,\"found\":false}");
                        } else {
                            string rowJson = rowToJson(retTable->schema(), pkBytes, *rowBytes, select->columns);
                            response = string("{\"ok\":true,\"found\":true,\"row\":") + rowJson + "}";
                        }
                    } else {
                        if (select->orderByColumn.has_value() && *select->orderByColumn != pkName)
                            throw runtimeError("ORDER BY must use primary key");

                        bool desc = select->orderByColumn.has_value() ? select->orderDesc : false;
                        auto rows = retTable->scanAllRowsByPk(desc);
                        string out = "{\"ok\":true,\"rows\":[";
                        bool first = true;
                        for (const auto& r : rows) {
                            if (!first)
                                out += ",";
                            first = false;
                            out += rowToJson(retTable->schema(), r.pkBytes, r.rowBytes, select->columns);
                        }
                        out += "]}";
                        response = out;
                    }
                } else if (auto* flush = std::get_if<SqlFlush>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = flush->keyspace.empty() ? currentKeyspace : flush->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }

                    if (auto quota = quotaBytesForKeyspace(keyspace); quota.has_value() && *quota > 0) {
                        const u64 used = bytesUsedForKeyspaceCached(keyspace);
                        if (used >= *quota)
                            throw runtimeError("quota_exceeded");
                    }

                    auto retTable = db_->openTable(keyspace, flush->table);
                    retTable->flush();
                    invalidateBytesUsedCache(keyspace);
                    response = jsonOk();
                } else if (auto* del = std::get_if<SqlDelete>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = del->keyspace.empty() ? currentKeyspace : del->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }

                    auto retTable = db_->openTable(keyspace, del->table);
                    auto pkIndex = retTable->schema().primaryKeyIndex;
                    auto pkName = retTable->schema().columns[pkIndex].name;
                    if (del->whereColumn != pkName)
                        throw runtimeError("Where must use primary key");
                    byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, del->whereValue);
                    retTable->deleteRow(pkBytes);

                    if (authEnabled_ && isSystemKeyspaceName(keyspace)) {
                        if (del->table == "USERS" && del->whereValue.kind == SqlLiteral::Kind::Quoted) {
                            db_->onSystemUsersDelete(del->whereValue.text);
                        } else if (del->table == "KEYSPACE_OWNERS" && del->whereValue.kind == SqlLiteral::Kind::Quoted) {
                            db_->onSystemKeyspaceOwnersDelete(del->whereValue.text);
                        } else if (del->table == "KEYSPACE_GRANTS" && del->whereValue.kind == SqlLiteral::Kind::Quoted) {
                            auto pos = del->whereValue.text.find('#');
                            if (pos != string::npos)
                                db_->onSystemKeyspaceGrantsDelete(del->whereValue.text.substr(0, pos), del->whereValue.text.substr(pos + 1));
                        } else if (del->table == "KEYSPACE_QUOTAS" && del->whereValue.kind == SqlLiteral::Kind::Quoted) {
                            db_->onSystemKeyspaceQuotasDelete(del->whereValue.text);
                        }
                    }
                    response = jsonOk();
                } else if (auto* upd = std::get_if<SqlUpdate>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    auto keyspace = upd->keyspace.empty() ? currentKeyspace : upd->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    if (authEnabled_ && !db_->canAccessKeyspace(u, keyspace))
                        throw runtimeError("forbidden");

                    if (db_ != nullptr) {
                        db_->metricsOnCommand(keyspace);
                    }

                    auto retTable = db_->openTable(keyspace, upd->table);
                    auto pkIndex = retTable->schema().primaryKeyIndex;
                    auto pkName = retTable->schema().columns[pkIndex].name;
                    if (upd->whereColumn != pkName)
                        throw runtimeError("Where must use primary key");

                    if (upd->setColumns.size() != upd->setValues.size())
                        throw runtimeError("set column/value count");

                    for (const auto& c : upd->setColumns) {
                        if (c == pkName)
                            throw runtimeError("cannot update pk");
                    }

                    byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, upd->whereValue);
                    auto existing = retTable->getRow(pkBytes);
                    byteVec newRowBytes = mergeRowBytesForUpdate(retTable->schema(), existing, upd->setColumns, upd->setValues);

                    if (auto quota = quotaBytesForKeyspace(keyspace); quota.has_value() && *quota > 0) {
                        u64 est = static_cast<u64>(pkBytes.size()) + static_cast<u64>(newRowBytes.size()) + 64;
                        if (!quotaWouldAllowAndReserve(keyspace, *quota, est))
                            throw runtimeError("quota_exceeded");
                    }
                    retTable->putRow(pkBytes, newRowBytes);

                    if (authEnabled_ && isSystemKeyspaceName(keyspace)) {
                        if (upd->whereValue.kind == SqlLiteral::Kind::Quoted && upd->table == "USERS") {
                            std::optional<string> password;
                            std::optional<i32> level;
                            std::optional<bool> enabled;
                            for (usize i = 0; i < upd->setColumns.size(); i++) {
                                const auto& c = upd->setColumns[i];
                                const auto& v = upd->setValues[i];
                                if (c == "password" && v.kind == SqlLiteral::Kind::Quoted)
                                    password = v.text;
                                else if (c == "level" && v.kind == SqlLiteral::Kind::Number)
                                    level = static_cast<i32>(std::stoll(v.text));
                                else if (c == "enabled" && v.kind == SqlLiteral::Kind::Bool)
                                    enabled = (v.text == "true");
                            }
                            if (password.has_value() && level.has_value() && enabled.has_value()) {
                                db_->onSystemUsersPut(upd->whereValue.text, *password, *level, *enabled);
                            }
                        } else if (upd->whereValue.kind == SqlLiteral::Kind::Quoted && upd->table == "KEYSPACE_OWNERS") {
                            for (usize i = 0; i < upd->setColumns.size(); i++) {
                                if (upd->setColumns[i] == "owner_username" && upd->setValues[i].kind == SqlLiteral::Kind::Quoted) {
                                    db_->onSystemKeyspaceOwnersPut(upd->whereValue.text, upd->setValues[i].text);
                                }
                            }
                        } else if (upd->whereValue.kind == SqlLiteral::Kind::Quoted && upd->table == "KEYSPACE_QUOTAS") {
                            for (usize i = 0; i < upd->setColumns.size(); i++) {
                                if (upd->setColumns[i] == "quota_bytes" && upd->setValues[i].kind == SqlLiteral::Kind::Number) {
                                    i64 q = std::stoll(upd->setValues[i].text);
                                    if (q > 0) {
                                        db_->onSystemKeyspaceQuotasPut(upd->whereValue.text, static_cast<u64>(q));
                                    } else {
                                        db_->onSystemKeyspaceQuotasDelete(upd->whereValue.text);
                                    }
                                }
                            }
                        }
                    }
                    response = jsonOk();
                } else {
                    response = jsonError("Unsupported command");
                }
            } catch (const std::exception& e) {
                response = jsonError(e.what());
            }

            sendAll(clientFd, response + "\n");
        }
    }
}

}
