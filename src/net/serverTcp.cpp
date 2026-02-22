#include "net/serverTcp.h"

#include "util/json.h"
#include "query/sql.h"

#include "util/log.h"

#include <cerrno>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

using std::string;

namespace xeondb {

ServerTcp::ServerTcp(std::shared_ptr<Db> db, string host, u16 port, usize maxLineBytes, usize maxConnections, string authUsername, string authPassword)
    : db_(std::move(db))
    , host_(std::move(host))
    , port_(port)
    , maxLineBytes_(maxLineBytes)
    , maxConnections_(maxConnections)
    , authUsername_(std::move(authUsername))
    , authPassword_(std::move(authPassword))
    , authEnabled_(!authUsername_.empty() && !authPassword_.empty())
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
    bool authed = !authEnabled_;

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

            if (authEnabled_ && !authed) {
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
                    } else if (auth->username == authUsername_ && auth->password == authPassword_) {
                        authed = true;
                        response = jsonOk();
                    } else {
                        response = jsonError("bad_auth");
                    }
                } else if (!authed) {
                    response = jsonError("unauthorized");
                } else if (std::holds_alternative<SqlPing>(cmd)) {
                    response = jsonString("result", "PONG");
                } else if (auto* use = std::get_if<SqlUse>(&cmd)) {
                    currentKeyspace = use->keyspace;
                    response = jsonOk();
                } else if (auto* createKeyspace = std::get_if<SqlCreateKeyspace>(&cmd)) {
                    db_->createKeyspace(createKeyspace->keyspace);
                    response = jsonOk();
                } else if (auto* createTable = std::get_if<SqlCreateTable>(&cmd)) {
                    auto keyspace = createTable->keyspace.empty() ? currentKeyspace : createTable->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");

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
                    auto keyspace = dropTable->keyspace.empty() ? currentKeyspace : dropTable->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    db_->dropTable(keyspace, dropTable->table, dropTable->ifExists);
                    response = jsonOk();
                } else if (auto* dropKeyspace = std::get_if<SqlDropKeyspace>(&cmd)) {
                    db_->dropKeyspace(dropKeyspace->keyspace, dropKeyspace->ifExists);
                    if (currentKeyspace == dropKeyspace->keyspace)
                        currentKeyspace.clear();
                    response = jsonOk();
                } else if (std::holds_alternative<SqlShowKeyspaces>(cmd)) {
                    auto keyspaces = db_->listKeyspaces();
                    string out = "{\"ok\":true,\"keyspaces\":[";
                    for (usize i = 0; i < keyspaces.size(); i++) {
                        if (i)
                            out += ",";
                        out += "\"" + jsonEscape(keyspaces[i]) + "\"";
                    }
                    out += "]}";
                    response = out;
                } else if (auto* showTables = std::get_if<SqlShowTables>(&cmd)) {
                    string keyspace;
                    if (showTables->inKeyspace.has_value()) {
                        keyspace = *showTables->inKeyspace;
                    } else {
                        keyspace = currentKeyspace;
                    }
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
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
                    auto keyspace = describe->keyspace.empty() ? currentKeyspace : describe->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
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
                    auto keyspace = showCreate->keyspace.empty() ? currentKeyspace : showCreate->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
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
                } else if (auto* trunc = std::get_if<SqlTruncateTable>(&cmd)) {
                    auto keyspace = trunc->keyspace.empty() ? currentKeyspace : trunc->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");
                    db_->truncateTable(keyspace, trunc->table);
                    response = jsonOk();
                } else if (auto* insert = std::get_if<SqlInsert>(&cmd)) {
                    auto keyspace = insert->keyspace.empty() ? currentKeyspace : insert->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");

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

                    for (const auto& row : insert->rows) {
                        auto pkLit = row[*pkPos];
                        byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, pkLit);
                        byteVec rowBytesBuf = rowBytes(retTable->schema(), insert->columns, row, pkBytes);
                        retTable->putRow(pkBytes, rowBytesBuf);
                    }
                    response = jsonOk();
                } else if (auto* select = std::get_if<SqlSelect>(&cmd)) {
                    auto keyspace = select->keyspace.empty() ? currentKeyspace : select->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");

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
                    auto keyspace = flush->keyspace.empty() ? currentKeyspace : flush->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");

                    auto retTable = db_->openTable(keyspace, flush->table);
                    retTable->flush();
                    response = jsonOk();
                } else if (auto* del = std::get_if<SqlDelete>(&cmd)) {
                    auto keyspace = del->keyspace.empty() ? currentKeyspace : del->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");

                    auto retTable = db_->openTable(keyspace, del->table);
                    auto pkIndex = retTable->schema().primaryKeyIndex;
                    auto pkName = retTable->schema().columns[pkIndex].name;
                    if (del->whereColumn != pkName)
                        throw runtimeError("Where must use primary key");
                    byteVec pkBytes = partitionKeyBytes(retTable->schema().columns[pkIndex].type, del->whereValue);
                    retTable->deleteRow(pkBytes);
                    response = jsonOk();
                } else if (auto* upd = std::get_if<SqlUpdate>(&cmd)) {
                    auto keyspace = upd->keyspace.empty() ? currentKeyspace : upd->keyspace;
                    if (keyspace.empty())
                        throw runtimeError("No keyspace selected");

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
                    retTable->putRow(pkBytes, newRowBytes);
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
