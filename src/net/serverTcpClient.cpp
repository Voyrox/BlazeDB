#include "net/serverTcp.h"

#include "net/detail/serverTcpInternal.h"

#include "core/paths.h"

#include "query/sql.h"

#include "util/ascii.h"
#include "util/binIo.h"
#include "util/json.h"

#include "query/schema/detail/internal.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <functional>
#include <limits>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <sys/socket.h>
#include <unistd.h>

using std::string;

namespace xeondb {

struct OrderByKey {
    bool isNull = true;
    ColumnType type = ColumnType::Text;
    std::vector<u8> bytes;
    i32 i32v = 0;
    i64 i64v = 0;
    u8 u8v = 0;
    float f32v = 0.0f;
};

static OrderByKey orderByKeyFromRowBytes(const TableSchema& schema, usize orderByColumnIndex, const byteVec& rowBytes) {
    if (orderByColumnIndex >= schema.columns.size())
        throw runtimeError("unknown column");
    if (orderByColumnIndex == schema.primaryKeyIndex)
        throw runtimeError("bad order by");

    OrderByKey key;
    key.type = schema.columns[orderByColumnIndex].type;

    usize o = 0;
    auto version = readBeU32(rowBytes, o);
    if (version != 1)
        throw runtimeError("bad row version");

    for (usize i = 0; i < schema.columns.size(); i++) {
        if (i == schema.primaryKeyIndex)
            continue;
        if (o >= rowBytes.size())
            throw runtimeError("bad row");
        u8 nullMarker = rowBytes[o++];
        bool isNull = (nullMarker != 0);

        if (i != orderByColumnIndex) {
            if (!isNull)
                schema_detail::skipValueBytes(schema.columns[i].type, rowBytes, o);
            continue;
        }

        key.isNull = isNull;
        if (isNull)
            return key;

        ColumnType type = schema.columns[i].type;
        if (type == ColumnType::Text || type == ColumnType::Char || type == ColumnType::Blob) {
            u32 len = readBeU32(rowBytes, o);
            if (o + len > rowBytes.size())
                throw runtimeError("bad row");
            key.bytes.assign(rowBytes.begin() + static_cast<byteVec::difference_type>(o), rowBytes.begin() + static_cast<byteVec::difference_type>(o + len));
            o += len;
            return key;
        }
        if (type == ColumnType::Int32 || type == ColumnType::Date) {
            key.i32v = readBe32(rowBytes, o);
            return key;
        }
        if (type == ColumnType::Int64 || type == ColumnType::Timestamp) {
            key.i64v = readBe64(rowBytes, o);
            return key;
        }
        if (type == ColumnType::Boolean) {
            if (o + 1 > rowBytes.size())
                throw runtimeError("bad row");
            key.u8v = rowBytes[o++];
            return key;
        }
        if (type == ColumnType::Float32) {
            if (o + 4 > rowBytes.size())
                throw runtimeError("bad row");
            u32 u = 0;
            u |= static_cast<u32>(rowBytes[o + 0]) << 24;
            u |= static_cast<u32>(rowBytes[o + 1]) << 16;
            u |= static_cast<u32>(rowBytes[o + 2]) << 8;
            u |= static_cast<u32>(rowBytes[o + 3]) << 0;
            o += 4;
            std::memcpy(&key.f32v, &u, 4);
            return key;
        }

        throw runtimeError("bad type");
    }

    throw runtimeError("bad row");
}

static int orderByKeyCompareNonNull(const OrderByKey& a, const OrderByKey& b) {
    ColumnType type = a.type;
    if (type != b.type)
        throw runtimeError("bad order by");

    if (type == ColumnType::Text || type == ColumnType::Char || type == ColumnType::Blob) {
        if (std::lexicographical_compare(a.bytes.begin(), a.bytes.end(), b.bytes.begin(), b.bytes.end()))
            return -1;
        if (std::lexicographical_compare(b.bytes.begin(), b.bytes.end(), a.bytes.begin(), a.bytes.end()))
            return 1;
        return 0;
    }
    if (type == ColumnType::Boolean) {
        if (a.u8v < b.u8v)
            return -1;
        if (a.u8v > b.u8v)
            return 1;
        return 0;
    }
    if (type == ColumnType::Int32 || type == ColumnType::Date) {
        if (a.i32v < b.i32v)
            return -1;
        if (a.i32v > b.i32v)
            return 1;
        return 0;
    }
    if (type == ColumnType::Int64 || type == ColumnType::Timestamp) {
        if (a.i64v < b.i64v)
            return -1;
        if (a.i64v > b.i64v)
            return 1;
        return 0;
    }
    if (type == ColumnType::Float32) {
        bool aNan = std::isnan(a.f32v);
        bool bNan = std::isnan(b.f32v);
        if (aNan && bNan)
            return 0;
        if (aNan)
            return -1;
        if (bNan)
            return 1;
        if (a.f32v < b.f32v)
            return -1;
        if (a.f32v > b.f32v)
            return 1;
        return 0;
    }

    throw runtimeError("bad type");
}

static bool orderByKeyLess(const OrderByKey& a, const OrderByKey& b, bool desc) {
    // NULL ordering: ASC => NULLS FIRST, DESC => NULLS LAST
    if (a.isNull != b.isNull) {
        if (!desc)
            return a.isNull;
        return !a.isNull;
    }
    if (a.isNull)
        return false;

    int cmp = orderByKeyCompareNonNull(a, b);
    return desc ? (cmp > 0) : (cmp < 0);
}

static OrderByKey orderByKeyFromPkBytes(ColumnType type, const byteVec& pkBytes) {
    OrderByKey key;
    key.isNull = false;
    key.type = type;
    if (type == ColumnType::Text || type == ColumnType::Char || type == ColumnType::Blob) {
        key.bytes = pkBytes;
        return key;
    }
    if (type == ColumnType::Boolean) {
        key.u8v = pkBytes.empty() ? 0 : pkBytes[0];
        return key;
    }
    if (type == ColumnType::Int32 || type == ColumnType::Date) {
        if (pkBytes.size() != 4)
            throw runtimeError("bad pk");
        byteVec tmp = pkBytes;
        usize o = 0;
        key.i32v = readBe32(tmp, o);
        return key;
    }
    if (type == ColumnType::Int64 || type == ColumnType::Timestamp) {
        if (pkBytes.size() != 8)
            throw runtimeError("bad pk");
        byteVec tmp = pkBytes;
        usize o = 0;
        key.i64v = readBe64(tmp, o);
        return key;
    }
    if (type == ColumnType::Float32) {
        if (pkBytes.size() != 4)
            throw runtimeError("bad pk");
        u32 u = 0;
        u |= static_cast<u32>(pkBytes[0]) << 24;
        u |= static_cast<u32>(pkBytes[1]) << 16;
        u |= static_cast<u32>(pkBytes[2]) << 8;
        u |= static_cast<u32>(pkBytes[3]) << 0;
        std::memcpy(&key.f32v, &u, 4);
        return key;
    }
    throw runtimeError("bad type");
}

struct OutVal {
    enum class Kind {
        TypedBytes,
        I64,
        F64,
    };
    Kind kind = Kind::TypedBytes;
    bool isNull = true;
    ColumnType type = ColumnType::Text;
    byteVec bytes;
    i64 i64v = 0;
    long double f64v = 0.0;
};

static int compareCanonicalBytes(ColumnType type, const byteVec& a, const byteVec& b) {
    if (type == ColumnType::Text || type == ColumnType::Char || type == ColumnType::Blob) {
        if (std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()))
            return -1;
        if (std::lexicographical_compare(b.begin(), b.end(), a.begin(), a.end()))
            return 1;
        return 0;
    }
    if (type == ColumnType::Boolean) {
        u8 av = a.empty() ? 0 : a[0];
        u8 bv = b.empty() ? 0 : b[0];
        if (av < bv)
            return -1;
        if (av > bv)
            return 1;
        return 0;
    }
    if (type == ColumnType::Int32 || type == ColumnType::Date) {
        if (a.size() != 4 || b.size() != 4)
            return (a.size() < b.size()) ? -1 : (a.size() > b.size() ? 1 : 0);
        byteVec ta = a;
        byteVec tb = b;
        usize oa = 0;
        usize ob = 0;
        i32 av = readBe32(ta, oa);
        i32 bv = readBe32(tb, ob);
        if (av < bv)
            return -1;
        if (av > bv)
            return 1;
        return 0;
    }
    if (type == ColumnType::Int64 || type == ColumnType::Timestamp) {
        if (a.size() != 8 || b.size() != 8)
            return (a.size() < b.size()) ? -1 : (a.size() > b.size() ? 1 : 0);
        byteVec ta = a;
        byteVec tb = b;
        usize oa = 0;
        usize ob = 0;
        i64 av = readBe64(ta, oa);
        i64 bv = readBe64(tb, ob);
        if (av < bv)
            return -1;
        if (av > bv)
            return 1;
        return 0;
    }
    if (type == ColumnType::Float32) {
        if (a.size() != 4 || b.size() != 4)
            return (a.size() < b.size()) ? -1 : (a.size() > b.size() ? 1 : 0);
        u32 au = 0;
        au |= static_cast<u32>(a[0]) << 24;
        au |= static_cast<u32>(a[1]) << 16;
        au |= static_cast<u32>(a[2]) << 8;
        au |= static_cast<u32>(a[3]) << 0;
        u32 bu = 0;
        bu |= static_cast<u32>(b[0]) << 24;
        bu |= static_cast<u32>(b[1]) << 16;
        bu |= static_cast<u32>(b[2]) << 8;
        bu |= static_cast<u32>(b[3]) << 0;
        float af;
        float bf;
        std::memcpy(&af, &au, 4);
        std::memcpy(&bf, &bu, 4);
        bool aNan = std::isnan(af);
        bool bNan = std::isnan(bf);
        if (aNan && bNan)
            return 0;
        if (aNan)
            return -1;
        if (bNan)
            return 1;
        if (af < bf)
            return -1;
        if (af > bf)
            return 1;
        return 0;
    }
    if (std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end()))
        return -1;
    if (std::lexicographical_compare(b.begin(), b.end(), a.begin(), a.end()))
        return 1;
    return 0;
}

static bool outValLess(const OutVal& a, const OutVal& b, bool desc) {
    // NULL ordering: ASC => NULLS FIRST, DESC => NULLS LAST
    if (a.isNull != b.isNull) {
        if (!desc)
            return a.isNull;
        return !a.isNull;
    }
    if (a.isNull)
        return false;

    if (a.kind != b.kind)
        throw runtimeError("bad order by");

    if (a.kind == OutVal::Kind::TypedBytes) {
        int cmp = compareCanonicalBytes(a.type, a.bytes, b.bytes);
        return desc ? (cmp > 0) : (cmp < 0);
    }
    if (a.kind == OutVal::Kind::I64) {
        if (a.i64v == b.i64v)
            return false;
        return desc ? (a.i64v > b.i64v) : (a.i64v < b.i64v);
    }
    if (a.kind == OutVal::Kind::F64) {
        double av = static_cast<double>(a.f64v);
        double bv = static_cast<double>(b.f64v);
        bool aNan = std::isnan(av);
        bool bNan = std::isnan(bv);
        if (aNan != bNan) {
            if (!desc)
                return aNan;
            return !aNan;
        }
        if (aNan)
            return false;
        if (av == bv)
            return false;
        return desc ? (av > bv) : (av < bv);
    }
    throw runtimeError("bad order by");
}

struct CanonValue {
    bool isNull = true;
    ColumnType type = ColumnType::Text;
    byteVec bytes;
};

static void decodeNeededNonPkColumns(const TableSchema& schema, const byteVec& rowBytes, const std::vector<bool>& needed, std::vector<CanonValue>& out) {
    out.clear();
    out.resize(schema.columns.size());
    for (usize i = 0; i < schema.columns.size(); i++) {
        out[i].isNull = true;
        out[i].type = schema.columns[i].type;
        out[i].bytes.clear();
    }

    usize o = 0;
    auto version = readBeU32(rowBytes, o);
    if (version != 1)
        throw runtimeError("bad row version");

    for (usize i = 0; i < schema.columns.size(); i++) {
        if (i == schema.primaryKeyIndex)
            continue;
        if (o >= rowBytes.size())
            throw runtimeError("bad row");
        u8 nullMarker = rowBytes[o++];
        bool isNull = (nullMarker != 0);

        if (!needed[i]) {
            if (!isNull)
                schema_detail::skipValueBytes(schema.columns[i].type, rowBytes, o);
            continue;
        }

        out[i].isNull = isNull;
        if (isNull)
            continue;

        ColumnType type = schema.columns[i].type;
        if (type == ColumnType::Text || type == ColumnType::Char || type == ColumnType::Blob) {
            u32 len = readBeU32(rowBytes, o);
            if (o + len > rowBytes.size())
                throw runtimeError("bad row");
            out[i].bytes.assign(rowBytes.begin() + static_cast<byteVec::difference_type>(o), rowBytes.begin() + static_cast<byteVec::difference_type>(o + len));
            o += len;
            continue;
        }
        if (type == ColumnType::Int32 || type == ColumnType::Float32 || type == ColumnType::Date) {
            if (o + 4 > rowBytes.size())
                throw runtimeError("bad row");
            out[i].bytes.assign(rowBytes.begin() + static_cast<byteVec::difference_type>(o), rowBytes.begin() + static_cast<byteVec::difference_type>(o + 4));
            o += 4;
            continue;
        }
        if (type == ColumnType::Int64 || type == ColumnType::Timestamp) {
            if (o + 8 > rowBytes.size())
                throw runtimeError("bad row");
            out[i].bytes.assign(rowBytes.begin() + static_cast<byteVec::difference_type>(o), rowBytes.begin() + static_cast<byteVec::difference_type>(o + 8));
            o += 8;
            continue;
        }
        if (type == ColumnType::Boolean) {
            if (o + 1 > rowBytes.size())
                throw runtimeError("bad row");
            out[i].bytes.assign(rowBytes.begin() + static_cast<byteVec::difference_type>(o), rowBytes.begin() + static_cast<byteVec::difference_type>(o + 1));
            o += 1;
            continue;
        }
        throw runtimeError("bad type");
    }
}

static bool canonicalFloatFinite(const byteVec& bytes) {
    if (bytes.size() != 4)
        return false;
    u32 u = 0;
    u |= static_cast<u32>(bytes[0]) << 24;
    u |= static_cast<u32>(bytes[1]) << 16;
    u |= static_cast<u32>(bytes[2]) << 8;
    u |= static_cast<u32>(bytes[3]) << 0;
    float f;
    std::memcpy(&f, &u, 4);
    return std::isfinite(f);
}

void ServerTcp::handleClient(int clientFd) {
    using server_tcp_detail::sendAll;

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
                if (!startsWithKeywordIcase(line, "auth")) {
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
                    response = cmdAuth(*auth, currentUser);
                } else if (authEnabled_ && !currentUser.has_value()) {
                    response = jsonError("unauthorized");
                } else if (std::holds_alternative<SqlPing>(cmd)) {
                    response = cmdPing();
                } else if (auto* use = std::get_if<SqlUse>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdUse(*use, currentKeyspace, u);
                } else if (auto* createKeyspace = std::get_if<SqlCreateKeyspace>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdCreateKeyspace(*createKeyspace, u);
                } else if (auto* createTable = std::get_if<SqlCreateTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdCreateTable(*createTable, currentKeyspace, u);
                } else if (auto* dropTable = std::get_if<SqlDropTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdDropTable(*dropTable, currentKeyspace, u);
                } else if (auto* dropKeyspace = std::get_if<SqlDropKeyspace>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdDropKeyspace(*dropKeyspace, currentKeyspace, u);
                } else if (std::holds_alternative<SqlShowKeyspaces>(cmd)) {
                    response = cmdShowKeyspaces(currentUser);
                } else if (auto* showTables = std::get_if<SqlShowTables>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdShowTables(*showTables, currentKeyspace, u);
                } else if (auto* describe = std::get_if<SqlDescribeTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdDescribeTable(*describe, currentKeyspace, u);
                } else if (auto* showCreate = std::get_if<SqlShowCreateTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdShowCreateTable(*showCreate, currentKeyspace, u);
                } else if (auto* showMetrics = std::get_if<SqlShowMetrics>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdShowMetrics(*showMetrics, u);
                } else if (auto* trunc = std::get_if<SqlTruncateTable>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdTruncateTable(*trunc, currentKeyspace, u);
                } else if (auto* insert = std::get_if<SqlInsert>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdInsert(*insert, currentKeyspace, u);
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

                    const TableSchema& schema = retTable->schema();
                    bool hasAgg = false;
                    for (const auto& it : select->selectItems) {
                        if (std::holds_alternative<SqlSelect::SelectAggregate>(it)) {
                            hasAgg = true;
                            break;
                        }
                    }
                    bool hasGroupBy = !select->groupBy.empty();
                    bool isGroupedQuery = hasAgg || hasGroupBy;

                    auto selectMapping = [&]() -> std::vector<std::pair<string, string>> {
                        std::vector<std::pair<string, string>> mapped;
                        if (select->selectStar)
                            return mapped;
                        mapped.reserve(select->selectItems.size());
                        for (const auto& it : select->selectItems) {
                            auto* col = std::get_if<SqlSelect::SelectColumn>(&it);
                            if (!col)
                                throw runtimeError("mixed aggregate");
                            string outName = col->alias.has_value() ? *col->alias : col->name;
                            mapped.push_back({outName, col->name});
                        }
                        return mapped;
                    };

                    std::vector<Table::ScanRow> rows;
                    bool haveRows = false;

                    if (select->whereColumn.has_value()) {
                        if (!select->whereValue.has_value())
                            throw runtimeError("Expected where value");
                        if (*select->whereColumn != pkName)
                            throw runtimeError("Where must use primary key");
                        byteVec pkBytes = partitionKeyBytes(schema.columns[pkIndex].type, *select->whereValue);
                        auto rowBytesBuf = retTable->getRow(pkBytes);

                        if (!isGroupedQuery) {
                            if (!rowBytesBuf.has_value()) {
                                response = string("{\"ok\":true,\"found\":false}");
                            } else {
                                auto mapped = selectMapping();
                                string rowJson = rowToJsonMapped(schema, pkBytes, *rowBytesBuf, mapped);
                                response = string("{\"ok\":true,\"found\":true,\"row\":") + rowJson + "}";
                            }
                        } else {
                            // GROUP BY / aggregates over 0-1 rows.
                            if (rowBytesBuf.has_value()) {
                                Table::ScanRow r;
                                r.pkBytes = pkBytes;
                                r.rowBytes = *rowBytesBuf;
                                rows.push_back(std::move(r));
                            }
                            haveRows = true;
                        }
                    } else {
                        // Full scan.
                        rows = retTable->scanAllRowsByPk(false);
                        haveRows = true;
                    }

                    if (haveRows) {
                        if (!isGroupedQuery) {
                            // Resolve ORDER BY to schema column indices.
                            struct ResolvedOrder {
                                usize colIndex;
                                bool desc;
                            };
                            std::vector<ResolvedOrder> resolved;

                            auto mapped = selectMapping();

                            auto resolveNameToColIndex = [&](const string& name) -> std::optional<usize> {
                                // First try output columns (aliases).
                                if (!select->selectStar) {
                                    for (const auto& it : select->selectItems) {
                                        auto* c = std::get_if<SqlSelect::SelectColumn>(&it);
                                        if (!c)
                                            continue;
                                        if (c->alias.has_value() && asciiIEquals(*c->alias, name))
                                            return findColumnIndex(schema, c->name);
                                        if (asciiIEquals(c->name, name))
                                            return findColumnIndex(schema, c->name);
                                    }
                                }
                                // Then try schema column directly.
                                return findColumnIndex(schema, name);
                            };

                            for (const auto& ob : select->orderBy) {
                                if (ob.aggregateExpr.has_value())
                                    throw runtimeError("ORDER BY aggregate requires GROUP BY");

                                usize colIndex = 0;
                                if (ob.position.has_value()) {
                                    usize pos = *ob.position;
                                    if (pos == 0)
                                        throw runtimeError("Bad ORDER BY position");
                                    if (select->selectStar) {
                                        if (pos < 1 || pos > schema.columns.size())
                                            throw runtimeError("Bad ORDER BY position");
                                        colIndex = pos - 1;
                                    } else {
                                        if (pos < 1 || pos > select->selectItems.size())
                                            throw runtimeError("Bad ORDER BY position");
                                        auto* c = std::get_if<SqlSelect::SelectColumn>(&select->selectItems[pos - 1]);
                                        if (!c)
                                            throw runtimeError("Bad ORDER BY position");
                                        auto idx = findColumnIndex(schema, c->name);
                                        if (!idx.has_value())
                                            throw runtimeError("unknown column");
                                        colIndex = *idx;
                                    }
                                } else if (ob.nameOrAlias.has_value()) {
                                    auto idx = resolveNameToColIndex(*ob.nameOrAlias);
                                    if (!idx.has_value())
                                        throw runtimeError("unknown column");
                                    colIndex = *idx;
                                } else {
                                    throw runtimeError("bad order by");
                                }
                                resolved.push_back({colIndex, ob.desc});
                            }

                            if (!resolved.empty()) {
                                // Precompute keys.
                                std::vector<std::vector<OrderByKey>> keys;
                                keys.resize(rows.size());
                                for (usize r = 0; r < rows.size(); r++) {
                                    keys[r].reserve(resolved.size());
                                    for (const auto& t : resolved) {
                                        if (t.colIndex == pkIndex)
                                            keys[r].push_back(orderByKeyFromPkBytes(schema.columns[pkIndex].type, rows[r].pkBytes));
                                        else
                                            keys[r].push_back(orderByKeyFromRowBytes(schema, t.colIndex, rows[r].rowBytes));
                                    }
                                }

                                std::vector<usize> idx(rows.size());
                                std::iota(idx.begin(), idx.end(), 0);
                                std::stable_sort(idx.begin(), idx.end(), [&](usize a, usize b) {
                                    for (usize t = 0; t < resolved.size(); t++) {
                                        const auto& ra = keys[a][t];
                                        const auto& rb = keys[b][t];
                                        if (orderByKeyLess(ra, rb, resolved[t].desc))
                                            return true;
                                        if (orderByKeyLess(rb, ra, resolved[t].desc))
                                            return false;
                                    }
                                    return false;
                                });

                                std::vector<Table::ScanRow> sorted;
                                sorted.resize(rows.size());
                                for (usize outI = 0; outI < idx.size(); outI++)
                                    sorted[outI] = std::move(rows[idx[outI]]);
                                rows = std::move(sorted);
                            }

                            string out = "{\"ok\":true,\"rows\":[";
                            bool first = true;
                            usize emitted = 0;
                            for (const auto& r : rows) {
                                if (select->limit.has_value() && emitted >= *select->limit)
                                    break;
                                if (!first)
                                    out += ",";
                                first = false;
                                out += rowToJsonMapped(schema, r.pkBytes, r.rowBytes, mapped);
                                emitted++;
                            }
                            out += "]}";
                            response = out;
                        } else {
                            // GROUP BY / aggregate scan.
                            struct AggSpec {
                                SqlSelect::SelectAggregate agg;
                                usize colIndex = 0;
                                ColumnType colType = ColumnType::Text;
                                bool hasCol = false;
                            };

                            // Resolve group-by columns.
                            std::vector<usize> groupCols;
                            std::unordered_map<string, string> aliasToCol;
                            for (const auto& it : select->selectItems) {
                                if (auto* c = std::get_if<SqlSelect::SelectColumn>(&it)) {
                                    if (c->alias.has_value())
                                        aliasToCol[*c->alias] = c->name;
                                }
                            }

                            for (const auto& gb : select->groupBy) {
                                string colName;
                                if (gb.position.has_value()) {
                                    usize pos = *gb.position;
                                    if (pos < 1 || pos > select->selectItems.size())
                                        throw runtimeError("Bad GROUP BY position");
                                    auto* c = std::get_if<SqlSelect::SelectColumn>(&select->selectItems[pos - 1]);
                                    if (!c)
                                        throw runtimeError("Bad GROUP BY position");
                                    colName = c->name;
                                } else if (gb.name.has_value()) {
                                    auto aliasIt = aliasToCol.find(*gb.name);
                                    if (aliasIt != aliasToCol.end()) {
                                        colName = aliasIt->second;
                                    } else {
                                        colName = *gb.name;
                                    }
                                } else {
                                    throw runtimeError("bad group by");
                                }
                                auto idx = findColumnIndex(schema, colName);
                                if (!idx.has_value())
                                    throw runtimeError("unknown column");
                                groupCols.push_back(*idx);
                            }

                            bool anyAgg = hasAgg;
                            if (anyAgg && groupCols.empty()) {
                                // Aggregate without GROUP BY: no non-aggregate columns allowed.
                                for (const auto& it : select->selectItems) {
                                    if (std::holds_alternative<SqlSelect::SelectColumn>(it))
                                        throw runtimeError("non-aggregate column in aggregate query");
                                }
                            }

                            // Build group col set.
                            std::vector<bool> isGroupCol(schema.columns.size(), false);
                            for (auto idx : groupCols)
                                isGroupCol[idx] = true;

                            // Validate select list.
                            if (select->selectStar)
                                throw runtimeError("SELECT * not allowed with GROUP BY");
                            for (const auto& it : select->selectItems) {
                                if (auto* c = std::get_if<SqlSelect::SelectColumn>(&it)) {
                                    auto idx = findColumnIndex(schema, c->name);
                                    if (!idx.has_value())
                                        throw runtimeError("unknown column");
                                    if (!isGroupCol[*idx])
                                        throw runtimeError("non-grouped column");
                                }
                            }

                            // Collect aggregate specs and output names.
                            std::vector<AggSpec> aggs;
                            aggs.reserve(select->selectItems.size());
                            std::vector<string> outNames;
                            outNames.reserve(select->selectItems.size());
                            std::unordered_set<string> seenNames;

                            auto defaultAggName = [&](const SqlSelect::SelectAggregate& a) -> string {
                                auto funcName = [&](SqlSelect::AggFunc f) -> string {
                                    switch (f) {
                                    case SqlSelect::AggFunc::Count:
                                        return "count";
                                    case SqlSelect::AggFunc::Min:
                                        return "min";
                                    case SqlSelect::AggFunc::Max:
                                        return "max";
                                    case SqlSelect::AggFunc::Sum:
                                        return "sum";
                                    case SqlSelect::AggFunc::Avg:
                                        return "avg";
                                    default:
                                        return "agg";
                                    }
                                };
                                if (a.starArg)
                                    return funcName(a.func);
                                if (a.columnArg.has_value())
                                    return funcName(a.func) + "_" + *a.columnArg;
                                return funcName(a.func);
                            };

                            for (const auto& it : select->selectItems) {
                                if (auto* c = std::get_if<SqlSelect::SelectColumn>(&it)) {
                                    string outName = c->alias.has_value() ? *c->alias : c->name;
                                    outNames.push_back(outName);
                                    if (seenNames.count(outName) != 0)
                                        throw runtimeError("duplicate output column");
                                    seenNames.insert(outName);
                                    continue;
                                }
                                auto* a = std::get_if<SqlSelect::SelectAggregate>(&it);
                                if (!a)
                                    throw runtimeError("bad select");

                                AggSpec spec;
                                spec.agg = *a;
                                if (!a->starArg) {
                                    if (!a->columnArg.has_value())
                                        throw runtimeError("bad aggregate");
                                    auto idx = findColumnIndex(schema, *a->columnArg);
                                    if (!idx.has_value())
                                        throw runtimeError("unknown column");
                                    spec.hasCol = true;
                                    spec.colIndex = *idx;
                                    spec.colType = schema.columns[*idx].type;
                                } else {
                                    if (a->func != SqlSelect::AggFunc::Count)
                                        throw runtimeError("Only COUNT supports *");
                                }

                                if (a->func == SqlSelect::AggFunc::Sum || a->func == SqlSelect::AggFunc::Avg) {
                                    if (!spec.hasCol)
                                        throw runtimeError("SUM/AVG requires column");
                                    ColumnType t = spec.colType;
                                    if (!(t == ColumnType::Int32 || t == ColumnType::Int64 || t == ColumnType::Float32))
                                        throw runtimeError("SUM/AVG requires numeric");
                                }

                                aggs.push_back(spec);
                                string outName = a->alias.has_value() ? *a->alias : defaultAggName(*a);
                                outNames.push_back(outName);
                                if (seenNames.count(outName) != 0)
                                    throw runtimeError("duplicate output column");
                                seenNames.insert(outName);
                            }

                            // Needed columns for decoding.
                            std::vector<bool> needed(schema.columns.size(), false);
                            for (auto idx : groupCols) {
                                if (idx != pkIndex)
                                    needed[idx] = true;
                            }
                            for (const auto& it : select->selectItems) {
                                if (auto* c = std::get_if<SqlSelect::SelectColumn>(&it)) {
                                    auto idx = findColumnIndex(schema, c->name);
                                    if (idx.has_value() && *idx != pkIndex)
                                        needed[*idx] = true;
                                } else if (auto* a = std::get_if<SqlSelect::SelectAggregate>(&it)) {
                                    if (a->columnArg.has_value()) {
                                        auto idx = findColumnIndex(schema, *a->columnArg);
                                        if (idx.has_value() && *idx != pkIndex)
                                            needed[*idx] = true;
                                    }
                                }
                            }

                            auto appendBeU32ToString = [](string& s, u32 v) {
                                char buf[4];
                                buf[0] = static_cast<char>((v >> 24) & 0xFF);
                                buf[1] = static_cast<char>((v >> 16) & 0xFF);
                                buf[2] = static_cast<char>((v >> 8) & 0xFF);
                                buf[3] = static_cast<char>((v >> 0) & 0xFF);
                                s.append(buf, buf + 4);
                            };

                            auto makeGroupKey = [&](const std::vector<CanonValue>& decoded, const Table::ScanRow& r) -> string {
                                string key;
                                key.reserve(groupCols.size() * 16);
                                for (auto colIdx : groupCols) {
                                    ColumnType t = schema.columns[colIdx].type;
                                    key.push_back(static_cast<char>(static_cast<u8>(t)));
                                    bool isNull = false;
                                    const byteVec* bytes = nullptr;
                                    byteVec tmp;
                                    if (colIdx == pkIndex) {
                                        bytes = &r.pkBytes;
                                    } else {
                                        isNull = decoded[colIdx].isNull;
                                        if (!isNull)
                                            bytes = &decoded[colIdx].bytes;
                                    }
                                    key.push_back(isNull ? 1 : 0);
                                    if (isNull) {
                                        appendBeU32ToString(key, 0);
                                    } else {
                                        appendBeU32ToString(key, static_cast<u32>(bytes->size()));
                                        key.append(reinterpret_cast<const char*>(bytes->data()), reinterpret_cast<const char*>(bytes->data() + bytes->size()));
                                    }
                                }
                                return key;
                            };

                            struct AggAcc {
                                // shared
                                u64 count = 0;
                                // min/max
                                bool hasBest = false;
                                byteVec best;
                                // sum/avg
                                bool hasSum = false;
                                i64 isum = 0;
                                bool isumOverflow = false;
                                long double isumLd = 0.0;
                                long double fsum = 0.0;
                                u64 n = 0;
                            };

                            struct GroupState {
                                string groupKey;
                                std::vector<CanonValue> groupValsByIndex; // size schema.columns, only group cols populated
                                std::vector<AggAcc> acc;
                            };

                            std::unordered_map<string, GroupState> groups;
                            groups.reserve(128);

                            bool needAny = false;
                            for (usize ii = 0; ii < needed.size(); ii++) {
                                if (needed[ii]) {
                                    needAny = true;
                                    break;
                                }
                            }

                            std::vector<CanonValue> decoded;
                            for (const auto& r : rows) {
                                if (needAny)
                                    decodeNeededNonPkColumns(schema, r.rowBytes, needed, decoded);
                                else {
                                    decoded.clear();
                                    decoded.resize(schema.columns.size());
                                    for (usize ii = 0; ii < schema.columns.size(); ii++) {
                                        decoded[ii].isNull = true;
                                        decoded[ii].type = schema.columns[ii].type;
                                    }
                                }

                                string gk = makeGroupKey(decoded, r);
                                auto it = groups.find(gk);
                                if (it == groups.end()) {
                                    GroupState st;
                                    st.groupKey = gk;
                                    st.groupValsByIndex.resize(schema.columns.size());
                                    for (usize ii = 0; ii < schema.columns.size(); ii++) {
                                        st.groupValsByIndex[ii].isNull = true;
                                        st.groupValsByIndex[ii].type = schema.columns[ii].type;
                                    }
                                    for (auto colIdx : groupCols) {
                                        CanonValue v;
                                        v.type = schema.columns[colIdx].type;
                                        if (colIdx == pkIndex) {
                                            v.isNull = false;
                                            v.bytes = r.pkBytes;
                                        } else {
                                            v = decoded[colIdx];
                                        }
                                        st.groupValsByIndex[colIdx] = v;
                                    }
                                    st.acc.resize(aggs.size());
                                    it = groups.emplace(st.groupKey, std::move(st)).first;
                                }

                                GroupState& st = it->second;

                                // Update aggregate accumulators.
                                usize aggPos = 0;
                                for (const auto& spec : aggs) {
                                    const auto& a = spec.agg;
                                    AggAcc& acc = st.acc[aggPos++];

                                    if (a.func == SqlSelect::AggFunc::Count) {
                                        if (a.starArg) {
                                            acc.count++;
                                        } else {
                                            if (spec.colIndex == pkIndex) {
                                                acc.count++;
                                            } else {
                                                if (!decoded[spec.colIndex].isNull)
                                                    acc.count++;
                                            }
                                        }
                                        continue;
                                    }

                                    // fetch value
                                    CanonValue v;
                                    if (!spec.hasCol) {
                                        v.isNull = true;
                                    } else if (spec.colIndex == pkIndex) {
                                        v.isNull = false;
                                        v.type = schema.columns[pkIndex].type;
                                        v.bytes = r.pkBytes;
                                    } else {
                                        v = decoded[spec.colIndex];
                                    }

                                    if (v.type == ColumnType::Float32 && !v.isNull && !canonicalFloatFinite(v.bytes)) {
                                        // Treat non-finite floats like NULL for MIN/MAX/SUM/AVG.
                                        v.isNull = true;
                                    }

                                    if (v.isNull)
                                        continue;

                                    if (a.func == SqlSelect::AggFunc::Min || a.func == SqlSelect::AggFunc::Max) {
                                        if (!acc.hasBest) {
                                            acc.hasBest = true;
                                            acc.best = v.bytes;
                                        } else {
                                            int cmp = compareCanonicalBytes(v.type, v.bytes, acc.best);
                                            if (a.func == SqlSelect::AggFunc::Min) {
                                                if (cmp < 0)
                                                    acc.best = v.bytes;
                                            } else {
                                                if (cmp > 0)
                                                    acc.best = v.bytes;
                                            }
                                        }
                                        continue;
                                    }

                                    if (a.func == SqlSelect::AggFunc::Sum || a.func == SqlSelect::AggFunc::Avg) {
                                        acc.hasSum = true;
                                        if (v.type == ColumnType::Int32 || v.type == ColumnType::Int64) {
                                            byteVec tmp = v.bytes;
                                            usize oo = 0;
                                            i64 iv = (v.type == ColumnType::Int32) ? static_cast<i64>(readBe32(tmp, oo)) : readBe64(tmp, oo);
                                            acc.isumLd += static_cast<long double>(iv);
                                            if (!acc.isumOverflow) {
                                                i64 next = 0;
                                                if (__builtin_add_overflow(acc.isum, iv, &next)) {
                                                    acc.isumOverflow = true;
                                                } else {
                                                    acc.isum = next;
                                                }
                                            }
                                            acc.n += 1;
                                        } else if (v.type == ColumnType::Float32) {
                                            if (v.bytes.size() != 4)
                                                throw runtimeError("bad float");
                                            u32 u = 0;
                                            u |= static_cast<u32>(v.bytes[0]) << 24;
                                            u |= static_cast<u32>(v.bytes[1]) << 16;
                                            u |= static_cast<u32>(v.bytes[2]) << 8;
                                            u |= static_cast<u32>(v.bytes[3]) << 0;
                                            float f;
                                            std::memcpy(&f, &u, 4);
                                            if (std::isfinite(f)) {
                                                acc.fsum += static_cast<long double>(f);
                                                acc.n += 1;
                                            }
                                        } else {
                                            throw runtimeError("SUM/AVG requires numeric");
                                        }
                                        continue;
                                    }
                                }
                            }

                            // Aggregate-without-GROUP-BY over empty input returns one row.
                            if (groups.empty() && anyAgg && groupCols.empty()) {
                                GroupState st;
                                st.groupKey = string();
                                st.groupValsByIndex.resize(schema.columns.size());
                                for (usize ii = 0; ii < schema.columns.size(); ii++) {
                                    st.groupValsByIndex[ii].isNull = true;
                                    st.groupValsByIndex[ii].type = schema.columns[ii].type;
                                }
                                st.acc.resize(aggs.size());
                                groups.emplace(st.groupKey, std::move(st));
                            }

                            // Build base result order by groupKey for determinism.
                            struct OutputRow {
                                string tie;
                                std::vector<OutVal> vals;
                            };
                            std::vector<OutputRow> outRows;
                            outRows.reserve(groups.size());

                            for (auto& kv : groups) {
                                GroupState& st = kv.second;
                                OutputRow orow;
                                orow.tie = st.groupKey;
                                orow.vals.reserve(select->selectItems.size());

                                usize aggPos = 0;
                                for (const auto& itSel : select->selectItems) {
                                    if (auto* c = std::get_if<SqlSelect::SelectColumn>(&itSel)) {
                                        auto idx = findColumnIndex(schema, c->name);
                                        if (!idx.has_value())
                                            throw runtimeError("unknown column");
                                        CanonValue v = st.groupValsByIndex[*idx];
                                        OutVal ov;
                                        ov.kind = OutVal::Kind::TypedBytes;
                                        ov.isNull = v.isNull;
                                        ov.type = v.type;
                                        ov.bytes = v.bytes;
                                        orow.vals.push_back(std::move(ov));
                                    } else if (auto* a = std::get_if<SqlSelect::SelectAggregate>(&itSel)) {
                                        const AggAcc& acc = st.acc[aggPos++];
                                        OutVal ov;

                                        if (a->func == SqlSelect::AggFunc::Count) {
                                            ov.kind = OutVal::Kind::I64;
                                            ov.isNull = false;
                                            ov.i64v = static_cast<i64>(acc.count);
                                            orow.vals.push_back(std::move(ov));
                                            continue;
                                        }

                                        if (a->func == SqlSelect::AggFunc::Min || a->func == SqlSelect::AggFunc::Max) {
                                            ov.kind = OutVal::Kind::TypedBytes;
                                            if (!acc.hasBest) {
                                                ov.isNull = true;
                                            } else {
                                                ov.isNull = false;
                                                ColumnType t = ColumnType::Text;
                                                if (a->columnArg.has_value()) {
                                                    auto idx = findColumnIndex(schema, *a->columnArg);
                                                    if (!idx.has_value())
                                                        throw runtimeError("unknown column");
                                                    t = schema.columns[*idx].type;
                                                }
                                                ov.type = t;
                                                ov.bytes = acc.best;
                                            }
                                            orow.vals.push_back(std::move(ov));
                                            continue;
                                        }

                                        if (a->func == SqlSelect::AggFunc::Sum) {
                                            if (!acc.hasSum || acc.n == 0) {
                                                ov.isNull = true;
                                                ov.kind = OutVal::Kind::I64;
                                            } else {
                                                ColumnType t = ColumnType::Int64;
                                                if (a->columnArg.has_value()) {
                                                    auto idx = findColumnIndex(schema, *a->columnArg);
                                                    if (!idx.has_value())
                                                        throw runtimeError("unknown column");
                                                    t = schema.columns[*idx].type;
                                                }
                                                if (t == ColumnType::Float32) {
                                                    ov.kind = OutVal::Kind::F64;
                                                    ov.isNull = false;
                                                    ov.f64v = acc.fsum;
                                                } else {
                                                    ov.kind = OutVal::Kind::I64;
                                                    ov.isNull = false;
                                                    if (acc.isumOverflow) {
                                                        throw runtimeError("sum overflow");
                                                    }
                                                    ov.i64v = acc.isum;
                                                }
                                            }
                                            orow.vals.push_back(std::move(ov));
                                            continue;
                                        }

                                        if (a->func == SqlSelect::AggFunc::Avg) {
                                            ov.kind = OutVal::Kind::F64;
                                            if (!acc.hasSum || acc.n == 0) {
                                                ov.isNull = true;
                                            } else {
                                                ov.isNull = false;
                                                ColumnType t = ColumnType::Int64;
                                                if (a->columnArg.has_value()) {
                                                    auto idx = findColumnIndex(schema, *a->columnArg);
                                                    if (!idx.has_value())
                                                        throw runtimeError("unknown column");
                                                    t = schema.columns[*idx].type;
                                                }
                                                if (t == ColumnType::Float32) {
                                                    ov.f64v = acc.fsum / static_cast<long double>(acc.n);
                                                } else {
                                                    ov.f64v = acc.isumLd / static_cast<long double>(acc.n);
                                                }
                                            }
                                            orow.vals.push_back(std::move(ov));
                                            continue;
                                        }

                                        throw runtimeError("bad aggregate");
                                    }
                                }

                                outRows.push_back(std::move(orow));
                            }

                            std::sort(outRows.begin(), outRows.end(), [](const OutputRow& a, const OutputRow& b) {
                                return a.tie < b.tie;
                            });

                            // Resolve ORDER BY to output indices.
                            struct ResolvedOutOrder {
                                usize outIndex;
                                bool desc;
                            };
                            std::vector<ResolvedOutOrder> outOrder;
                            outOrder.reserve(select->orderBy.size());

                            auto aggEq = [](const SqlSelect::SelectAggregate& a, const SqlSelect::SelectAggregate& b) {
                                if (a.func != b.func)
                                    return false;
                                if (a.starArg != b.starArg)
                                    return false;
                                if (a.columnArg.has_value() != b.columnArg.has_value())
                                    return false;
                                if (a.columnArg.has_value() && b.columnArg.has_value() && !asciiIEquals(*a.columnArg, *b.columnArg))
                                    return false;
                                return true;
                            };

                            auto resolveOutName = [&](const string& name) -> std::optional<usize> {
                                for (usize idx = 0; idx < outNames.size(); idx++) {
                                    if (asciiIEquals(outNames[idx], name))
                                        return idx;
                                }
                                return std::nullopt;
                            };

                            for (const auto& ob : select->orderBy) {
                                usize outIdx = 0;
                                if (ob.position.has_value()) {
                                    usize pos = *ob.position;
                                    if (pos < 1 || pos > outNames.size())
                                        throw runtimeError("Bad ORDER BY position");
                                    outIdx = pos - 1;
                                } else if (ob.nameOrAlias.has_value()) {
                                    auto tmp = resolveOutName(*ob.nameOrAlias);
                                    if (!tmp.has_value())
                                        throw runtimeError("unknown column");
                                    outIdx = *tmp;
                                } else if (ob.aggregateExpr.has_value()) {
                                    bool found = false;
                                    for (usize si = 0; si < select->selectItems.size(); si++) {
                                        auto* a = std::get_if<SqlSelect::SelectAggregate>(&select->selectItems[si]);
                                        if (!a)
                                            continue;
                                        if (aggEq(*a, *ob.aggregateExpr)) {
                                            outIdx = si;
                                            found = true;
                                            break;
                                        }
                                    }
                                    if (!found)
                                        throw runtimeError("unknown aggregate");
                                } else {
                                    throw runtimeError("bad order by");
                                }
                                outOrder.push_back({outIdx, ob.desc});
                            }

                            if (!outOrder.empty()) {
                                std::stable_sort(outRows.begin(), outRows.end(), [&](const OutputRow& a, const OutputRow& b) {
                                    for (const auto& t : outOrder) {
                                        const OutVal& av = a.vals[t.outIndex];
                                        const OutVal& bv = b.vals[t.outIndex];
                                        if (outValLess(av, bv, t.desc))
                                            return true;
                                        if (outValLess(bv, av, t.desc))
                                            return false;
                                    }
                                    return false;
                                });
                            }

                            string out = "{\"ok\":true,\"rows\":[";
                            bool first = true;
                            usize emitted = 0;
                            for (const auto& rr : outRows) {
                                if (select->limit.has_value() && emitted >= *select->limit)
                                    break;
                                if (!first)
                                    out += ",";
                                first = false;
                                out += "{";
                                for (usize ci = 0; ci < rr.vals.size(); ci++) {
                                    if (ci != 0)
                                        out += ",";
                                    out += "\"" + jsonEscape(outNames[ci]) + "\":";
                                    const OutVal& v = rr.vals[ci];
                                    if (v.isNull) {
                                        out += "null";
                                    } else if (v.kind == OutVal::Kind::TypedBytes) {
                                        out += schema_detail::jsonPkValue(v.type, v.bytes);
                                    } else if (v.kind == OutVal::Kind::I64) {
                                        out += std::to_string(v.i64v);
                                    } else if (v.kind == OutVal::Kind::F64) {
                                        double dv = static_cast<double>(v.f64v);
                                        if (!std::isfinite(dv))
                                            out += "null";
                                        else
                                            out += std::to_string(dv);
                                    } else {
                                        out += "null";
                                    }
                                }
                                out += "}";
                                emitted++;
                            }
                            out += "]}";
                            response = out;
                        }
                    }
                } else if (auto* flush = std::get_if<SqlFlush>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdFlush(*flush, currentKeyspace, u);
                } else if (auto* del = std::get_if<SqlDelete>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdDelete(*del, currentKeyspace, u);
                } else if (auto* upd = std::get_if<SqlUpdate>(&cmd)) {
                    const AuthedUser& u = authEnabled_ ? *currentUser : noAuthRoot;
                    response = cmdUpdate(*upd, currentKeyspace, u);
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
