#include "query/sql.h"

#include "query/sql/detail/parse_utils.h"

using std::string;
using std::vector;

namespace xeondb {

namespace {

using namespace sql_detail;

static bool requireKeyword(stringView s, usize& i, stringView keyword, string& error, const char* message) {
    if (!matchKeyword(s, i, keyword)) {
        error = message;
        return false;
    }
    return true;
}

static bool requireChar(stringView s, usize& i, char c, string& error, const char* message) {
    if (!consumeChar(s, i, c)) {
        error = message;
        return false;
    }
    return true;
}

static bool requireIdentifier(stringView s, usize& i, string& out, string& error, const char* message) {
    if (!parseIdentifier(s, i, out)) {
        error = message;
        return false;
    }
    return true;
}

static bool requireQuoted(stringView s, usize& i, string& out, string& error, const char* message) {
    if (!parseQuoted(s, i, out)) {
        error = message;
        return false;
    }
    return true;
}

static bool requireLiteral(stringView s, usize& i, SqlLiteral& out, string& error, const char* message) {
    if (!literal(s, i, out)) {
        error = message;
        return false;
    }
    return true;
}

static bool requireEof(stringView s, usize& i, string& error) {
    skipWhitespace(s, i);
    if (i != s.size()) {
        error = "Unexpected trailing input";
        return false;
    }
    return true;
}

static bool parseQualifiedName(stringView s, usize& i, string& outKeyspace, string& outTable, string& error, const char* expectedMessage) {
    string firstName;
    if (!parseIdentifier(s, i, firstName)) {
        error = expectedMessage;
        return false;
    }
    if (consumeChar(s, i, '.')) {
        outKeyspace = firstName;
        if (!parseIdentifier(s, i, outTable)) {
            error = expectedMessage;
            return false;
        }
    } else {
        outKeyspace.clear();
        outTable = firstName;
    }
    return true;
}

static bool tryParsePing(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    (void)error;
    usize j = i;
    if (!matchKeyword(s, j, "ping"))
        return false;
    out = SqlPing{};
    return true;
}

static bool tryParseAuth(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "auth"))
        return false;
    i = j;
    SqlAuth cmd;
    if (!requireQuoted(s, i, cmd.username, error, "Expected username")) {
        out.reset();
        return true;
    }
    if (!requireQuoted(s, i, cmd.password, error, "Expected password")) {
        out.reset();
        return true;
    }
    out = cmd;
    return true;
}

static bool tryParseUse(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "use"))
        return false;
    i = j;
    SqlUse cmd;
    if (!requireIdentifier(s, i, cmd.keyspace, error, "Expected keyspace")) {
        out.reset();
        return true;
    }
    out = cmd;
    return true;
}

static bool tryParseCreate(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "create"))
        return false;
    i = j;

    if (matchKeyword(s, i, "keyspace")) {
        SqlCreateKeyspace cmd;
        if (!ifNotExists(s, i, cmd.ifNotExists)) {
            error = "Expected not exists";
            out.reset();
            return true;
        }
        if (!requireIdentifier(s, i, cmd.keyspace, error, "Expected keyspace")) {
            out.reset();
            return true;
        }
        out = cmd;
        return true;
    }

    if (matchKeyword(s, i, "table")) {
        SqlCreateTable cmd;
        if (!ifNotExists(s, i, cmd.ifNotExists)) {
            error = "Expected not exists";
            out.reset();
            return true;
        }

        if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
            out.reset();
            return true;
        }

        if (!requireChar(s, i, '(', error, "Expected (")) {
            out.reset();
            return true;
        }

        std::vector<ColumnDef> cols;
        string pkName;
        while (true) {
            skipWhitespace(s, i);
            if (consumeChar(s, i, ')'))
                break;

            usize k = i;
            if (matchKeyword(s, k, "primary")) {
                i = k;
                if (!requireKeyword(s, i, "key", error, "Expected key")) {
                    out.reset();
                    return true;
                }
                if (!requireChar(s, i, '(', error, "Expected (")) {
                    out.reset();
                    return true;
                }
                if (!requireIdentifier(s, i, pkName, error, "Expected pk")) {
                    out.reset();
                    return true;
                }
                if (!requireChar(s, i, ')', error, "Expected )")) {
                    out.reset();
                    return true;
                }
            } else {
                string colName;
                string typeNameStr;
                if (!requireIdentifier(s, i, colName, error, "Expected column")) {
                    out.reset();
                    return true;
                }
                if (!typeName(s, i, typeNameStr)) {
                    error = "Expected type";
                    out.reset();
                    return true;
                }
                auto ct = columnTypeFromName(typeNameStr);
                if (!ct.has_value()) {
                    error = "unknown type";
                    out.reset();
                    return true;
                }
                cols.push_back(ColumnDef{colName, *ct});
            }

            skipWhitespace(s, i);
            if (consumeChar(s, i, ','))
                continue;
            if (consumeChar(s, i, ')'))
                break;
            error = "Expected , or )";
            out.reset();
            return true;
        }

        if (pkName.empty()) {
            error = "missing primary key";
            out.reset();
            return true;
        }

        TableSchema schema;
        schema.columns = std::move(cols);
        auto col = findColumnIndex(schema, pkName);
        if (!col.has_value()) {
            error = "pk not in columns";
            out.reset();
            return true;
        }
        schema.primaryKeyIndex = *col;
        cmd.schema = std::move(schema);
        out = cmd;
        return true;
    }

    error = "Expected keyspace or table";
    out.reset();
    return true;
}

static bool tryParseDrop(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "drop"))
        return false;
    i = j;

    if (matchKeyword(s, i, "table")) {
        SqlDropTable cmd;
        if (!ifExists(s, i, cmd.ifExists)) {
            error = "Expected exists";
            out.reset();
            return true;
        }

        if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
            out.reset();
            return true;
        }
        if (!requireEof(s, i, error)) {
            out.reset();
            return true;
        }
        out = cmd;
        return true;
    }

    if (matchKeyword(s, i, "keyspace")) {
        SqlDropKeyspace cmd;
        if (!ifExists(s, i, cmd.ifExists)) {
            error = "Expected exists";
            out.reset();
            return true;
        }
        if (!requireIdentifier(s, i, cmd.keyspace, error, "Expected keyspace")) {
            out.reset();
            return true;
        }
        if (!requireEof(s, i, error)) {
            out.reset();
            return true;
        }
        out = cmd;
        return true;
    }

    error = "Expected table or keyspace";
    out.reset();
    return true;
}

static bool tryParseTruncate(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "truncate"))
        return false;
    i = j;
    if (!requireKeyword(s, i, "table", error, "Expected table")) {
        out.reset();
        return true;
    }

    SqlTruncateTable cmd;
    if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
        out.reset();
        return true;
    }
    if (!requireEof(s, i, error)) {
        out.reset();
        return true;
    }
    out = cmd;
    return true;
}

static bool tryParseDescribe(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "describe"))
        return false;
    i = j;
    if (!requireKeyword(s, i, "table", error, "Expected table")) {
        out.reset();
        return true;
    }

    SqlDescribeTable cmd;
    if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
        out.reset();
        return true;
    }
    if (!requireEof(s, i, error)) {
        out.reset();
        return true;
    }
    out = cmd;
    return true;
}

static bool tryParseShow(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "show"))
        return false;
    i = j;

    {
        usize k = i;
        if (matchKeyword(s, k, "keyspaces")) {
            i = k;
            if (!requireEof(s, i, error)) {
                out.reset();
                return true;
            }
            out = SqlShowKeyspaces{};
            return true;
        }
    }

    {
        usize k = i;
        if (matchKeyword(s, k, "tables")) {
            i = k;
            SqlShowTables cmd;
            cmd.inKeyspace.reset();

            {
                usize m = i;
                if (matchKeyword(s, m, "in")) {
                    i = m;
                    string ks;
                    if (!requireIdentifier(s, i, ks, error, "Expected keyspace")) {
                        out.reset();
                        return true;
                    }
                    cmd.inKeyspace = ks;
                }
            }

            if (!requireEof(s, i, error)) {
                out.reset();
                return true;
            }
            out = cmd;
            return true;
        }
    }

    {
        usize k = i;
        if (matchKeyword(s, k, "create")) {
            i = k;
            if (!requireKeyword(s, i, "table", error, "Expected table")) {
                out.reset();
                return true;
            }

            SqlShowCreateTable cmd;
            if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
                out.reset();
                return true;
            }
            if (!requireEof(s, i, error)) {
                out.reset();
                return true;
            }
            out = cmd;
            return true;
        }
    }

    {
        usize k = i;
        if (matchKeyword(s, k, "metrics")) {
            i = k;
            if (!requireKeyword(s, i, "in", error, "Expected in")) {
                out.reset();
                return true;
            }
            SqlShowMetrics cmd;
            if (!requireIdentifier(s, i, cmd.keyspace, error, "Expected keyspace")) {
                out.reset();
                return true;
            }
            if (!requireEof(s, i, error)) {
                out.reset();
                return true;
            }
            out = cmd;
            return true;
        }
    }

    error = "Expected keyspaces, tables, create, or metrics";
    out.reset();
    return true;
}

static bool tryParseInsert(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "insert"))
        return false;
    i = j;
    if (!requireKeyword(s, i, "into", error, "Expected into")) {
        out.reset();
        return true;
    }

    SqlInsert cmd;
    if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
        out.reset();
        return true;
    }
    if (!requireChar(s, i, '(', error, "Expected (")) {
        out.reset();
        return true;
    }
    while (true) {
        string col;
        if (!requireIdentifier(s, i, col, error, "Expected column")) {
            out.reset();
            return true;
        }
        cmd.columns.push_back(col);
        if (consumeChar(s, i, ','))
            continue;
        if (consumeChar(s, i, ')'))
            break;
        error = "Expected , or )";
        out.reset();
        return true;
    }
    if (!requireKeyword(s, i, "values", error, "Expected values")) {
        out.reset();
        return true;
    }

    while (true) {
        if (!requireChar(s, i, '(', error, "Expected (")) {
            out.reset();
            return true;
        }

        vector<SqlLiteral> row;
        while (true) {
            SqlLiteral lit;
            if (!requireLiteral(s, i, lit, error, "Expected literal")) {
                out.reset();
                return true;
            }
            row.push_back(lit);
            if (consumeChar(s, i, ','))
                continue;
            if (consumeChar(s, i, ')'))
                break;
            error = "Expected , or )";
            out.reset();
            return true;
        }

        if (row.size() != cmd.columns.size()) {
            error = "Wrong values count";
            out.reset();
            return true;
        }
        cmd.rows.push_back(std::move(row));

        skipWhitespace(s, i);
        if (consumeChar(s, i, ','))
            continue;
        break;
    }

    out = cmd;
    return true;
}

static bool tryParseUpdate(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "update"))
        return false;
    i = j;
    SqlUpdate cmd;

    if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
        out.reset();
        return true;
    }

    if (!requireKeyword(s, i, "set", error, "Expected set")) {
        out.reset();
        return true;
    }

    bool anySet = false;
    while (true) {
        string col;
        if (!parseIdentifier(s, i, col)) {
            error = "Expected column";
            out.reset();
            return true;
        }
        if (!requireChar(s, i, '=', error, "Expected =")) {
            out.reset();
            return true;
        }
        SqlLiteral lit;
        if (!requireLiteral(s, i, lit, error, "Expected literal")) {
            out.reset();
            return true;
        }
        cmd.setColumns.push_back(col);
        cmd.setValues.push_back(lit);
        anySet = true;

        skipWhitespace(s, i);
        if (consumeChar(s, i, ','))
            continue;
        break;
    }

    if (!requireKeyword(s, i, "where", error, "Expected where")) {
        out.reset();
        return true;
    }
    if (!requireIdentifier(s, i, cmd.whereColumn, error, "Expected where column")) {
        out.reset();
        return true;
    }
    if (!requireChar(s, i, '=', error, "Expected =")) {
        out.reset();
        return true;
    }
    if (!requireLiteral(s, i, cmd.whereValue, error, "Expected literal")) {
        out.reset();
        return true;
    }
    if (!anySet) {
        error = "Expected set assignments";
        out.reset();
        return true;
    }

    out = cmd;
    return true;
}

static bool tryParseDelete(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "delete"))
        return false;
    i = j;
    if (!requireKeyword(s, i, "from", error, "Expected from")) {
        out.reset();
        return true;
    }

    SqlDelete cmd;
    if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
        out.reset();
        return true;
    }
    if (!requireKeyword(s, i, "where", error, "Expected where")) {
        out.reset();
        return true;
    }
    if (!requireIdentifier(s, i, cmd.whereColumn, error, "Expected where column")) {
        out.reset();
        return true;
    }
    if (!requireChar(s, i, '=', error, "Expected =")) {
        out.reset();
        return true;
    }
    if (!requireLiteral(s, i, cmd.whereValue, error, "Expected literal")) {
        out.reset();
        return true;
    }
    out = cmd;
    return true;
}

static bool tryParseSelect(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "select"))
        return false;
    i = j;
    SqlSelect cmd;
    if (consumeChar(s, i, '*')) {
    } else {
        while (true) {
            string col;
            if (!requireIdentifier(s, i, col, error, "Expected column")) {
                out.reset();
                return true;
            }
            cmd.columns.push_back(col);
            if (consumeChar(s, i, ','))
                continue;
            break;
        }
    }
    if (!requireKeyword(s, i, "from", error, "Expected from")) {
        out.reset();
        return true;
    }

    if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
        out.reset();
        return true;
    }

    {
        usize k = i;
        if (matchKeyword(s, k, "where")) {
            i = k;
            string col;
            if (!requireIdentifier(s, i, col, error, "Expected where column")) {
                out.reset();
                return true;
            }
            if (!requireChar(s, i, '=', error, "Expected =")) {
                out.reset();
                return true;
            }
            SqlLiteral lit;
            if (!requireLiteral(s, i, lit, error, "Expected literal")) {
                out.reset();
                return true;
            }
            cmd.whereColumn = col;
            cmd.whereValue = lit;
        }
    }

    {
        string col;
        bool desc = false;
        if (!orderByClause(s, i, col, desc, error)) {
            out.reset();
            return true;
        }
        if (!col.empty()) {
            cmd.orderByColumn = col;
            cmd.orderDesc = desc;
        }
    }

    {
        usize k = i;
        if (matchKeyword(s, k, "limit")) {
            i = k;
            string n;
            if (!numberToken(s, i, n)) {
                error = "Expected limit";
                out.reset();
                return true;
            }
            if (n.empty()) {
                error = "Expected integer limit";
                out.reset();
                return true;
            }
            for (char c : n) {
                if (c < '0' || c > '9') {
                    error = "Expected integer limit";
                    out.reset();
                    return true;
                }
            }
            try {
                cmd.limit = static_cast<usize>(std::stoull(n));
            } catch (const std::exception&) {
                error = "limit out of range";
                out.reset();
                return true;
            }
        }
    }

    if (!requireEof(s, i, error)) {
        out.reset();
        return true;
    }
    out = cmd;
    return true;
}

static bool tryParseFlush(stringView s, usize& i, std::optional<SqlCommand>& out, string& error) {
    usize j = i;
    if (!matchKeyword(s, j, "flush"))
        return false;
    i = j;

    SqlFlush cmd;
    if (!parseQualifiedName(s, i, cmd.keyspace, cmd.table, error, "Expected table")) {
        out.reset();
        return true;
    }
    out = cmd;
    return true;
}

}

std::optional<SqlCommand> sqlCommand(const string& rawLine, string& error) {
    error.clear();
    string line = stripTrailingSemicolon(rawLine);
    stringView s(line);
    usize i = 0;

    skipWhitespace(s, i);
    if (i >= s.size()) {
        error = "empty";
        return std::nullopt;
    }

    std::optional<SqlCommand> out;
    if (tryParsePing(s, i, out, error))
        return out;
    if (tryParseAuth(s, i, out, error))
        return out;
    if (tryParseUse(s, i, out, error))
        return out;
    if (tryParseCreate(s, i, out, error))
        return out;
    if (tryParseDrop(s, i, out, error))
        return out;
    if (tryParseTruncate(s, i, out, error))
        return out;
    if (tryParseDescribe(s, i, out, error))
        return out;
    if (tryParseShow(s, i, out, error))
        return out;
    if (tryParseInsert(s, i, out, error))
        return out;
    if (tryParseUpdate(s, i, out, error))
        return out;
    if (tryParseDelete(s, i, out, error))
        return out;
    if (tryParseSelect(s, i, out, error))
        return out;
    if (tryParseFlush(s, i, out, error))
        return out;

    error = "unknown";
    return std::nullopt;
}

}
