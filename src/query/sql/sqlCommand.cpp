#include "query/sql.h"

#include "query/sql/detail/parse_utils.h"

using std::string;
using std::vector;

namespace xeondb {

std::optional<SqlCommand> sqlCommand(const string& rawLine, string& error) {
    using namespace sql_detail;

    error.clear();
    string line = stripTrailingSemicolon(rawLine);
    stringView s(line);
    usize i = 0;

    skipWhitespace(s, i);
    if (i >= s.size()) {
        error = "empty";
        return std::nullopt;
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "ping"))
            return SqlPing{};
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "auth")) {
            i = j;
            SqlAuth cmd;
            if (!parseQuoted(s, i, cmd.username)) {
                error = "Expected username";
                return std::nullopt;
            }
            if (!parseQuoted(s, i, cmd.password)) {
                error = "Expected password";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "use")) {
            i = j;
            SqlUse cmd;
            if (!parseIdentifier(s, i, cmd.keyspace)) {
                error = "Expected keyspace";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "create")) {
            i = j;
            if (matchKeyword(s, i, "keyspace")) {
                SqlCreateKeyspace cmd;
                if (!ifNotExists(s, i, cmd.ifNotExists)) {
                    error = "Expected not exists";
                    return std::nullopt;
                }
                if (!parseIdentifier(s, i, cmd.keyspace)) {
                    error = "Expected keyspace";
                    return std::nullopt;
                }
                return cmd;
            }
            if (matchKeyword(s, i, "table")) {
                SqlCreateTable cmd;
                if (!ifNotExists(s, i, cmd.ifNotExists)) {
                    error = "Expected not exists";
                    return std::nullopt;
                }

                string firstName;
                if (!parseIdentifier(s, i, firstName)) {
                    error = "Expected table";
                    return std::nullopt;
                }
                if (consumeChar(s, i, '.')) {
                    cmd.keyspace = firstName;
                    if (!parseIdentifier(s, i, cmd.table)) {
                        error = "Expected table";
                        return std::nullopt;
                    }
                } else {
                    cmd.keyspace.clear();
                    cmd.table = firstName;
                }
                if (!consumeChar(s, i, '(')) {
                    error = "Expected (";
                    return std::nullopt;
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
                        if (!matchKeyword(s, i, "key")) {
                            error = "Expected key";
                            return std::nullopt;
                        }
                        if (!consumeChar(s, i, '(')) {
                            error = "Expected (";
                            return std::nullopt;
                        }
                        if (!parseIdentifier(s, i, pkName)) {
                            error = "Expected pk";
                            return std::nullopt;
                        }
                        if (!consumeChar(s, i, ')')) {
                            error = "Expected )";
                            return std::nullopt;
                        }
                    } else {
                        string colName;
                        string typeNameStr;
                        if (!parseIdentifier(s, i, colName)) {
                            error = "Expected column";
                            return std::nullopt;
                        }
                        if (!typeName(s, i, typeNameStr)) {
                            error = "Expected type";
                            return std::nullopt;
                        }
                        auto ct = columnTypeFromName(typeNameStr);
                        if (!ct.has_value()) {
                            error = "unknown type";
                            return std::nullopt;
                        }
                        cols.push_back(ColumnDef{colName, *ct});
                    }

                    skipWhitespace(s, i);
                    if (consumeChar(s, i, ','))
                        continue;
                    if (consumeChar(s, i, ')'))
                        break;
                    error = "Expected , or )";
                    return std::nullopt;
                }

                if (pkName.empty()) {
                    error = "missing primary key";
                    return std::nullopt;
                }

                TableSchema schema;
                schema.columns = std::move(cols);
                auto col = findColumnIndex(schema, pkName);
                if (!col.has_value()) {
                    error = "pk not in columns";
                    return std::nullopt;
                }
                schema.primaryKeyIndex = *col;
                cmd.schema = std::move(schema);
                return cmd;
            }
            error = "Expected keyspace or table";
            return std::nullopt;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "drop")) {
            i = j;
            if (matchKeyword(s, i, "table")) {
                SqlDropTable cmd;
                if (!ifExists(s, i, cmd.ifExists)) {
                    error = "Expected exists";
                    return std::nullopt;
                }

                string firstName;
                if (!parseIdentifier(s, i, firstName)) {
                    error = "Expected table";
                    return std::nullopt;
                }
                if (consumeChar(s, i, '.')) {
                    cmd.keyspace = firstName;
                    if (!parseIdentifier(s, i, cmd.table)) {
                        error = "Expected table";
                        return std::nullopt;
                    }
                } else {
                    cmd.keyspace.clear();
                    cmd.table = firstName;
                }

                skipWhitespace(s, i);
                if (i != s.size()) {
                    error = "Unexpected trailing input";
                    return std::nullopt;
                }
                return cmd;
            }

            if (matchKeyword(s, i, "keyspace")) {
                SqlDropKeyspace cmd;
                if (!ifExists(s, i, cmd.ifExists)) {
                    error = "Expected exists";
                    return std::nullopt;
                }
                if (!parseIdentifier(s, i, cmd.keyspace)) {
                    error = "Expected keyspace";
                    return std::nullopt;
                }
                skipWhitespace(s, i);
                if (i != s.size()) {
                    error = "Unexpected trailing input";
                    return std::nullopt;
                }
                return cmd;
            }

            error = "Expected table or keyspace";
            return std::nullopt;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "truncate")) {
            i = j;
            if (!matchKeyword(s, i, "table")) {
                error = "Expected table";
                return std::nullopt;
            }

            SqlTruncateTable cmd;
            string firstName;
            if (!parseIdentifier(s, i, firstName)) {
                error = "Expected table";
                return std::nullopt;
            }
            if (consumeChar(s, i, '.')) {
                cmd.keyspace = firstName;
                if (!parseIdentifier(s, i, cmd.table)) {
                    error = "Expected table";
                    return std::nullopt;
                }
            } else {
                cmd.keyspace.clear();
                cmd.table = firstName;
            }

            skipWhitespace(s, i);
            if (i != s.size()) {
                error = "Unexpected trailing input";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "describe")) {
            i = j;
            if (!matchKeyword(s, i, "table")) {
                error = "Expected table";
                return std::nullopt;
            }
            SqlDescribeTable cmd;
            string firstName;
            if (!parseIdentifier(s, i, firstName)) {
                error = "Expected table";
                return std::nullopt;
            }
            if (consumeChar(s, i, '.')) {
                cmd.keyspace = firstName;
                if (!parseIdentifier(s, i, cmd.table)) {
                    error = "Expected table";
                    return std::nullopt;
                }
            } else {
                cmd.keyspace.clear();
                cmd.table = firstName;
            }
            skipWhitespace(s, i);
            if (i != s.size()) {
                error = "Unexpected trailing input";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "show")) {
            i = j;

            {
                usize k = i;
                if (matchKeyword(s, k, "keyspaces")) {
                    i = k;
                    skipWhitespace(s, i);
                    if (i != s.size()) {
                        error = "Unexpected trailing input";
                        return std::nullopt;
                    }
                    return SqlShowKeyspaces{};
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
                            if (!parseIdentifier(s, i, ks)) {
                                error = "Expected keyspace";
                                return std::nullopt;
                            }
                            cmd.inKeyspace = ks;
                        }
                    }

                    skipWhitespace(s, i);
                    if (i != s.size()) {
                        error = "Unexpected trailing input";
                        return std::nullopt;
                    }
                    return cmd;
                }
            }

            {
                usize k = i;
                if (matchKeyword(s, k, "create")) {
                    i = k;
                    if (!matchKeyword(s, i, "table")) {
                        error = "Expected table";
                        return std::nullopt;
                    }

                    SqlShowCreateTable cmd;
                    string firstName;
                    if (!parseIdentifier(s, i, firstName)) {
                        error = "Expected table";
                        return std::nullopt;
                    }
                    if (consumeChar(s, i, '.')) {
                        cmd.keyspace = firstName;
                        if (!parseIdentifier(s, i, cmd.table)) {
                            error = "Expected table";
                            return std::nullopt;
                        }
                    } else {
                        cmd.keyspace.clear();
                        cmd.table = firstName;
                    }
                    skipWhitespace(s, i);
                    if (i != s.size()) {
                        error = "Unexpected trailing input";
                        return std::nullopt;
                    }
                    return cmd;
                }
            }

            {
                usize k = i;
                if (matchKeyword(s, k, "metrics")) {
                    i = k;
                    if (!matchKeyword(s, i, "in")) {
                        error = "Expected in";
                        return std::nullopt;
                    }
                    SqlShowMetrics cmd;
                    if (!parseIdentifier(s, i, cmd.keyspace)) {
                        error = "Expected keyspace";
                        return std::nullopt;
                    }
                    skipWhitespace(s, i);
                    if (i != s.size()) {
                        error = "Unexpected trailing input";
                        return std::nullopt;
                    }
                    return cmd;
                }
            }

            error = "Expected keyspaces, tables, create, or metrics";
            return std::nullopt;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "insert")) {
            i = j;
            if (!matchKeyword(s, i, "into")) {
                error = "Expected into";
                return std::nullopt;
            }
            SqlInsert cmd;

            string firstName;
            if (!parseIdentifier(s, i, firstName)) {
                error = "Expected table";
                return std::nullopt;
            }
            if (consumeChar(s, i, '.')) {
                cmd.keyspace = firstName;
                if (!parseIdentifier(s, i, cmd.table)) {
                    error = "Expected table";
                    return std::nullopt;
                }
            } else {
                cmd.keyspace.clear();
                cmd.table = firstName;
            }
            if (!consumeChar(s, i, '(')) {
                error = "Expected (";
                return std::nullopt;
            }
            while (true) {
                string col;
                if (!parseIdentifier(s, i, col)) {
                    error = "Expected column";
                    return std::nullopt;
                }
                cmd.columns.push_back(col);
                if (consumeChar(s, i, ','))
                    continue;
                if (consumeChar(s, i, ')'))
                    break;
                error = "Expected , or )";
                return std::nullopt;
            }
            if (!matchKeyword(s, i, "values")) {
                error = "Expected values";
                return std::nullopt;
            }

            while (true) {
                if (!consumeChar(s, i, '(')) {
                    error = "Expected (";
                    return std::nullopt;
                }

                vector<SqlLiteral> row;
                while (true) {
                    SqlLiteral lit;
                    if (!literal(s, i, lit)) {
                        error = "Expected literal";
                        return std::nullopt;
                    }
                    row.push_back(lit);
                    if (consumeChar(s, i, ','))
                        continue;
                    if (consumeChar(s, i, ')'))
                        break;
                    error = "Expected , or )";
                    return std::nullopt;
                }

                if (row.size() != cmd.columns.size()) {
                    error = "Wrong values count";
                    return std::nullopt;
                }
                cmd.rows.push_back(std::move(row));

                skipWhitespace(s, i);
                if (consumeChar(s, i, ','))
                    continue;
                break;
            }

            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "update")) {
            i = j;
            SqlUpdate cmd;

            string firstName;
            if (!parseIdentifier(s, i, firstName)) {
                error = "Expected table";
                return std::nullopt;
            }
            if (consumeChar(s, i, '.')) {
                cmd.keyspace = firstName;
                if (!parseIdentifier(s, i, cmd.table)) {
                    error = "Expected table";
                    return std::nullopt;
                }
            } else {
                cmd.keyspace.clear();
                cmd.table = firstName;
            }

            if (!matchKeyword(s, i, "set")) {
                error = "Expected set";
                return std::nullopt;
            }

            bool anySet = false;
            while (true) {
                string col;
                if (!parseIdentifier(s, i, col)) {
                    if (!anySet)
                        error = "Expected column";
                    else
                        error = "Expected column";
                    return std::nullopt;
                }
                if (!consumeChar(s, i, '=')) {
                    error = "Expected =";
                    return std::nullopt;
                }
                SqlLiteral lit;
                if (!literal(s, i, lit)) {
                    error = "Expected literal";
                    return std::nullopt;
                }
                cmd.setColumns.push_back(col);
                cmd.setValues.push_back(lit);
                anySet = true;

                skipWhitespace(s, i);
                if (consumeChar(s, i, ','))
                    continue;
                break;
            }

            if (!matchKeyword(s, i, "where")) {
                error = "Expected where";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.whereColumn)) {
                error = "Expected where column";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '=')) {
                error = "Expected =";
                return std::nullopt;
            }
            if (!literal(s, i, cmd.whereValue)) {
                error = "Expected literal";
                return std::nullopt;
            }
            if (!anySet) {
                error = "Expected set assignments";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "delete")) {
            i = j;
            if (!matchKeyword(s, i, "from")) {
                error = "Expected from";
                return std::nullopt;
            }
            SqlDelete cmd;

            string firstName;
            if (!parseIdentifier(s, i, firstName)) {
                error = "Expected table";
                return std::nullopt;
            }
            if (consumeChar(s, i, '.')) {
                cmd.keyspace = firstName;
                if (!parseIdentifier(s, i, cmd.table)) {
                    error = "Expected table";
                    return std::nullopt;
                }
            } else {
                cmd.keyspace.clear();
                cmd.table = firstName;
            }
            if (!matchKeyword(s, i, "where")) {
                error = "Expected where";
                return std::nullopt;
            }
            if (!parseIdentifier(s, i, cmd.whereColumn)) {
                error = "Expected where column";
                return std::nullopt;
            }
            if (!consumeChar(s, i, '=')) {
                error = "Expected =";
                return std::nullopt;
            }
            if (!literal(s, i, cmd.whereValue)) {
                error = "Expected literal";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "select")) {
            i = j;
            SqlSelect cmd;
            if (consumeChar(s, i, '*')) {
            } else {
                while (true) {
                    string col;
                    if (!parseIdentifier(s, i, col)) {
                        error = "Expected column";
                        return std::nullopt;
                    }
                    cmd.columns.push_back(col);
                    if (consumeChar(s, i, ','))
                        continue;
                    break;
                }
            }
            if (!matchKeyword(s, i, "from")) {
                error = "Expected from";
                return std::nullopt;
            }

            string firstName;
            if (!parseIdentifier(s, i, firstName)) {
                error = "Expected table";
                return std::nullopt;
            }
            if (consumeChar(s, i, '.')) {
                cmd.keyspace = firstName;
                if (!parseIdentifier(s, i, cmd.table)) {
                    error = "Expected table";
                    return std::nullopt;
                }
            } else {
                cmd.keyspace.clear();
                cmd.table = firstName;
            }

            {
                usize k = i;
                if (matchKeyword(s, k, "where")) {
                    i = k;
                    string col;
                    if (!parseIdentifier(s, i, col)) {
                        error = "Expected where column";
                        return std::nullopt;
                    }
                    if (!consumeChar(s, i, '=')) {
                        error = "Expected =";
                        return std::nullopt;
                    }
                    SqlLiteral lit;
                    if (!literal(s, i, lit)) {
                        error = "Expected literal";
                        return std::nullopt;
                    }
                    cmd.whereColumn = col;
                    cmd.whereValue = lit;
                }
            }

            {
                string col;
                bool desc = false;
                if (!orderByClause(s, i, col, desc, error))
                    return std::nullopt;
                if (!col.empty()) {
                    cmd.orderByColumn = col;
                    cmd.orderDesc = desc;
                }
            }

            skipWhitespace(s, i);
            if (i != s.size()) {
                error = "Unexpected trailing input";
                return std::nullopt;
            }
            return cmd;
        }
    }

    {
        usize j = i;
        if (matchKeyword(s, j, "flush")) {
            i = j;
            SqlFlush cmd;

            string firstName;
            if (!parseIdentifier(s, i, firstName)) {
                error = "Expected table";
                return std::nullopt;
            }
            if (consumeChar(s, i, '.')) {
                cmd.keyspace = firstName;
                if (!parseIdentifier(s, i, cmd.table)) {
                    error = "Expected table";
                    return std::nullopt;
                }
            } else {
                cmd.keyspace.clear();
                cmd.table = firstName;
            }
            return cmd;
        }
    }

    error = "unknown";
    return std::nullopt;
}

}
