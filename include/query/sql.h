#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "query/schema.h"

using std::string;
using std::vector;

namespace xeondb {

struct SqlPing {};

struct SqlAuth {
    string username;
    string password;
};

struct SqlUse {
    string keyspace;
};

struct SqlCreateKeyspace {
    string keyspace;
    bool ifNotExists;
};

struct SqlCreateTable {
    string keyspace;
    string table;
    bool ifNotExists;
    TableSchema schema;
};

struct SqlInsert {
    string keyspace;
    string table;
    vector<string> columns;
    vector<vector<SqlLiteral>> rows;
};

struct SqlSelect {
    string keyspace;
    string table;
    vector<string> columns;
    std::optional<string> whereColumn;
    std::optional<SqlLiteral> whereValue;

    std::optional<string> orderByColumn;
    bool orderDesc = false;

    std::optional<usize> limit;
};

struct SqlFlush {
    string keyspace;
    string table;
};

struct SqlDropTable {
    string keyspace;
    string table;
    bool ifExists;
};

struct SqlDropKeyspace {
    string keyspace;
    bool ifExists;
};

struct SqlShowKeyspaces {};

struct SqlShowTables {
    std::optional<string> inKeyspace;
};

struct SqlDescribeTable {
    string keyspace;
    string table;
};

struct SqlShowCreateTable {
    string keyspace;
    string table;
};

struct SqlShowMetrics {
    string keyspace;
};

struct SqlTruncateTable {
    string keyspace;
    string table;
};

struct SqlDelete {
    string keyspace;
    string table;
    string whereColumn;
    SqlLiteral whereValue;
};

struct SqlUpdate {
    string keyspace;
    string table;
    vector<string> setColumns;
    vector<SqlLiteral> setValues;
    string whereColumn;
    SqlLiteral whereValue;
};

using SqlCommand = std::variant<SqlPing, SqlAuth, SqlUse, SqlCreateKeyspace, SqlCreateTable, SqlInsert, SqlSelect, SqlFlush, SqlDelete, SqlUpdate, SqlDropTable,
        SqlDropKeyspace, SqlShowKeyspaces, SqlShowTables, SqlDescribeTable, SqlShowCreateTable, SqlShowMetrics, SqlTruncateTable>;

std::optional<SqlCommand> sqlCommand(const string& line, string& error);

}
