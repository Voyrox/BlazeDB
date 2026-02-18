#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "query/schema.h"

using std::string;
using std::vector;

namespace blazeDb
{

struct SqlPing
{
};

struct SqlCreateKeyspace
{
    string keyspace;
    bool ifNotExists;
};

struct SqlCreateTable
{
    string keyspace;
    string table;
    bool ifNotExists;
    TableSchema schema;
};

struct SqlInsert
{
    string keyspace;
    string table;
    vector<string> columns;
    vector<vector<SqlLiteral>> rows;
};

struct SqlSelect
{
    string keyspace;
    string table;
    vector<string> columns;
    string whereColumn;
    SqlLiteral whereValue;
};

struct SqlFlush
{
    string keyspace;
    string table;
};

struct SqlDelete
{
    string keyspace;
    string table;
    string whereColumn;
    SqlLiteral whereValue;
};

using SqlCommand = std::variant<SqlPing, SqlCreateKeyspace, SqlCreateTable, SqlInsert, SqlSelect, SqlFlush, SqlDelete>;

std::optional<SqlCommand> parseSqlLine(const string& line, string& error);

}
