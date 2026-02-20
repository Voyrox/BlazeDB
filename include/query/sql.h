#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "query/schema.h"

using std::string;
using std::vector;

namespace xeondb
{

struct SqlPing
{
};

struct SqlUse
{
    string keyspace;
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
    std::optional<string> whereColumn;
    std::optional<SqlLiteral> whereValue;

    std::optional<string> orderByColumn;
    bool orderDesc = false;
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

struct SqlUpdate
{
    string keyspace;
    string table;
    vector<string> setColumns;
    vector<SqlLiteral> setValues;
    string whereColumn;
    SqlLiteral whereValue;
};

using SqlCommand = std::variant<SqlPing, SqlUse, SqlCreateKeyspace, SqlCreateTable, SqlInsert, SqlSelect, SqlFlush, SqlDelete, SqlUpdate>;

std::optional<SqlCommand> sqlCommand(const string& line, string& error);

}
