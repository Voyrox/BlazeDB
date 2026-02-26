#pragma once

#include "prelude.h"

#include <optional>
#include <utility>

using std::string;

namespace xeondb {

enum class ColumnType : u8 { Char = 1, Text = 2, Blob = 3, Int32 = 4, Int64 = 5, Boolean = 6, Float32 = 7, Date = 8, Timestamp = 9 };

struct ColumnDef {
    std::string name;
    ColumnType type;
};

struct TableSchema {
    std::vector<ColumnDef> columns;
    usize primaryKeyIndex;
};

struct SqlLiteral {
    enum class Kind : u8 { Null = 1, Number = 2, Bool = 3, Quoted = 4, Hex = 5, Base64 = 6 };

    Kind kind;
    string text;
};

std::optional<ColumnType> columnTypeFromName(const string& s);
std::string columnTypeName(ColumnType t);

std::optional<usize> findColumnIndex(const TableSchema& schema, const string& name);

byteVec partitionKeyBytes(ColumnType type, const SqlLiteral& lit);

byteVec rowBytes(const TableSchema& schema, const std::vector<string>& columnNames, const std::vector<SqlLiteral>& values, const byteVec& pkBytes);

string dateFromDays(i32 days);
string timestampFromMs(i64 ms);

string rowToJsonMapped(const TableSchema& schema, const byteVec& pkBytes, const byteVec& rowBytes, const std::vector<std::pair<string, string>>& selectColumns);

string rowToJson(const TableSchema& schema, const byteVec& pkBytes, const byteVec& rowBytes, const std::vector<string>& selectColumns);

byteVec mergeRowBytesForUpdate(const TableSchema& schema, const std::optional<byteVec>& existingRowBytes, const std::vector<string>& setColumns,
        const std::vector<SqlLiteral>& setValues);

}
