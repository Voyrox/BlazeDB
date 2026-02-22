#include "query/schema.h"

#include "query/schema/detail/internal.h"

namespace xeondb {

std::optional<ColumnType> columnTypeFromName(const string& s) {
    auto type = schema_detail::toLower(s);
    if (type == "char")
        return ColumnType::Char;
    if (type == "varchar" || type == "string" || type == "text")
        return ColumnType::Text;
    if (type == "binary" || type == "blob")
        return ColumnType::Blob;
    if (type == "int" || type == "int32")
        return ColumnType::Int32;
    if (type == "int64" || type == "bigint")
        return ColumnType::Int64;
    if (type == "boolean" || type == "bool")
        return ColumnType::Boolean;
    if (type == "float")
        return ColumnType::Float32;
    if (type == "date")
        return ColumnType::Date;
    if (type == "timestamp")
        return ColumnType::Timestamp;
    return std::nullopt;
}

string columnTypeName(ColumnType t) {
    switch (t) {
    case ColumnType::Char:
        return "char";
    case ColumnType::Text:
        return "varchar";
    case ColumnType::Blob:
        return "binary";
    case ColumnType::Int32:
        return "int32";
    case ColumnType::Int64:
        return "int64";
    case ColumnType::Boolean:
        return "boolean";
    case ColumnType::Float32:
        return "float";
    case ColumnType::Date:
        return "date";
    case ColumnType::Timestamp:
        return "timestamp";
    default:
        return "unknown";
    }
}

std::optional<usize> findColumnIndex(const TableSchema& schema, const string& name) {
    for (usize i = 0; i < schema.columns.size(); i++) {
        if (schema.columns[i].name == name) {
            return i;
        }
    }
    return std::nullopt;
}

}
