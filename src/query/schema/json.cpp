#include "query/schema.h"

#include "query/schema/detail/internal.h"

#include "util/binIo.h"
#include "util/json.h"

namespace xeondb {

string rowToJsonMapped(
        const TableSchema& schema, const byteVec& pkBytes, const byteVec& rowBytes, const std::vector<std::pair<string, string>>& selectColumns) {
    std::vector<string> cols;
    std::vector<std::pair<string, string>> mapped;
    if (selectColumns.empty()) {
        for (const auto& column : schema.columns)
            mapped.push_back({column.name, column.name});
    } else {
        mapped = selectColumns;
    }

    usize offset = 0;
    auto version = readBeU32(rowBytes, offset);
    if (version != 1)
        throw runtimeError("bad row version");

    std::vector<bool> isNull;
    isNull.resize(schema.columns.size(), true);
    std::vector<usize> valueOffsets;
    valueOffsets.resize(schema.columns.size(), 0);
    std::vector<usize> valueSizes;
    valueSizes.resize(schema.columns.size(), 0);

    for (usize i = 0; i < schema.columns.size(); i++) {
        if (i == schema.primaryKeyIndex)
            continue;
        if (offset >= rowBytes.size())
            throw runtimeError("bad row");
        u8 row = rowBytes[offset++];
        if (row != 0) {
            isNull[i] = true;
            continue;
        }
        isNull[i] = false;
        usize before = offset;
        (void)schema_detail::jsonValueFromBytes(schema.columns[i].type, rowBytes, offset);
        valueOffsets[i] = before;
        valueSizes[i] = offset - before;
    }

    string out = "{";
    bool first = true;
    for (const auto& it : mapped) {
        const auto& outName = it.first;
        const auto& sourceName = it.second;
        auto columnIndex = findColumnIndex(schema, sourceName);
        if (!columnIndex.has_value())
            throw runtimeError("unknown column");
        usize i = *columnIndex;
        if (!first)
            out += ",";
        first = false;
        out += "\"" + jsonEscape(outName) + "\":";
        if (i == schema.primaryKeyIndex) {
            out += schema_detail::jsonPkValue(schema.columns[i].type, pkBytes);
        } else {
            if (isNull[i]) {
                out += "null";
            } else {
                usize valueOffset = valueOffsets[i];
                out += schema_detail::jsonValueFromBytes(schema.columns[i].type, rowBytes, valueOffset);
            }
        }
    }
    out += "}";
    return out;
}

string rowToJson(const TableSchema& schema, const byteVec& pkBytes, const byteVec& rowBytes, const std::vector<string>& selectColumns) {
    std::vector<std::pair<string, string>> mapped;
    mapped.reserve(selectColumns.size());
    for (const auto& c : selectColumns)
        mapped.push_back({c, c});
    return rowToJsonMapped(schema, pkBytes, rowBytes, mapped);
}

}
