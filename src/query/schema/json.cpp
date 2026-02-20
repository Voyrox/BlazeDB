#include "query/schema.h"

#include "query/schema/detail/internal.h"

#include "util/binIo.h"
#include "util/json.h"

namespace xeondb {

string rowToJson(const TableSchema& schema, const byteVec& pkBytes, const byteVec& rowBytes, const std::vector<string>& selectColumns) {
    std::vector<string> cols;
    if (selectColumns.empty()) {
        for (const auto& column : schema.columns)
            cols.push_back(column.name);
    } else {
        cols = selectColumns;
    }

    usize num = 0;
    auto version = readU32(rowBytes, num);
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
        if (num >= rowBytes.size())
            throw runtimeError("bad row");
        u8 row = rowBytes[num++];
        if (row != 0) {
            isNull[i] = true;
            continue;
        }
        isNull[i] = false;
        usize before = num;
        (void)schema_detail::jsonValueFromBytes(schema.columns[i].type, rowBytes, num);
        valueOffsets[i] = before;
        valueSizes[i] = num - before;
    }

    string out = "{";
    bool first = true;
    for (const auto& name : cols) {
        auto columnIndex = findColumnIndex(schema, name);
        if (!columnIndex.has_value())
            throw runtimeError("unknown column");
        usize i = *columnIndex;
        if (!first)
            out += ",";
        first = false;
        out += "\"" + jsonEscape(name) + "\":";
        if (i == schema.primaryKeyIndex) {
            out += schema_detail::jsonPkValue(schema.columns[i].type, pkBytes);
        } else {
            if (isNull[i]) {
                out += "null";
            } else {
                usize offset = valueOffsets[i];
                out += schema_detail::jsonValueFromBytes(schema.columns[i].type, rowBytes, offset);
            }
        }
    }
    out += "}";
    return out;
}

}
