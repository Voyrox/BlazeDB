#include "query/schema.h"

#include "query/schema/detail/internal.h"

#include "util/binIo.h"
#include "util/encoding.h"

#include <cstring>

namespace xeondb {

byteVec partitionKeyBytes(ColumnType type, const SqlLiteral& lit) {
    if (lit.kind == SqlLiteral::Kind::Null)
        throw runtimeError("pk cannot be null");
    byteVec out;
    switch (type) {
    case ColumnType::Char: {
        if (lit.kind != SqlLiteral::Kind::Quoted)
            throw runtimeError("char requires quoted");
        if (lit.text.size() != 1)
            throw runtimeError("char length");
        out.assign(lit.text.begin(), lit.text.end());
        return out;
    }
    case ColumnType::Text: {
        if (lit.kind != SqlLiteral::Kind::Quoted)
            throw runtimeError("text requires quoted");
        out.assign(lit.text.begin(), lit.text.end());
        return out;
    }
    case ColumnType::Blob: {
        if (lit.kind == SqlLiteral::Kind::Hex)
            return hexToBytes(lit.text);
        if (lit.kind == SqlLiteral::Kind::Base64)
            return base64ToBytes(lit.text);
        throw runtimeError("blob requires 0x or b64");
    }
    case ColumnType::Int32: {
        if (lit.kind != SqlLiteral::Kind::Number)
            throw runtimeError("int32 requires number");
        appendBe32(out, schema_detail::parseI32(lit.text));
        return out;
    }
    case ColumnType::Int64: {
        if (lit.kind != SqlLiteral::Kind::Number)
            throw runtimeError("int64 requires number");
        appendBe64(out, schema_detail::parseI64(lit.text));
        return out;
    }
    case ColumnType::Boolean: {
        if (lit.kind != SqlLiteral::Kind::Bool)
            throw runtimeError("boolean requires bool");
        out.push_back((schema_detail::toLower(lit.text) == "true") ? 1 : 0);
        return out;
    }
    case ColumnType::Float32: {
        if (lit.kind != SqlLiteral::Kind::Number)
            throw runtimeError("float requires number");
        float f = schema_detail::parseF32(lit.text);
        u32 u;
        static_assert(sizeof(float) == 4);
        std::memcpy(&u, &f, 4);
        out.push_back(static_cast<u8>((u >> 24) & 0xFF));
        out.push_back(static_cast<u8>((u >> 16) & 0xFF));
        out.push_back(static_cast<u8>((u >> 8) & 0xFF));
        out.push_back(static_cast<u8>((u >> 0) & 0xFF));
        return out;
    }
    case ColumnType::Date: {
        if (lit.kind != SqlLiteral::Kind::Quoted)
            throw runtimeError("date requires quoted");
        appendBe32(out, schema_detail::parseDateDays(lit.text));
        return out;
    }
    case ColumnType::Timestamp: {
        if (lit.kind == SqlLiteral::Kind::Number) {
            appendBe64(out, schema_detail::parseI64(lit.text));
            return out;
        }
        if (lit.kind != SqlLiteral::Kind::Quoted)
            throw runtimeError("timestamp requires quoted");
        appendBe64(out, schema_detail::parseTimestampMs(lit.text));
        return out;
    }
    default:
        throw runtimeError("bad type");
    }
}

byteVec rowBytes(const TableSchema& schema, const std::vector<string>& columnNames, const std::vector<SqlLiteral>& values, const byteVec& pkBytes) {
    (void)pkBytes;
    if (columnNames.size() != values.size())
        throw runtimeError("column/value count");

    auto pkName = schema.columns[schema.primaryKeyIndex].name;
    bool pkPresent = false;
    for (const auto& colName : columnNames) {
        if (colName == pkName)
            pkPresent = true;
    }
    if (!pkPresent)
        throw runtimeError("missing pk");

    std::vector<std::optional<SqlLiteral>> byIndex;
    byIndex.resize(schema.columns.size());
    for (usize i = 0; i < columnNames.size(); i++) {
        auto colIndex = findColumnIndex(schema, columnNames[i]);
        if (!colIndex.has_value())
            throw runtimeError("unknown column");
        byIndex[*colIndex] = values[i];
    }

    byteVec out;
    appendU32(out, 1);
    for (usize i = 0; i < schema.columns.size(); i++) {
        if (i == schema.primaryKeyIndex)
            continue;
        if (!byIndex[i].has_value() || byIndex[i]->kind == SqlLiteral::Kind::Null) {
            out.push_back(1);
            continue;
        }
        out.push_back(0);
        schema_detail::appendValueBytes(out, schema.columns[i].type, *byIndex[i]);
    }
    return out;
}

}
