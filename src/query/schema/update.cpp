#include "query/schema.h"

#include "query/schema/detail/internal.h"

#include "util/binIo.h"

namespace xeondb {

byteVec mergeRowBytesForUpdate(const TableSchema& schema, const std::optional<byteVec>& existingRowBytes, const std::vector<string>& setColumns,
        const std::vector<SqlLiteral>& setValues) {
    if (setColumns.size() != setValues.size())
        throw runtimeError("set column/value count");

    std::vector<std::optional<SqlLiteral>> byIndex;
    byIndex.resize(schema.columns.size());
    for (usize i = 0; i < setColumns.size(); i++) {
        auto col = findColumnIndex(schema, setColumns[i]);
        if (!col.has_value())
            throw runtimeError("unknown column");
        if (*col == schema.primaryKeyIndex)
            throw runtimeError("cannot update pk");
        if (byIndex[*col].has_value())
            throw runtimeError("duplicate column");
        byIndex[*col] = setValues[i];
    }

    std::vector<bool> existingIsNull;
    std::vector<usize> existingValueOffsets;
    std::vector<usize> existingValueSizes;
    existingIsNull.resize(schema.columns.size(), true);
    existingValueOffsets.resize(schema.columns.size(), 0);
    existingValueSizes.resize(schema.columns.size(), 0);

    if (existingRowBytes.has_value()) {
        const auto& rowBytes = *existingRowBytes;
        usize o = 0;
        auto version = readU32(rowBytes, o);
        if (version != 1)
            throw runtimeError("bad row version");

        for (usize i = 0; i < schema.columns.size(); i++) {
            if (i == schema.primaryKeyIndex)
                continue;
            if (o >= rowBytes.size())
                throw runtimeError("bad row");
            u8 nullMarker = rowBytes[o++];
            if (nullMarker != 0) {
                existingIsNull[i] = true;
                continue;
            }
            existingIsNull[i] = false;
            usize before = o;
            schema_detail::skipValueBytes(schema.columns[i].type, rowBytes, o);
            existingValueOffsets[i] = before;
            existingValueSizes[i] = o - before;
        }
    }

    byteVec out;
    appendU32(out, 1);
    for (usize i = 0; i < schema.columns.size(); i++) {
        if (i == schema.primaryKeyIndex)
            continue;

        if (byIndex[i].has_value()) {
            const auto& lit = *byIndex[i];
            if (lit.kind == SqlLiteral::Kind::Null) {
                out.push_back(1);
            } else {
                out.push_back(0);
                schema_detail::appendValueBytes(out, schema.columns[i].type, lit);
            }
        } else {
            if (existingRowBytes.has_value() && !existingIsNull[i]) {
                out.push_back(0);
                const auto& rowBytes = *existingRowBytes;
                usize off = existingValueOffsets[i];
                usize sz = existingValueSizes[i];
                if (off + sz > rowBytes.size())
                    throw runtimeError("bad row");
                using Diff = std::vector<u8>::difference_type;
                out.insert(out.end(), rowBytes.begin() + static_cast<Diff>(off), rowBytes.begin() + static_cast<Diff>(off + sz));
            } else {
                out.push_back(1);
            }
        }
    }
    return out;
}

}
