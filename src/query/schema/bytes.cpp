#include "query/schema/detail/internal.h"

#include "util/binIo.h"
#include "util/encoding.h"

#include <cstring>

namespace xeondb::schema_detail {

void appendValueBytes(byteVec& out, ColumnType type, const SqlLiteral& lit) {
    if (lit.kind == SqlLiteral::Kind::Null)
        throw runtimeError("null");
    switch (type) {
    case ColumnType::Char: {
        if (lit.kind != SqlLiteral::Kind::Quoted || lit.text.size() != 1)
            throw runtimeError("char");
        appendU32(out, 1);
        out.push_back(static_cast<u8>(lit.text[0]));
        return;
    }
    case ColumnType::Text: {
        if (lit.kind != SqlLiteral::Kind::Quoted)
            throw runtimeError("text");
        appendU32(out, static_cast<u32>(lit.text.size()));
        out.insert(out.end(), lit.text.begin(), lit.text.end());
        return;
    }
    case ColumnType::Blob: {
        byteVec b;
        if (lit.kind == SqlLiteral::Kind::Hex)
            b = hexToBytes(lit.text);
        else if (lit.kind == SqlLiteral::Kind::Base64)
            b = base64ToBytes(lit.text);
        else
            throw runtimeError("blob");
        appendU32(out, static_cast<u32>(b.size()));
        out.insert(out.end(), b.begin(), b.end());
        return;
    }
    case ColumnType::Int32: {
        if (lit.kind != SqlLiteral::Kind::Number)
            throw runtimeError("int32");
        appendBe32(out, parseI32(lit.text));
        return;
    }
    case ColumnType::Int64: {
        if (lit.kind != SqlLiteral::Kind::Number)
            throw runtimeError("int64");
        appendBe64(out, parseI64(lit.text));
        return;
    }
    case ColumnType::Boolean: {
        if (lit.kind != SqlLiteral::Kind::Bool)
            throw runtimeError("bool");
        out.push_back((toLower(lit.text) == "true") ? 1 : 0);
        return;
    }
    case ColumnType::Float32: {
        if (lit.kind != SqlLiteral::Kind::Number)
            throw runtimeError("float");
        float f = parseF32(lit.text);
        u32 u;
        std::memcpy(&u, &f, 4);
        out.push_back(static_cast<u8>((u >> 24) & 0xFF));
        out.push_back(static_cast<u8>((u >> 16) & 0xFF));
        out.push_back(static_cast<u8>((u >> 8) & 0xFF));
        out.push_back(static_cast<u8>((u >> 0) & 0xFF));
        return;
    }
    case ColumnType::Date: {
        if (lit.kind != SqlLiteral::Kind::Quoted)
            throw runtimeError("date");
        appendBe32(out, parseDateDays(lit.text));
        return;
    }
    case ColumnType::Timestamp: {
        if (lit.kind == SqlLiteral::Kind::Number) {
            appendBe64(out, parseI64(lit.text));
            return;
        }
        if (lit.kind != SqlLiteral::Kind::Quoted)
            throw runtimeError("timestamp requires quoted");
        appendBe64(out, parseTimestampMs(lit.text));
        return;
    }
    default:
        throw runtimeError("bad type");
    }
}

void skipValueBytes(ColumnType type, const byteVec& b, usize& o) {
    if (type == ColumnType::Text || type == ColumnType::Char || type == ColumnType::Blob) {
        u32 len = readU32(b, o);
        if (o + len > b.size())
            throw runtimeError("bad row");
        o += len;
        return;
    }
    if (type == ColumnType::Int32 || type == ColumnType::Float32 || type == ColumnType::Date) {
        if (o + 4 > b.size())
            throw runtimeError("bad row");
        o += 4;
        return;
    }
    if (type == ColumnType::Int64 || type == ColumnType::Timestamp) {
        if (o + 8 > b.size())
            throw runtimeError("bad row");
        o += 8;
        return;
    }
    if (type == ColumnType::Boolean) {
        if (o + 1 > b.size())
            throw runtimeError("bad row");
        o += 1;
        return;
    }
    throw runtimeError("bad type");
}

}
