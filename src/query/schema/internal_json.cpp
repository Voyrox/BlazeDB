#include "query/schema/detail/internal.h"

#include "util/binIo.h"
#include "util/encoding.h"
#include "util/json.h"

#include <cmath>
#include <cstring>

namespace xeondb::schema_detail {

std::string jsonValueFromBytes(ColumnType type, const byteVec& b, usize& o) {
    if (type == ColumnType::Text || type == ColumnType::Char) {
        u32 len = readU32(b, o);
        if (o + len > b.size())
            throw runtimeError("bad row");
        std::string s;
        s.assign(reinterpret_cast<const char*>(b.data() + o), reinterpret_cast<const char*>(b.data() + o + len));
        o += len;
        return std::string("\"") + jsonEscape(s) + "\"";
    }
    if (type == ColumnType::Blob) {
        u32 len = readU32(b, o);
        if (o + len > b.size())
            throw runtimeError("bad row");
        byteVec v;
        v.insert(v.end(), b.begin() + static_cast<byteVec::difference_type>(o), b.begin() + static_cast<byteVec::difference_type>(o + len));
        o += len;
        return std::string("\"") + jsonEscape(bytesToBase64(v)) + "\"";
    }
    if (type == ColumnType::Int32) {
        auto val = readBe32(b, o);
        return std::to_string(val);
    }
    if (type == ColumnType::Int64) {
        auto val = readBe64(b, o);
        return std::to_string(val);
    }
    if (type == ColumnType::Boolean) {
        if (o + 1 > b.size())
            throw runtimeError("bad row");
        bool val = b[o++] != 0;
        return val ? "true" : "false";
    }
    if (type == ColumnType::Float32) {
        if (o + 4 > b.size())
            throw runtimeError("bad row");
        u32 u = 0;
        u |= static_cast<u32>(b[o + 0]) << 24;
        u |= static_cast<u32>(b[o + 1]) << 16;
        u |= static_cast<u32>(b[o + 2]) << 8;
        u |= static_cast<u32>(b[o + 3]) << 0;
        o += 4;
        float f;
        std::memcpy(&f, &u, 4);
        if (!std::isfinite(f))
            return "null";
        return std::to_string(static_cast<double>(f));
    }
    if (type == ColumnType::Date) {
        auto days = readBe32(b, o);
        return std::string("\"") + dateFromDays(days) + "\"";
    }
    if (type == ColumnType::Timestamp) {
        auto ms = readBe64(b, o);
        return std::string("\"") + timestampFromMs(ms) + "\"";
    }
    throw runtimeError("bad type");
}

std::string jsonPkValue(ColumnType type, const byteVec& pkBytes) {
    byteVec b;
    usize o = 0;
    if (type == ColumnType::Text || type == ColumnType::Char) {
        std::string s(reinterpret_cast<const char*>(pkBytes.data()), reinterpret_cast<const char*>(pkBytes.data() + pkBytes.size()));
        return std::string("\"") + jsonEscape(s) + "\"";
    }
    if (type == ColumnType::Blob) {
        return std::string("\"") + jsonEscape(bytesToBase64(pkBytes)) + "\"";
    }
    if (type == ColumnType::Int32) {
        if (pkBytes.size() != 4)
            throw runtimeError("bad pk");
        o = 0;
        b = pkBytes;
        return std::to_string(readBe32(b, o));
    }
    if (type == ColumnType::Int64) {
        if (pkBytes.size() != 8)
            throw runtimeError("bad pk");
        o = 0;
        b = pkBytes;
        return std::to_string(readBe64(b, o));
    }
    if (type == ColumnType::Boolean) {
        if (pkBytes.size() != 1)
            throw runtimeError("bad pk");
        return pkBytes[0] ? "true" : "false";
    }
    if (type == ColumnType::Float32) {
        if (pkBytes.size() != 4)
            throw runtimeError("bad pk");
        u32 u = 0;
        u |= static_cast<u32>(pkBytes[0]) << 24;
        u |= static_cast<u32>(pkBytes[1]) << 16;
        u |= static_cast<u32>(pkBytes[2]) << 8;
        u |= static_cast<u32>(pkBytes[3]) << 0;
        float f;
        std::memcpy(&f, &u, 4);
        return std::to_string(static_cast<double>(f));
    }
    if (type == ColumnType::Date) {
        if (pkBytes.size() != 4)
            throw runtimeError("bad pk");
        b = pkBytes;
        o = 0;
        return std::string("\"") + dateFromDays(readBe32(b, o)) + "\"";
    }
    if (type == ColumnType::Timestamp) {
        if (pkBytes.size() != 8)
            throw runtimeError("bad pk");
        b = pkBytes;
        o = 0;
        return std::string("\"") + timestampFromMs(readBe64(b, o)) + "\"";
    }
    return "null";
}

}
