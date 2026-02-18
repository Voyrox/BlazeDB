#include "query/schema.h"

#include "util/binIo.h"
#include "util/encoding.h"
#include "util/json.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

using std::string;

namespace blazeDb
{

    static string toLower(string s)
    {
        for (char &c : s)
        {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        return s;
    }

    std::optional<ColumnType> columnTypeFromName(const string &s)
    {
        auto type = toLower(s);
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

    string columnTypeName(ColumnType t)
    {
        switch (t)
        {
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

    std::optional<usize> findColumnIndex(const TableSchema &schema, const string &name)
    {
        for (usize i = 0; i < schema.columns.size(); i++)
        {
            if (schema.columns[i].name == name)
            {
                return i;
            }
        }
        return std::nullopt;
    }

    static i32 parseI32(const string &s)
    {
        usize i = 0;
        long long v = std::stoll(s, &i, 10);
        if (i != s.size())
            throw runtimeError("bad int");
        if (v < -2147483648LL || v > 2147483647LL)
            throw runtimeError("int32 overflow");
        return static_cast<i32>(v);
    }

    static i64 parseI64(const string &s)
    {
        usize i = 0;
        long long v = std::stoll(s, &i, 10);
        if (i != s.size())
            throw runtimeError("bad int");
        return static_cast<i64>(v);
    }

    static float parseF32(const string &s)
    {
        usize i = 0;
        float val = std::stof(s, &i);
        if (i != s.size())
            throw runtimeError("bad float");
        return val;
    }

    static i32 parseDateDays(const string &s)
    {
        if (s.size() != 10 || s[4] != '-' || s[7] != '-')
            throw runtimeError("bad date");
        int y = std::stoi(s.substr(0, 4));
        unsigned m = static_cast<unsigned>(std::stoi(s.substr(5, 2)));
        unsigned d = static_cast<unsigned>(std::stoi(s.substr(8, 2)));
        std::chrono::year_month_day ymd{std::chrono::year{y}, std::chrono::month{m}, std::chrono::day{d}};
        if (!ymd.ok())
            throw runtimeError("bad date");
        auto days = std::chrono::sys_days{ymd}.time_since_epoch();
        return static_cast<i32>(std::chrono::duration_cast<std::chrono::days>(days).count());
    }

    static i64 parseTimestampMs(const string &s)
    {
        if (s.size() < 20)
            throw runtimeError("bad timestamp");
        if (s[4] != '-' || s[7] != '-' || (s[10] != 'T' && s[10] != 't'))
            throw runtimeError("bad timestamp");
        int y = std::stoi(s.substr(0, 4));
        unsigned mo = static_cast<unsigned>(std::stoi(s.substr(5, 2)));
        unsigned da = static_cast<unsigned>(std::stoi(s.substr(8, 2)));
        unsigned hh = static_cast<unsigned>(std::stoi(s.substr(11, 2)));
        unsigned mm = static_cast<unsigned>(std::stoi(s.substr(14, 2)));
        unsigned ss = static_cast<unsigned>(std::stoi(s.substr(17, 2)));

        usize pos = 19;
        int ms = 0;
        if (pos < s.size() && s[pos] == '.')
        {
            pos++;
            int mult = 100;
            int count = 0;
            while (pos < s.size() && std::isdigit(static_cast<unsigned char>(s[pos])) && count < 3)
            {
                ms += (s[pos] - '0') * mult;
                mult /= 10;
                pos++;
                count++;
            }
            while (pos < s.size() && std::isdigit(static_cast<unsigned char>(s[pos])))
                pos++;
        }
        if (pos >= s.size() || (s[pos] != 'Z' && s[pos] != 'z'))
            throw runtimeError("timestamp must end with Z");

        std::chrono::year_month_day ymd{std::chrono::year{y}, std::chrono::month{mo}, std::chrono::day{da}};
        if (!ymd.ok())
            throw runtimeError("bad timestamp");
        auto dayPoint = std::chrono::sys_days{ymd};
        auto tp = dayPoint + std::chrono::hours{hh} + std::chrono::minutes{mm} + std::chrono::seconds{ss} + std::chrono::milliseconds{ms};
        return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
    }

    byteVec encodePartitionKeyBytes(ColumnType type, const SqlLiteral &lit)
    {
        if (lit.kind == SqlLiteral::Kind::Null)
            throw runtimeError("pk cannot be null");
        byteVec out;
        switch (type)
        {
        case ColumnType::Char:
        {
            if (lit.kind != SqlLiteral::Kind::Quoted)
                throw runtimeError("char requires quoted");
            if (lit.text.size() != 1)
                throw runtimeError("char length");
            out.assign(lit.text.begin(), lit.text.end());
            return out;
        }
        case ColumnType::Text:
        {
            if (lit.kind != SqlLiteral::Kind::Quoted)
                throw runtimeError("text requires quoted");
            out.assign(lit.text.begin(), lit.text.end());
            return out;
        }
        case ColumnType::Blob:
        {
            if (lit.kind == SqlLiteral::Kind::Hex)
                return hexToBytes(lit.text);
            if (lit.kind == SqlLiteral::Kind::Base64)
                return base64ToBytes(lit.text);
            throw runtimeError("blob requires 0x or b64");
        }
        case ColumnType::Int32:
        {
            if (lit.kind != SqlLiteral::Kind::Number)
                throw runtimeError("int32 requires number");
            appendBe32(out, parseI32(lit.text));
            return out;
        }
        case ColumnType::Int64:
        {
            if (lit.kind != SqlLiteral::Kind::Number)
                throw runtimeError("int64 requires number");
            appendBe64(out, parseI64(lit.text));
            return out;
        }
        case ColumnType::Boolean:
        {
            if (lit.kind != SqlLiteral::Kind::Bool)
                throw runtimeError("boolean requires bool");
            out.push_back((toLower(lit.text) == "true") ? 1 : 0);
            return out;
        }
        case ColumnType::Float32:
        {
            if (lit.kind != SqlLiteral::Kind::Number)
                throw runtimeError("float requires number");
            float f = parseF32(lit.text);
            u32 u;
            static_assert(sizeof(float) == 4);
            std::memcpy(&u, &f, 4);
            out.push_back(static_cast<u8>((u >> 24) & 0xFF));
            out.push_back(static_cast<u8>((u >> 16) & 0xFF));
            out.push_back(static_cast<u8>((u >> 8) & 0xFF));
            out.push_back(static_cast<u8>((u >> 0) & 0xFF));
            return out;
        }
        case ColumnType::Date:
        {
            if (lit.kind != SqlLiteral::Kind::Quoted)
                throw runtimeError("date requires quoted");
            appendBe32(out, parseDateDays(lit.text));
            return out;
        }
        case ColumnType::Timestamp:
        {
            if (lit.kind == SqlLiteral::Kind::Number)
            {
                appendBe64(out, parseI64(lit.text));
                return out;
            }
            if (lit.kind != SqlLiteral::Kind::Quoted)
                throw runtimeError("timestamp requires quoted");
            appendBe64(out, parseTimestampMs(lit.text));
            return out;
        }
        default:
            throw runtimeError("bad type");
        }
    }

    static void appendValueBytes(byteVec &out, ColumnType type, const SqlLiteral &lit)
    {
        if (lit.kind == SqlLiteral::Kind::Null)
            throw runtimeError("null");
        switch (type)
        {
        case ColumnType::Char:
        {
            if (lit.kind != SqlLiteral::Kind::Quoted || lit.text.size() != 1)
                throw runtimeError("char");
            appendU32(out, 1);
            out.push_back(static_cast<u8>(lit.text[0]));
            return;
        }
        case ColumnType::Text:
        {
            if (lit.kind != SqlLiteral::Kind::Quoted)
                throw runtimeError("text");
            appendU32(out, static_cast<u32>(lit.text.size()));
            out.insert(out.end(), lit.text.begin(), lit.text.end());
            return;
        }
        case ColumnType::Blob:
        {
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
        case ColumnType::Int32:
        {
            if (lit.kind != SqlLiteral::Kind::Number)
                throw runtimeError("int32");
            appendBe32(out, parseI32(lit.text));
            return;
        }
        case ColumnType::Int64:
        {
            if (lit.kind != SqlLiteral::Kind::Number)
                throw runtimeError("int64");
            appendBe64(out, parseI64(lit.text));
            return;
        }
        case ColumnType::Boolean:
        {
            if (lit.kind != SqlLiteral::Kind::Bool)
                throw runtimeError("bool");
            out.push_back((toLower(lit.text) == "true") ? 1 : 0);
            return;
        }
        case ColumnType::Float32:
        {
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
        case ColumnType::Date:
        {
            if (lit.kind != SqlLiteral::Kind::Quoted)
                throw runtimeError("date");
            appendBe32(out, parseDateDays(lit.text));
            return;
        }
        case ColumnType::Timestamp:
        {
            if (lit.kind == SqlLiteral::Kind::Number)
            {
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

    byteVec encodeRowBytes(const TableSchema &schema, const std::vector<string> &columnNames, const std::vector<SqlLiteral> &values, const byteVec &pkBytes)
    {
        (void)pkBytes;
        if (columnNames.size() != values.size())
            throw runtimeError("column/value count");

        auto pkName = schema.columns[schema.primaryKeyIndex].name;
        bool pkPresent = false;
        for (const auto &colName : columnNames)
        {
            if (colName == pkName)
                pkPresent = true;
        }
        if (!pkPresent)
            throw runtimeError("missing pk");

        std::vector<std::optional<SqlLiteral>> byIndex;
        byIndex.resize(schema.columns.size());
        for (usize i = 0; i < columnNames.size(); i++)
        {
            auto colIndex = findColumnIndex(schema, columnNames[i]);
            if (!colIndex.has_value())
                throw runtimeError("unknown column");
            byIndex[*colIndex] = values[i];
        }

        byteVec out;
        appendU32(out, 1);
        for (usize i = 0; i < schema.columns.size(); i++)
        {
            if (i == schema.primaryKeyIndex)
                continue;
            if (!byIndex[i].has_value() || byIndex[i]->kind == SqlLiteral::Kind::Null)
            {
                out.push_back(1);
                continue;
            }
            out.push_back(0);
            appendValueBytes(out, schema.columns[i].type, *byIndex[i]);
        }
        return out;
    }

    string formatDateFromDays(i32 days)
    {
        std::chrono::sys_days d{std::chrono::days{days}};
        auto ymd = std::chrono::year_month_day{d};
        int y = int(ymd.year());
        unsigned m = unsigned(ymd.month());
        unsigned da = unsigned(ymd.day());
        char buf[16];
        std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u", y, m, da);
        return string(buf);
    }

    string formatTimestampFromMs(i64 ms)
    {
        using namespace std::chrono;
        sys_time<milliseconds> tp{milliseconds{ms}};
        auto dp = floor<days>(tp);
        year_month_day ymd{dp};
        auto sinceDay = tp - dp;
        auto hh = duration_cast<hours>(sinceDay);
        sinceDay -= hh;
        auto mm = duration_cast<minutes>(sinceDay);
        sinceDay -= mm;
        auto ss = duration_cast<seconds>(sinceDay);
        sinceDay -= ss;
        auto msPart = duration_cast<milliseconds>(sinceDay);
        int y = int(ymd.year());
        unsigned mo = unsigned(ymd.month());
        unsigned da = unsigned(ymd.day());
        auto h = hh.count();
        auto m = mm.count();
        auto s = ss.count();
        auto milli = msPart.count();
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%04d-%02u-%02uT%02lld:%02lld:%02lld.%03lldZ", y, mo, da, (long long)h, (long long)m, (long long)s, (long long)milli);
        return string(buf);
    }

    static string jsonValueFromBytes(ColumnType type, const byteVec &b, usize &o)
    {
        if (type == ColumnType::Text || type == ColumnType::Char)
        {
            u32 len = readU32(b, o);
            if (o + len > b.size())
                throw runtimeError("bad row");
            string s;
            s.assign(reinterpret_cast<const char *>(b.data() + o), reinterpret_cast<const char *>(b.data() + o + len));
            o += len;
            return string("\"") + jsonEscape(s) + "\"";
        }
        if (type == ColumnType::Blob)
        {
            u32 len = readU32(b, o);
            if (o + len > b.size())
                throw runtimeError("bad row");
            byteVec v;
            v.insert(v.end(), b.begin() + o, b.begin() + o + len);
            o += len;
            return string("\"") + jsonEscape(bytesToBase64(v)) + "\"";
        }
        if (type == ColumnType::Int32)
        {
            auto val = readBe32(b, o);
            return std::to_string(val);
        }
        if (type == ColumnType::Int64)
        {
            auto val = readBe64(b, o);
            return std::to_string(val);
        }
        if (type == ColumnType::Boolean)
        {
            if (o + 1 > b.size())
                throw runtimeError("bad row");
            bool val = b[o++] != 0;
            return val ? "true" : "false";
        }
        if (type == ColumnType::Float32)
        {
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
        if (type == ColumnType::Date)
        {
            auto days = readBe32(b, o);
            return string("\"") + formatDateFromDays(days) + "\"";
        }
        if (type == ColumnType::Timestamp)
        {
            auto ms = readBe64(b, o);
            return string("\"") + formatTimestampFromMs(ms) + "\"";
        }
        throw runtimeError("bad type");
    }

    static string jsonPkValue(ColumnType type, const byteVec &pkBytes)
    {
        byteVec b;
        usize o = 0;
        if (type == ColumnType::Text || type == ColumnType::Char)
        {
            string s(reinterpret_cast<const char *>(pkBytes.data()), reinterpret_cast<const char *>(pkBytes.data() + pkBytes.size()));
            return string("\"") + jsonEscape(s) + "\"";
        }
        if (type == ColumnType::Blob)
        {
            return string("\"") + jsonEscape(bytesToBase64(pkBytes)) + "\"";
        }
        if (type == ColumnType::Int32)
        {
            if (pkBytes.size() != 4)
                throw runtimeError("bad pk");
            o = 0;
            b = pkBytes;
            return std::to_string(readBe32(b, o));
        }
        if (type == ColumnType::Int64)
        {
            if (pkBytes.size() != 8)
                throw runtimeError("bad pk");
            o = 0;
            b = pkBytes;
            return std::to_string(readBe64(b, o));
        }
        if (type == ColumnType::Boolean)
        {
            if (pkBytes.size() != 1)
                throw runtimeError("bad pk");
            return pkBytes[0] ? "true" : "false";
        }
        if (type == ColumnType::Float32)
        {
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
        if (type == ColumnType::Date)
        {
            if (pkBytes.size() != 4)
                throw runtimeError("bad pk");
            b = pkBytes;
            o = 0;
            return string("\"") + formatDateFromDays(readBe32(b, o)) + "\"";
        }
        if (type == ColumnType::Timestamp)
        {
            if (pkBytes.size() != 8)
                throw runtimeError("bad pk");
            b = pkBytes;
            o = 0;
            return string("\"") + formatTimestampFromMs(readBe64(b, o)) + "\"";
        }
        return "null";
    }

    string rowToJson(const TableSchema &schema, const byteVec &pkBytes, const byteVec &rowBytes, const std::vector<string> &selectColumns)
    {
        std::vector<string> cols;
        if (selectColumns.empty())
        {
            for (const auto &column : schema.columns)
                cols.push_back(column.name);
        }
        else
        {
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

        for (usize i = 0; i < schema.columns.size(); i++)
        {
            if (i == schema.primaryKeyIndex)
                continue;
            if (num >= rowBytes.size())
                throw runtimeError("bad row");
            u8 row = rowBytes[num++];
            if (row != 0)
            {
                isNull[i] = true;
                continue;
            }
            isNull[i] = false;
            usize before = num;
            (void)jsonValueFromBytes(schema.columns[i].type, rowBytes, num);
            valueOffsets[i] = before;
            valueSizes[i] = num - before;
        }

        string out = "{";
        bool first = true;
        for (const auto &name : cols)
        {
            auto columnIndex = findColumnIndex(schema, name);
            if (!columnIndex.has_value())
                throw runtimeError("unknown column");
            usize i = *columnIndex;
            if (!first)
                out += ",";
            first = false;
            out += "\"" + jsonEscape(name) + "\":";
            if (i == schema.primaryKeyIndex)
            {
                out += jsonPkValue(schema.columns[i].type, pkBytes);
            }
            else
            {
                if (isNull[i])
                {
                    out += "null";
                }
                else
                {
                    usize offset = valueOffsets[i];
                    out += jsonValueFromBytes(schema.columns[i].type, rowBytes, offset);
                }
            }
        }
        out += "}";
        return out;
    }

}
