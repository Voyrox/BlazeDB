#include "util/binIo.h"

using std::ifstream;
using std::ofstream;
using std::string;

namespace blazeDb
{

void writeU32(ofstream& data, u32 v)
{
    data.write(reinterpret_cast<const char*>(&v), sizeof(v));
    if (!data)
        throw runtimeError("Write failed");
}

void writeU64(ofstream& data, u64 v)
{
    data.write(reinterpret_cast<const char*>(&v), sizeof(v));
    if (!data)
        throw runtimeError("Write failed");
}

u32 readU32(ifstream& data)
{
    u32 val = 0;
    data.read(reinterpret_cast<char*>(&val), sizeof(val));
    if (!data)
        throw runtimeError("Read failed");
    return val;
}

u64 readU64(ifstream& data)
{
    u64 val = 0;
    data.read(reinterpret_cast<char*>(&val), sizeof(val));
    if (!data)
        throw runtimeError("Read failed");
    return val;
}

void writeString(ofstream& data, const string& s)
{
    writeU32(data, static_cast<u32>(s.size()));
    data.write(s.data(), static_cast<std::streamsize>(s.size()));
    if (!data)
        throw runtimeError("Write failed");
}

string readString(ifstream& data)
{
    auto len = readU32(data);
    string s;
    s.resize(len);
    if (len > 0)
        data.read(s.data(), static_cast<std::streamsize>(len));
    if (!data)
        throw runtimeError("Read failed");
    return s;
}

void writeBytes(ofstream& data, const byteVec& b)
{
    writeU32(data, static_cast<u32>(b.size()));
    if (!b.empty())
        data.write(reinterpret_cast<const char*>(b.data()), static_cast<std::streamsize>(b.size()));
    if (!data)
        throw runtimeError("Write failed");
}

byteVec readBytes(ifstream& data)
{
    auto len = readU32(data);
    byteVec b;
    b.resize(len);
    if (len > 0)
        data.read(reinterpret_cast<char*>(b.data()), static_cast<std::streamsize>(len));
    if (!data)
        throw runtimeError("Read failed");
    return b;
}

void appendU32(byteVec& data, u32 v)
{
    data.push_back(static_cast<u8>((v >> 24) & 0xFF));
    data.push_back(static_cast<u8>((v >> 16) & 0xFF));
    data.push_back(static_cast<u8>((v >> 8) & 0xFF));
    data.push_back(static_cast<u8>((v >> 0) & 0xFF));
}

u32 readU32(const byteVec& data, usize& o)
{
    if (o + 4 > data.size())
        throw runtimeError("Bad bytes");
    u32 val = 0;
    val |= static_cast<u32>(data[o + 0]) << 24;
    val |= static_cast<u32>(data[o + 1]) << 16;
    val |= static_cast<u32>(data[o + 2]) << 8;
    val |= static_cast<u32>(data[o + 3]) << 0;
    o += 4;
    return val;
}

void appendBe32(byteVec& data, i32 v)
{
    u32 val = static_cast<u32>(v);
    data.push_back(static_cast<u8>((val >> 24) & 0xFF));
    data.push_back(static_cast<u8>((val >> 16) & 0xFF));
    data.push_back(static_cast<u8>((val >> 8) & 0xFF));
    data.push_back(static_cast<u8>((val >> 0) & 0xFF));
}

void appendBe64(byteVec& data, i64 v)
{
    u64 val = static_cast<u64>(v);
    data.push_back(static_cast<u8>((val >> 56) & 0xFF));
    data.push_back(static_cast<u8>((val >> 48) & 0xFF));
    data.push_back(static_cast<u8>((val >> 40) & 0xFF));
    data.push_back(static_cast<u8>((val >> 32) & 0xFF));
    data.push_back(static_cast<u8>((val >> 24) & 0xFF));
    data.push_back(static_cast<u8>((val >> 16) & 0xFF));
    data.push_back(static_cast<u8>((val >> 8) & 0xFF));
    data.push_back(static_cast<u8>((val >> 0) & 0xFF));
}

i32 readBe32(const byteVec& data, usize& o)
{
    if (o + 4 > data.size())
        throw runtimeError("Bad bytes");
    u32 val = 0;
    val |= static_cast<u32>(data[o + 0]) << 24;
    val |= static_cast<u32>(data[o + 1]) << 16;
    val |= static_cast<u32>(data[o + 2]) << 8;
    val |= static_cast<u32>(data[o + 3]) << 0;
    o += 4;
    return static_cast<i32>(val);
}

i64 readBe64(const byteVec& data, usize& o)
{
    if (o + 8 > data.size())
        throw runtimeError("Bad bytes");
    u64 val = 0;
    for (int i = 0; i < 8; i++)
        val = (val << 8) | static_cast<u64>(data[o + static_cast<usize>(i)]);
    o += 8;
    return static_cast<i64>(val);
}

}
