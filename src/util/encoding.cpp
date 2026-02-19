#include "util/encoding.h"

#include "prelude.h"

#include <cctype>

using std::string;

namespace blazeDb
{

byteVec hexToBytes(const string& hex)
{
    auto isHex = [](char c) {
        return std::isdigit(static_cast<unsigned char>(c)) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    };
    auto hexVal = [](char c) -> u8 {
        if (c >= '0' && c <= '9')
            return static_cast<u8>(c - '0');
        if (c >= 'a' && c <= 'f')
            return static_cast<u8>(10 + (c - 'a'));
        if (c >= 'A' && c <= 'F')
            return static_cast<u8>(10 + (c - 'A'));
        return 0;
    };

    if (hex.size() % 2 != 0)
        throw runtimeError("bad hex");
    byteVec data;
    data.reserve(hex.size() / 2);
    for (usize i = 0; i < hex.size(); i += 2)
    {
        if (!isHex(hex[i]) || !isHex(hex[i + 1]))
            throw runtimeError("bad hex");
        data.push_back(static_cast<u8>((hexVal(hex[i]) << 4) | hexVal(hex[i + 1])));
    }
    return data;
}

byteVec base64ToBytes(const string& s)
{
    auto val = [](unsigned char c) -> int {
        if (c >= 'A' && c <= 'Z')
            return c - 'A';
        if (c >= 'a' && c <= 'z')
            return 26 + (c - 'a');
        if (c >= '0' && c <= '9')
            return 52 + (c - '0');
        if (c == '+')
            return 62;
        if (c == '/')
            return 63;
        if (c == '=')
            return -2;
        return -1;
    };

    byteVec data;
    data.reserve((s.size() * 3) / 4 + 3);
    u32 acc = 0;
    int bits = 0;
    for (unsigned char c : s)
    {
        int base64 = val(c);
        if (base64 == -1)
            continue;
        if (base64 == -2)
            break;
        acc = (acc << 6) | static_cast<u32>(base64);
        bits += 6;
        if (bits >= 8)
        {
            bits -= 8;
            data.push_back(static_cast<u8>((acc >> bits) & 0xFF));
        }
    }
    return data;
}

string bytesToBase64(const byteVec& data)
{
    static const char* table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    string base64Str;
    base64Str.reserve(((data.size() + 2) / 3) * 4);
    usize i = 0;
    while (i + 3 <= data.size())
    {
        u32 chunk = (static_cast<u32>(data[i]) << 16) | (static_cast<u32>(data[i + 1]) << 8) | static_cast<u32>(data[i + 2]);
        base64Str.push_back(table[(chunk >> 18) & 63]);
        base64Str.push_back(table[(chunk >> 12) & 63]);
        base64Str.push_back(table[(chunk >> 6) & 63]);
        base64Str.push_back(table[chunk & 63]);
        i += 3;
    }
    usize rem = data.size() - i;
    if (rem == 1)
    {
        u32 chunk = static_cast<u32>(data[i]) << 16;
        base64Str.push_back(table[(chunk >> 18) & 63]);
        base64Str.push_back(table[(chunk >> 12) & 63]);
        base64Str.push_back('=');
        base64Str.push_back('=');
    }
    else if (rem == 2)
    {
        u32 chunk = (static_cast<u32>(data[i]) << 16) | (static_cast<u32>(data[i + 1]) << 8);
        base64Str.push_back(table[(chunk >> 18) & 63]);
        base64Str.push_back(table[(chunk >> 12) & 63]);
        base64Str.push_back(table[(chunk >> 6) & 63]);
        base64Str.push_back('=');
    }
    return base64Str;
}

}
