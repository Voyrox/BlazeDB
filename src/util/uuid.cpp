#include "util/uuid.h"

#include "prelude.h"

#include <array>
#include <random>
#include <string>

namespace xeondb
{

    static char hexChar(u8 v)
    {
        const char *table = "0123456789abcdef";
        return table[v & 0xF];
    }

    std::string newUuidHex()
    {
        std::array<u8, 16> bytes;
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<u32> dist(0, 0xFFFFFFFFu);

        for (usize i = 0; i < 16; i += 4)
        {
            u32 v = dist(gen);
            bytes[i + 0] = static_cast<u8>((v >> 24) & 0xFF);
            bytes[i + 1] = static_cast<u8>((v >> 16) & 0xFF);
            bytes[i + 2] = static_cast<u8>((v >> 8) & 0xFF);
            bytes[i + 3] = static_cast<u8>((v >> 0) & 0xFF);
        }

        std::string data;
        data.reserve(32);
        for (u8 b : bytes)
        {
            data.push_back(hexChar(static_cast<u8>(b >> 4)));
            data.push_back(hexChar(static_cast<u8>(b & 0xF)));
        }
        return data;
    }

}
