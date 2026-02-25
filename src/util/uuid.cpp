#include "util/uuid.h"

#include "prelude.h"

#include <array>
#include <random>
#include <string>

// mt19937_64 - 64-bit pseudorandom number generator also not cryptographically secure
namespace xeondb {

static char hexChar(u8 v) {
    const char* table = "0123456789abcdef";
    return table[v & 0xF];
}

std::string newUuidHex() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<u32> dist(0, 0xFFFFFFFFu);

    std::array<u8, 16> bytes;

    for (usize i = 0; i < 16; i += 4) {
        u32 v = dist(gen);
        bytes[i + 0] = static_cast<u8>((v >> 24) & 0xFF);
        bytes[i + 1] = static_cast<u8>((v >> 16) & 0xFF);
        bytes[i + 2] = static_cast<u8>((v >> 8) & 0xFF);
        bytes[i + 3] = static_cast<u8>((v >> 0) & 0xFF);
    }

    // Set UUID v4
    bytes[6] = (bytes[6] & 0x0F) | 0x40;

    // Set RFC variant
    bytes[8] = (bytes[8] & 0x3F) | 0x80;

    std::string data;
    data.reserve(32);

    for (u8 b : bytes) {
        data.push_back(hexChar(b >> 4));
        data.push_back(hexChar(b & 0xF));
    }

    return data;
}

}
