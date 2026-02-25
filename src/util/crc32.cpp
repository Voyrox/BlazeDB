#include "util/crc32.h"

#include <array>

namespace xeondb {

static std::array<u32, 256> makeTable() {
    std::array<u32, 256> t{};
    for (u32 i = 0; i < 256; i++) {
        u32 c = i;
        for (int k = 0; k < 8; k++) {
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        }
        t[i] = c;
    }
    return t;
}

// Cyclic Redundancy Check (CRC32) - Detect data corruption
u32 crc32(const u8* data, usize size) {
    static const auto table = makeTable();
    u32 initial = 0xFFFFFFFFu;
    for (usize i = 0; i < size; i++) {
        initial = table[(initial ^ data[i]) & 0xFF] ^ (initial >> 8);
    }
    return initial ^ 0xFFFFFFFFu;
}

}
