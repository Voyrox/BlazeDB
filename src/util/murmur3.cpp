#include "util/murmur3.h"
// Reference: https://github.com/PeterScott/murmur3

namespace xeondb {

static u64 rotateLeft64(u64 value, int bits) {
    return (value << bits) | (value >> (64 - bits));
}

static u64 finalMix64(u64 hash) {
    hash ^= hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;
    hash ^= hash >> 33;
    hash *= 0xc4ceb9fe1a85ec53ULL;
    hash ^= hash >> 33;
    return hash;
}

static u64 readBlock64(const u8* p) {
    u64 val = 0;
    val |= static_cast<u64>(p[0]) << 0;
    val |= static_cast<u64>(p[1]) << 8;
    val |= static_cast<u64>(p[2]) << 16;
    val |= static_cast<u64>(p[3]) << 24;
    val |= static_cast<u64>(p[4]) << 32;
    val |= static_cast<u64>(p[5]) << 40;
    val |= static_cast<u64>(p[6]) << 48;
    val |= static_cast<u64>(p[7]) << 56;
    return val;
}

i64 murmur3Token(const byteVec& bytes) {
    const u8* data = bytes.data();
    usize len = bytes.size();
    const usize nblocks = len / 16;

    u64 h1 = 0;
    u64 h2 = 0;

    const u64 c1 = 0x87c37b91114253d5ULL;
    const u64 c2 = 0x4cf5ad432745937fULL;

    const u8* blocks = data;
    for (usize i = 0; i < nblocks; i++) {
        u64 block = readBlock64(blocks + i * 16 + 0);
        u64 block2 = readBlock64(blocks + i * 16 + 8);

        block *= c1;
        block = rotateLeft64(block, 31);
        block *= c2;
        h1 ^= block;

        h1 = rotateLeft64(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        block2 *= c2;
        block2 = rotateLeft64(block2, 33);
        block2 *= c1;
        h2 ^= block2;

        h2 = rotateLeft64(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
    }

    const u8* tail = data + nblocks * 16;
    u64 tailFirst8 = 0;
    u64 tailNext8 = 0;

    switch (len & 15) {
    case 15:
        tailNext8 ^= static_cast<u64>(tail[14]) << 48;
        [[fallthrough]];
    case 14:
        tailNext8 ^= static_cast<u64>(tail[13]) << 40;
        [[fallthrough]];
    case 13:
        tailNext8 ^= static_cast<u64>(tail[12]) << 32;
        [[fallthrough]];
    case 12:
        tailNext8 ^= static_cast<u64>(tail[11]) << 24;
        [[fallthrough]];
    case 11:
        tailNext8 ^= static_cast<u64>(tail[10]) << 16;
        [[fallthrough]];
    case 10:
        tailNext8 ^= static_cast<u64>(tail[9]) << 8;
        [[fallthrough]];
    case 9:
        tailNext8 ^= static_cast<u64>(tail[8]) << 0;
        tailNext8 *= c2;
        tailNext8 = rotateLeft64(tailNext8, 33);
        tailNext8 *= c1;
        h2 ^= tailNext8;
        [[fallthrough]];
    case 8:
        tailFirst8 ^= static_cast<u64>(tail[7]) << 56;
        [[fallthrough]];
    case 7:
        tailFirst8 ^= static_cast<u64>(tail[6]) << 48;
        [[fallthrough]];
    case 6:
        tailFirst8 ^= static_cast<u64>(tail[5]) << 40;
        [[fallthrough]];
    case 5:
        tailFirst8 ^= static_cast<u64>(tail[4]) << 32;
        [[fallthrough]];
    case 4:
        tailFirst8 ^= static_cast<u64>(tail[3]) << 24;
        [[fallthrough]];
    case 3:
        tailFirst8 ^= static_cast<u64>(tail[2]) << 16;
        [[fallthrough]];
    case 2:
        tailFirst8 ^= static_cast<u64>(tail[1]) << 8;
        [[fallthrough]];
    case 1:
        tailFirst8 ^= static_cast<u64>(tail[0]) << 0;
        tailFirst8 *= c1;
        tailFirst8 = rotateLeft64(tailFirst8, 31);
        tailFirst8 *= c2;
        h1 ^= tailFirst8;
        [[fallthrough]];
    default:
        break;
    }

    h1 ^= static_cast<u64>(len);
    h2 ^= static_cast<u64>(len);

    h1 += h2;
    h2 += h1;

    h1 = finalMix64(h1);
    h2 = finalMix64(h2);

    h1 += h2;

    return static_cast<i64>(h1);
}

}
