#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace blazeDb
{

using u8 = std::uint8_t;
using u16 = std::uint16_t;
using u32 = std::uint32_t;
using u64 = std::uint64_t;
using i32 = std::int32_t;
using i64 = std::int64_t;
using usize = std::size_t;

using byteVec = std::vector<u8>;

using stringView = std::string_view;
using path = std::filesystem::path;

struct runtimeError : std::runtime_error
{
    using std::runtime_error::runtime_error;
};

}
