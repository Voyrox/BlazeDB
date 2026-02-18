cmake_minimum_required(VERSION 3.20)

project(blazedbNewdb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(blazedbd
    src/app/main.cpp
    src/config/config.cpp
    src/core/db.cpp
    src/core/paths.cpp
    src/net/serverTcp.cpp
    src/query/schema.cpp
    src/query/sql.cpp
    src/storage/commitLog.cpp
    src/storage/manifest.cpp
    src/storage/memTable.cpp
    src/storage/ssTable.cpp
    src/storage/table.cpp
    src/util/binIo.cpp
    src/util/crc32.cpp
    src/util/encoding.cpp
    src/util/json.cpp
    src/util/murmur3.cpp
    src/util/uuid.cpp
)

target_include_directories(blazedbd PRIVATE include)
target_compile_options(blazedbd PRIVATE -Wall -Wextra -Wpedantic)
