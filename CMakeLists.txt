cmake_minimum_required(VERSION 3.20)

project(Xeondb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

set(BLAZEDBD_SOURCES
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
    src/util/log.cpp
    src/util/murmur3.cpp
    src/util/uuid.cpp
)

add_executable(blazedbd ${BLAZEDBD_SOURCES})

target_include_directories(blazedbd PRIVATE include)
target_compile_options(blazedbd PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(blazedbd PRIVATE Threads::Threads)

set(CMAKE_CTEST_ARGUMENTS "--output-on-failure")
enable_testing()

if(EXISTS "${CMAKE_SOURCE_DIR}/.venv/bin/python3")
    set(BLAZEDB_PYTHON "${CMAKE_SOURCE_DIR}/.venv/bin/python3")
else()
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    set(BLAZEDB_PYTHON "${Python3_EXECUTABLE}")
endif()

add_test(
    NAME xeondb_pytest
    COMMAND
        ${CMAKE_COMMAND} -E env
            "XEONDB_EXECUTABLE=$<TARGET_FILE:blazedbd>"
            "${BLAZEDB_PYTHON}" -m pytest -vv --durations=10
)
set_tests_properties(xeondb_pytest PROPERTIES WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")

find_program(CLANG_TIDY_EXE NAMES clang-tidy)
if(CLANG_TIDY_EXE)
    add_custom_target(lint
        COMMAND "${CLANG_TIDY_EXE}" -p "${CMAKE_BINARY_DIR}" ${BLAZEDBD_SOURCES} || true
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        VERBATIM
    )
else()
    add_custom_target(lint
        COMMAND ${CMAKE_COMMAND} -E echo "clang-tidy not installed"
        COMMAND ${CMAKE_COMMAND} -E false
        VERBATIM
    )
endif()

file(GLOB_RECURSE BLAZEDB_FORMAT_SOURCES CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/src/*.cpp")
file(GLOB_RECURSE BLAZEDB_FORMAT_HEADERS CONFIGURE_DEPENDS "${CMAKE_SOURCE_DIR}/include/*.h")
set(BLAZEDB_FORMAT_FILES ${BLAZEDB_FORMAT_SOURCES} ${BLAZEDB_FORMAT_HEADERS})

find_program(CLANG_FORMAT_EXE NAMES clang-format)
if(CLANG_FORMAT_EXE)
    add_custom_target(formatCheck
        COMMAND "${CLANG_FORMAT_EXE}" --dry-run -Werror ${BLAZEDB_FORMAT_FILES}
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        VERBATIM
    )
    add_custom_target(format
        COMMAND "${CLANG_FORMAT_EXE}" -i ${BLAZEDB_FORMAT_FILES}
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        VERBATIM
    )
else()
    add_custom_target(formatCheck
        COMMAND ${CMAKE_COMMAND} -E echo "clang-format not installed"
        COMMAND ${CMAKE_COMMAND} -E false
        VERBATIM
    )
    add_custom_target(format
        COMMAND ${CMAKE_COMMAND} -E echo "clang-format not installed"
        COMMAND ${CMAKE_COMMAND} -E false
        VERBATIM
    )
endif()

add_custom_target(run
    COMMAND $<TARGET_FILE:blazedbd> --config "${CMAKE_SOURCE_DIR}/config/settings.yml"
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    DEPENDS blazedbd
    USES_TERMINAL
    VERBATIM
)

add_custom_target(wipeData
    COMMAND ${CMAKE_COMMAND} -D SOURCE_DIR=${CMAKE_SOURCE_DIR} -P ${CMAKE_SOURCE_DIR}/cmake/WipeData.cmake
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    VERBATIM
)

add_custom_target(purge
    COMMAND ${CMAKE_COMMAND} -D SOURCE_DIR=${CMAKE_SOURCE_DIR} -D BINARY_DIR=${CMAKE_BINARY_DIR} -P ${CMAKE_SOURCE_DIR}/cmake/PurgeBuild.cmake
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    VERBATIM
)
add_dependencies(purge wipeData)

add_custom_target(build
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    DEPENDS blazedbd lint
    VERBATIM
)
