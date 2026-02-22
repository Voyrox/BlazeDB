if(NOT DEFINED SOURCE_DIR)
    message(FATAL_ERROR "SOURCE_DIR is required")
endif()
if(NOT DEFINED BINARY_DIR)
    message(FATAL_ERROR "BINARY_DIR is required")
endif()

file(REAL_PATH "${SOURCE_DIR}" _src)
file(REAL_PATH "${BINARY_DIR}" _bin)

if(_src STREQUAL _bin)
    message(FATAL_ERROR "Refusing to purge: build dir equals source dir (${_bin})")
endif()

if(NOT EXISTS "${_bin}/CMakeCache.txt")
    message(FATAL_ERROR "Refusing to purge: no CMakeCache.txt in ${_bin}")
endif()

file(GLOB _entries "${_bin}/*" "${_bin}/.*")
foreach(_entry IN LISTS _entries)
    get_filename_component(_name "${_entry}" NAME)
    if(_name STREQUAL "." OR _name STREQUAL "..")
        continue()
    endif()
    file(REMOVE_RECURSE "${_entry}")
endforeach()

message(STATUS "Purged build directory contents: ${_bin}")
message(STATUS "Reconfigure with: cmake -S ${_src} -B ${_bin} -G Ninja")
