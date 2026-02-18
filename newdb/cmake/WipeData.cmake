if(NOT DEFINED SOURCE_DIR)
    message(FATAL_ERROR "SOURCE_DIR is required")
endif()

set(_dirs
    "/var/lib/blazedb/data"
    "${SOURCE_DIR}/var/lib/blazedb/data"
)

foreach(_dir IN LISTS _dirs)
    if(NOT IS_DIRECTORY "${_dir}")
        continue()
    endif()

    file(GLOB _children "${_dir}/*" "${_dir}/.*")
    foreach(_child IN LISTS _children)
        get_filename_component(_name "${_child}" NAME)
        if(_name STREQUAL "." OR _name STREQUAL "..")
            continue()
        endif()
        file(REMOVE_RECURSE "${_child}")
    endforeach()

    file(GLOB _after "${_dir}/*" "${_dir}/.*")
    set(_leftover 0)
    foreach(_child IN LISTS _after)
        get_filename_component(_name "${_child}" NAME)
        if(_name STREQUAL "." OR _name STREQUAL "..")
            continue()
        endif()
        set(_leftover 1)
        break()
    endforeach()

    if(_leftover)
        message(WARNING "Could not fully wipe dataDir ${_dir} (permissions?)")
    else()
        message(STATUS "Wiped dataDir ${_dir}")
    endif()
endforeach()
