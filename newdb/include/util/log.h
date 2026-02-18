#pragma once

#include <string_view>

namespace blazeDb
{

    enum class LogLevel
    {
        INFO,
        CONFIG,
        WARN,
        ERROR,
        INTERRUPT,
        DEBUG,
        TRACE,
    };

    void log(LogLevel level, std::string_view message);

}
