#pragma once

#include <string_view>

namespace xeondb {

enum class LogLevel {
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
