#include "util/log.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <mutex>
#include <string>

#include <unistd.h>

namespace xeondb
{

    static std::mutex g_logMutex;

    static const char *typeName(LogLevel level)
    {
        switch (level)
        {
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::CONFIG:
            return "CONFIG";
        case LogLevel::WARN:
            return "WARN";
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::INTERRUPT:
            return "INTERRUPT";
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::TRACE:
            return "TRACE";
        }
        return "INFO";
    }

    static const char *colour(LogLevel level)
    {
        switch (level)
        {
        case LogLevel::INFO:
            return "\x1b[34m"; // blue
        case LogLevel::CONFIG:
            return "\x1b[36m"; // cyan
        case LogLevel::WARN:
            return "\x1b[33m"; // yellow
        case LogLevel::ERROR:
            return "\x1b[31m"; // red
        case LogLevel::INTERRUPT:
            return "\x1b[1;31m"; // bold red
        case LogLevel::DEBUG:
            return "\x1b[90m"; // bright black / gray
        case LogLevel::TRACE:
            return "\x1b[2;90m"; // dim gray
        }
        return "\x1b[34m";
    }

    static bool colourEnabled(int fd)
    {
        if (!::isatty(fd))
        {
            return false;
        }
        if (std::getenv("NO_COLOR") != nullptr)
        {
            return false;
        }
        const char *term = std::getenv("TERM");
        if (term != nullptr && std::string(term) == "dumb")
        {
            return false;
        }
        return true;
    }

    static std::string nowTimestamp()
    {
        using namespace std::chrono;
        auto sysTime = system_clock::to_time_t(system_clock::now());
        std::tm time{};
        localtime_r(&sysTime, &time);

        char buf[64];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                      time.tm_year + 1900, time.tm_mon + 1, time.tm_mday,
                      time.tm_hour, time.tm_min, time.tm_sec);
        return std::string(buf);
    }

    static bool isErrLevel(LogLevel level)
    {
        return level == LogLevel::WARN || level == LogLevel::ERROR || level == LogLevel::INTERRUPT;
    }

        void log(LogLevel level, std::string_view message)
        {
            const bool isErr = isErrLevel(level);
            const int val = isErr ? STDERR_FILENO : STDOUT_FILENO;
            const bool color = colourEnabled(val);

            std::string timeStamp = nowTimestamp();
            const char *lvl = typeName(level);

            std::lock_guard<std::mutex> lock(g_logMutex);
            std::ostream &data = isErr ? std::cerr : std::cout;

            data << timeStamp << " ";
            if (color)
            {
                data << colour(level) << "[" << lvl << "]" << "\x1b[0m";
            }
            else
            {
                data << "[" << lvl << "]";
            }
            data << " " << message << "\n";
            data.flush();
        }
}
