#include "query/schema/detail/internal.h"

#include <chrono>
#include <cctype>
#include <string>

namespace xeondb::schema_detail {

i32 parseI32(const std::string& s) {
    usize i = 0;
    long long v = std::stoll(s, &i, 10);
    if (i != s.size())
        throw runtimeError("bad int");
    if (v < -2147483648LL || v > 2147483647LL)
        throw runtimeError("int32 overflow");
    return static_cast<i32>(v);
}

i64 parseI64(const std::string& s) {
    usize i = 0;
    long long v = std::stoll(s, &i, 10);
    if (i != s.size())
        throw runtimeError("bad int");
    return static_cast<i64>(v);
}

float parseF32(const std::string& s) {
    usize i = 0;
    float val = std::stof(s, &i);
    if (i != s.size())
        throw runtimeError("bad float");
    return val;
}

i32 parseDateDays(const std::string& s) {
    if (s.size() != 10 || s[4] != '-' || s[7] != '-')
        throw runtimeError("bad date");
    int y = std::stoi(s.substr(0, 4));
    unsigned m = static_cast<unsigned>(std::stoi(s.substr(5, 2)));
    unsigned d = static_cast<unsigned>(std::stoi(s.substr(8, 2)));
    std::chrono::year_month_day ymd{std::chrono::year{y}, std::chrono::month{m}, std::chrono::day{d}};
    if (!ymd.ok())
        throw runtimeError("bad date");
    auto days = std::chrono::sys_days{ymd}.time_since_epoch();
    return static_cast<i32>(std::chrono::duration_cast<std::chrono::days>(days).count());
}

i64 parseTimestampMs(const std::string& s) {
    if (s.size() < 20)
        throw runtimeError("bad timestamp");
    if (s[4] != '-' || s[7] != '-' || (s[10] != 'T' && s[10] != 't'))
        throw runtimeError("bad timestamp");
    int y = std::stoi(s.substr(0, 4));
    unsigned mo = static_cast<unsigned>(std::stoi(s.substr(5, 2)));
    unsigned da = static_cast<unsigned>(std::stoi(s.substr(8, 2)));
    unsigned hh = static_cast<unsigned>(std::stoi(s.substr(11, 2)));
    unsigned mm = static_cast<unsigned>(std::stoi(s.substr(14, 2)));
    unsigned ss = static_cast<unsigned>(std::stoi(s.substr(17, 2)));

    usize pos = 19;
    int ms = 0;
    if (pos < s.size() && s[pos] == '.') {
        pos++;
        int mult = 100;
        int count = 0;
        while (pos < s.size() && std::isdigit(static_cast<unsigned char>(s[pos])) && count < 3) {
            ms += (s[pos] - '0') * mult;
            mult /= 10;
            pos++;
            count++;
        }
        while (pos < s.size() && std::isdigit(static_cast<unsigned char>(s[pos])))
            pos++;
    }
    if (pos >= s.size() || (s[pos] != 'Z' && s[pos] != 'z'))
        throw runtimeError("timestamp must end with Z");

    std::chrono::year_month_day ymd{std::chrono::year{y}, std::chrono::month{mo}, std::chrono::day{da}};
    if (!ymd.ok())
        throw runtimeError("bad timestamp");
    auto dayPoint = std::chrono::sys_days{ymd};
    auto tp = dayPoint + std::chrono::hours{hh} + std::chrono::minutes{mm} + std::chrono::seconds{ss} + std::chrono::milliseconds{ms};
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

}
