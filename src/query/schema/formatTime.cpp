#include "query/schema.h"

#include <chrono>
#include <cstdio>

namespace xeondb {

string dateFromDays(i32 days) {
    std::chrono::sys_days d{std::chrono::days{days}};
    auto ymd = std::chrono::year_month_day{d};
    int y = int(ymd.year());
    unsigned m = unsigned(ymd.month());
    unsigned da = unsigned(ymd.day());
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%04d-%02u-%02u", y, m, da);
    return string(buf);
}

string timestampFromMs(i64 ms) {
    using namespace std::chrono;
    sys_time<milliseconds> tp{milliseconds{ms}};
    auto dp = floor<days>(tp);
    year_month_day ymd{dp};
    auto sinceDay = tp - dp;
    auto hh = duration_cast<hours>(sinceDay);
    sinceDay -= hh;
    auto mm = duration_cast<minutes>(sinceDay);
    sinceDay -= mm;
    auto ss = duration_cast<seconds>(sinceDay);
    sinceDay -= ss;
    auto msPart = duration_cast<milliseconds>(sinceDay);
    int y = int(ymd.year());
    unsigned mo = unsigned(ymd.month());
    unsigned da = unsigned(ymd.day());
    auto h = hh.count();
    auto m = mm.count();
    auto s = ss.count();
    auto milli = msPart.count();
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%04d-%02u-%02uT%02lld:%02lld:%02lld.%03lldZ", y, mo, da, (long long)h, (long long)m, (long long)s, (long long)milli);
    return string(buf);
}

}
