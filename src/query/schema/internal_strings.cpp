#include "query/schema/detail/internal.h"

#include <cctype>

namespace xeondb::schema_detail {

std::string toLower(std::string s) {
    for (char& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

}
