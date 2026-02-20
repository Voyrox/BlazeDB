#include "query/sql/detail/parse_utils.h"

namespace xeondb::sql_detail {

bool orderByClause(stringView s, usize& i, std::string& outColumn, bool& outDesc, std::string& error) {
    skipWhitespace(s, i);
    usize j = i;
    if (!matchKeyword(s, j, "order"))
        return true;

    i = j;
    if (!matchKeyword(s, i, "by")) {
        error = "Expected by";
        return false;
    }

    std::string col;
    if (!parseIdentifier(s, i, col)) {
        error = "Expected order by column";
        return false;
    }

    bool desc = false;
    {
        usize k = i;
        if (matchKeyword(s, k, "asc")) {
            i = k;
            desc = false;
        } else {
            k = i;
            if (matchKeyword(s, k, "desc")) {
                i = k;
                desc = true;
            }
        }
    }

    outColumn = col;
    outDesc = desc;
    return true;
}

}
