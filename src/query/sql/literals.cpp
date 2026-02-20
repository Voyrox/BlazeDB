#include "query/sql/detail/parse_utils.h"

#include <cctype>

namespace xeondb::sql_detail {

bool ifNotExists(stringView s, usize& i, bool& out) {
    out = false;
    usize j = i;
    if (!matchKeyword(s, j, "if"))
        return true;
    if (!matchKeyword(s, j, "not"))
        return false;
    if (!matchKeyword(s, j, "exists"))
        return false;
    i = j;
    out = true;
    return true;
}

bool hexLiteral(stringView s, usize& i, std::string& out) {
    skipWhitespace(s, i);
    if (i + 2 > s.size())
        return false;
    if (s[i] != '0' || (s[i + 1] != 'x' && s[i + 1] != 'X'))
        return false;
    usize j = i + 2;
    while (j < s.size()) {
        char c = s[j];
        bool ok = std::isdigit(static_cast<unsigned char>(c)) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (!ok)
            break;
        j++;
    }
    if (j == i + 2)
        return false;
    out.assign(s.substr(i + 2, j - (i + 2)));
    i = j;
    return true;
}

bool numberToken(stringView s, usize& i, std::string& out) {
    skipWhitespace(s, i);
    usize j = i;
    if (j < s.size() && (s[j] == '-' || s[j] == '+'))
        j++;
    bool any = false;
    while (j < s.size() && std::isdigit(static_cast<unsigned char>(s[j]))) {
        any = true;
        j++;
    }
    if (j < s.size() && s[j] == '.') {
        j++;
        while (j < s.size() && std::isdigit(static_cast<unsigned char>(s[j]))) {
            any = true;
            j++;
        }
    }
    if (!any)
        return false;
    out.assign(s.substr(i, j - i));
    i = j;
    return true;
}

bool literal(stringView s, usize& i, SqlLiteral& out) {
    skipWhitespace(s, i);
    std::string tmp;
    usize j = i;

    if (matchKeyword(s, j, "null")) {
        i = j;
        out.kind = SqlLiteral::Kind::Null;
        out.text.clear();
        return true;
    }

    j = i;
    if (matchKeyword(s, j, "true")) {
        i = j;
        out.kind = SqlLiteral::Kind::Bool;
        out.text = "true";
        return true;
    }
    j = i;
    if (matchKeyword(s, j, "false")) {
        i = j;
        out.kind = SqlLiteral::Kind::Bool;
        out.text = "false";
        return true;
    }

    j = i;
    if (matchKeyword(s, j, "b64")) {
        i = j;
        if (!parseQuoted(s, i, tmp))
            return false;
        out.kind = SqlLiteral::Kind::Base64;
        out.text = tmp;
        return true;
    }

    if (hexLiteral(s, i, tmp)) {
        out.kind = SqlLiteral::Kind::Hex;
        out.text = tmp;
        return true;
    }

    if (parseQuoted(s, i, tmp)) {
        out.kind = SqlLiteral::Kind::Quoted;
        out.text = tmp;
        return true;
    }

    if (numberToken(s, i, tmp)) {
        out.kind = SqlLiteral::Kind::Number;
        out.text = tmp;
        return true;
    }

    return false;
}

bool typeName(stringView s, usize& i, std::string& out) {
    std::string type;
    if (!parseIdentifier(s, i, type))
        return false;
    usize j = i;
    std::string size;
    if (type == "int" && numberToken(s, j, size)) {
        if (size == "32" || size == "64") {
            i = j;
            out = "int" + size;
            return true;
        }
    }
    out = type;
    return true;
}

}
