#include "query/sql/detail/parse_utils.h"

#include <cctype>

namespace xeondb::sql_detail {

bool isIdentChar(char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

void skipWhitespace(stringView s, usize& i) {
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i])))
        i++;
}

bool matchKeyword(stringView s, usize& i, stringView keyChars) {
    skipWhitespace(s, i);
    usize j = i;
    for (char c : keyChars) {
        if (j >= s.size())
            return false;
        char tmp = s[j];
        if (std::tolower(static_cast<unsigned char>(tmp)) != std::tolower(static_cast<unsigned char>(c)))
            return false;
        j++;
    }
    if (j < s.size() && isIdentChar(s[j]))
        return false;
    i = j;
    return true;
}

bool consumeChar(stringView s, usize& i, char c) {
    skipWhitespace(s, i);
    if (i < s.size() && s[i] == c) {
        i++;
        return true;
    }
    return false;
}

bool parseIdentifier(stringView s, usize& i, std::string& out) {
    skipWhitespace(s, i);
    if (i >= s.size() || !isIdentChar(s[i]) || std::isdigit(static_cast<unsigned char>(s[i])))
        return false;
    usize start = i;
    while (i < s.size() && isIdentChar(s[i]))
        i++;
    out.assign(s.substr(start, i - start));
    return true;
}

bool parseQuoted(stringView s, usize& i, std::string& out) {
    skipWhitespace(s, i);
    if (i >= s.size() || s[i] != '"')
        return false;
    i++;
    std::string result;
    while (i < s.size()) {
        char c = s[i++];
        if (c == '"') {
            out = result;
            return true;
        }
        if (c == '\\' && i < s.size()) {
            char nextc = s[i++];
            if (nextc == '"' || nextc == '\\' || nextc == '/')
                result.push_back(nextc);
            else if (nextc == 'n')
                result.push_back('\n');
            else if (nextc == 'r')
                result.push_back('\r');
            else if (nextc == 't')
                result.push_back('\t');
            else
                result.push_back(nextc);
        } else {
            result.push_back(c);
        }
    }
    return false;
}

std::string stripTrailingSemicolon(std::string s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
        s.pop_back();
    if (!s.empty() && s.back() == ';')
        s.pop_back();
    return s;
}

}
