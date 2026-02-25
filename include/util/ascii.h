#pragma once

#include "prelude.h"

#include <cctype>

namespace xeondb {

inline char asciiLower(char c) {
    if (c >= 'A' && c <= 'Z')
        return static_cast<char>(c - 'A' + 'a');
    return c;
}

inline bool asciiIEquals(stringView a, stringView b) {
    if (a.size() != b.size())
        return false;
    for (usize i = 0; i < a.size(); i++) {
        if (asciiLower(a[i]) != asciiLower(b[i]))
            return false;
    }
    return true;
}

inline bool startsWithKeywordIcase(stringView line, stringView keyword) {
    usize i = 0;
    while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i])))
        i++;

    if (i + keyword.size() > line.size())
        return false;

    for (usize j = 0; j < keyword.size(); j++) {
        if (asciiLower(line[i + j]) != asciiLower(keyword[j]))
            return false;
    }

    const usize end = i + keyword.size();
    if (end == line.size())
        return true;
    return std::isspace(static_cast<unsigned char>(line[end])) != 0;
}

}
