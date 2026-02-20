#pragma once

#include "prelude.h"
#include "query/schema.h"

#include <string>

namespace xeondb::sql_detail {

bool isIdentChar(char c);

void skipWhitespace(stringView s, usize& i);

bool matchKeyword(stringView s, usize& i, stringView keyChars);

bool consumeChar(stringView s, usize& i, char c);

bool parseIdentifier(stringView s, usize& i, std::string& out);

bool parseQuoted(stringView s, usize& i, std::string& out);

std::string stripTrailingSemicolon(std::string s);

bool ifNotExists(stringView s, usize& i, bool& out);

bool hexLiteral(stringView s, usize& i, std::string& out);

bool numberToken(stringView s, usize& i, std::string& out);

bool literal(stringView s, usize& i, SqlLiteral& out);

bool orderByClause(stringView s, usize& i, std::string& outColumn, bool& outDesc, std::string& error);

bool typeName(stringView s, usize& i, std::string& out);

}
