#pragma once

#include "query/schema.h"

#include <string>

namespace xeondb::schema_detail {

std::string toLower(std::string s);

i32 parseI32(const std::string& s);
i64 parseI64(const std::string& s);
float parseF32(const std::string& s);
i32 parseDateDays(const std::string& s);
i64 parseTimestampMs(const std::string& s);

void appendValueBytes(byteVec& out, ColumnType type, const SqlLiteral& lit);
void skipValueBytes(ColumnType type, const byteVec& b, usize& o);

std::string jsonValueFromBytes(ColumnType type, const byteVec& b, usize& o);
std::string jsonPkValue(ColumnType type, const byteVec& pkBytes);

}
