#pragma once

#include "prelude.h"

#include <fstream>

using std::ofstream;
using std::ifstream;
using std::string;

namespace xeondb {

void writeU32(ofstream& out, u32 v);
void writeU64(ofstream& out, u64 v);
u32 readU32(ifstream& in);
u64 readU64(ifstream& in);

void writeString(ofstream& out, const string& s);
string readString(ifstream& in);

void writeBytes(ofstream& out, const byteVec& b);
byteVec readBytes(ifstream& in);

void appendU32(byteVec& out, u32 v);
u32 readU32(const byteVec& b, usize& o);

void appendBeU32(byteVec& out, u32 v);
u32 readBeU32(const byteVec& b, usize& o);

void appendBe32(byteVec& out, i32 v);
void appendBe64(byteVec& out, i64 v);
i32 readBe32(const byteVec& b, usize& o);
i64 readBe64(const byteVec& b, usize& o);

}
