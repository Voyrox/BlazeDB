#pragma once

#include "prelude.h"

using std::string;

namespace blazeDb
{

byteVec hexToBytes(const string& hex);
byteVec base64ToBytes(const string& s);
string bytesToBase64(const byteVec& data);

}
