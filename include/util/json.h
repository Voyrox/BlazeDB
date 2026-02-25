#pragma once

#include <optional>
#include <string>

using std::string;

namespace xeondb {

string jsonEscape(const string& s);

string jsonOk();
string jsonString(const string& key, const string& value);
string jsonBool(const string& key, bool value);
string jsonOptionalString(const string& key, const std::optional<string>& value);
string jsonError(const string& message);

}
