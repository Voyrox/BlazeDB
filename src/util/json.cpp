#include "util/json.h"

#include <cstdio>

using std::string;

namespace xeondb {

static string hex2(unsigned char v) {
    const char* table = "0123456789abcdef";
    string s;
    s.push_back(table[(v >> 4) & 0xF]);
    s.push_back(table[v & 0xF]);
    return s;
}

string jsonEscape(const string& s) {
    string data;
    data.reserve(s.size() + 8);
    for (unsigned char c : s) {
        if (c == '"') {
            data += "\\\"";
        } else if (c == '\\') {
            data += "\\\\";
        } else if (c == '\b') {
            data += "\\b";
        } else if (c == '\f') {
            data += "\\f";
        } else if (c == '\n') {
            data += "\\n";
        } else if (c == '\r') {
            data += "\\r";
        } else if (c == '\t') {
            data += "\\t";
        } else if (c < 0x20) {
            data += "\\u00";
            data += hex2(c);
        } else {
            data.push_back(static_cast<char>(c));
        }
    }
    return data;
}

string jsonOk() {
    return string("{\"ok\":true}");
}

string jsonString(const string& key, const string& value) {
    return string("{\"ok\":true,\"") + jsonEscape(key) + "\":\"" + jsonEscape(value) + "\"}";
}

string jsonBool(const string& key, bool value) {
    return string("{\"ok\":true,\"") + jsonEscape(key) + "\":" + (value ? "true" : "false") + "}";
}

string jsonOptionalString(const string& key, const std::optional<string>& value) {
    if (!value.has_value()) {
        return string("{\"ok\":true,\"") + jsonEscape(key) + "\":null}";
    }
    return string("{\"ok\":true,\"") + jsonEscape(key) + "\":\"" + jsonEscape(*value) + "\"}";
}

string jsonError(const string& message) {
    return string("{\"ok\":false,\"error\":\"") + jsonEscape(message) + "\"}";
}

}
