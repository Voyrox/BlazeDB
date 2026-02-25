#pragma once

#include "prelude.h"

#include "query/schema.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>

#include <sys/socket.h>

namespace xeondb::server_tcp_detail {

inline i64 nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

inline SqlLiteral litQuoted(const std::string& s) {
    SqlLiteral l;
    l.kind = SqlLiteral::Kind::Quoted;
    l.text = s;
    return l;
}

inline SqlLiteral litNumber(i64 v) {
    SqlLiteral l;
    l.kind = SqlLiteral::Kind::Number;
    l.text = std::to_string(v);
    return l;
}

inline bool sendAll(int fd, const std::string& s) {
    const char* p = s.data();
    const usize n = s.size();
    usize sent = 0;
    while (sent < n) {
        ssize_t sentBytes = ::send(fd, p + sent, n - sent, 0);
        if (sentBytes < 0) {
            if (errno == EINTR)
                continue;
            return false;
        }
        sent += static_cast<usize>(sentBytes);
    }
    return true;
}

inline runtimeError errnoError(const std::string& prefix) {
    return runtimeError(prefix + " errno=" + std::to_string(errno) + " err=" + std::string(::strerror(errno)));
}

inline bool schemaEquals(const TableSchema& schema, const TableSchema& schemaB) {
    if (schema.primaryKeyIndex != schemaB.primaryKeyIndex)
        return false;
    if (schema.columns.size() != schemaB.columns.size())
        return false;
    for (usize i = 0; i < schema.columns.size(); i++) {
        if (schema.columns[i].name != schemaB.columns[i].name)
            return false;
        if (schema.columns[i].type != schemaB.columns[i].type)
            return false;
    }
    return true;
}

}
