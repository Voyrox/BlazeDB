#pragma once

#include "prelude.h"

#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

using std::optional;
using std::string;
using std::unordered_map;
using std::vector;
using std::pair;

namespace xeondb
{

    struct MemValue
    {
        u64 seq;
        byteVec value;
    };

    class MemTable
    {
    public:
        MemTable();

        void put(const string &key, u64 seq, const byteVec &value);
        optional<MemValue> get(const string &key) const;
        usize bytes() const;
        usize size() const;
        void clear();

        vector<pair<string, MemValue>> snapshot() const;

    private:
        unordered_map<string, MemValue> map;
        usize bytes_;
    };

}
