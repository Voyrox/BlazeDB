#include "storage/memTable.h"

namespace xeondb
{

    MemTable::MemTable() : bytes_(0) {}

    void MemTable::put(const std::string &key, u64 seq, const byteVec &value)
    {
        auto data = map.find(key);
        if (data != map.end())
        {
            bytes_ -= data->first.size();
            bytes_ -= data->second.value.size();
            data->second.seq = seq;
            data->second.value = value;
        }
        else
        {
            map.insert({key, MemValue{seq, value}});
        }
        bytes_ += key.size();
        bytes_ += value.size();
    }

    std::optional<MemValue> MemTable::get(const std::string &key) const
    {
        auto data = map.find(key);
        if (data == map.end())
        {
            return std::nullopt;
        }
        return data->second;
    }

    usize MemTable::bytes() const
    {
        return bytes_;
    }

    usize MemTable::size() const
    {
        return map.size();
    }

    void MemTable::clear()
    {
        map.clear();
        bytes_ = 0;
    }

    std::vector<std::pair<string, MemValue>> MemTable::snapshot() const
    {
        std::vector<std::pair<string, MemValue>> entries;
        entries.reserve(map.size());
        for (const auto &kv : map)
        {
            entries.push_back(kv);
        }
        return entries;
    }

}
