#pragma once

#include "prelude.h"

#include <optional>
#include <string>

using std::string;

namespace blazeDb
{

    path keyspaceDir(const path &dataDir, const string &keyspace);
    path schemaPath(const path &dataDir, const string &keyspace);

    std::optional<string> findTableUuidFromSchema(const path &schemaFile, const string &table);
    void upsertTableUuidToSchema(const path &schemaFile, const string &table, const string &uuid);
    std::optional<string> findTableUuidByScan(const path &keyspaceDirPath, const string &table);

    path tableDir(const path &dataDir, const string &keyspace, const string &table, const string &uuid);

}
