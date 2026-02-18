#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "util/log.h"

#include "config/config.h"
#include "core/db.h"
#include "net/serverTcp.h"

using std::string;

static string getArgValue(int argc, char **argv, const string &name, const string &defaultValue)
{
    for (int i = 1; i + 1 < argc; i++)
    {
        if (string(argv[i]) == name)
        {
            return string(argv[i + 1]);
        }
    }
    return defaultValue;
}

static bool hasArg(int argc, char **argv, const string &name)
{
    for (int i = 1; i < argc; i++)
    {
        if (string(argv[i]) == name)
        {
            return true;
        }
    }
    return false;
}

int main(int argc, char **argv)
{
    if (hasArg(argc, argv, "--version"))
    {
        std::cout << "blazedbd 0.1" << std::endl;
        return 0;
    }

    string configPath = getArgValue(argc, argv, "--config", "config/settings.yml");
    blazeDb::Settings settings;
    try
    {
        settings = blazeDb::loadSettings(configPath);
    }
    catch (const std::exception &e)
    {
        blazeDb::log(blazeDb::LogLevel::ERROR, e.what());
        return 1;
    }

    blazeDb::log(blazeDb::LogLevel::INFO, std::string("Loading configPath=") + configPath);

    auto db = std::make_shared<blazeDb::Db>(settings);

    blazeDb::log(
        blazeDb::LogLevel::CONFIG,
        std::string("Host=") + settings.host +
            " port=" + std::to_string(settings.port) +
            " dataDir=" + db->dataDir().string() +
            " walFsync=" + settings.walFsync +
            " walFsyncIntervalMs=" + std::to_string(settings.walFsyncIntervalMs) +
            " walFsyncBytes=" + std::to_string(settings.walFsyncBytes) +
            " maxLineBytes=" + std::to_string(settings.maxLineBytes) +
            " maxConnections=" + std::to_string(settings.maxConnections));

    blazeDb::ServerTcp server(db, settings.host, settings.port, settings.maxLineBytes, settings.maxConnections);

    try
    {
        server.run();
    }
    catch (const std::exception &e)
    {
        blazeDb::log(blazeDb::LogLevel::ERROR, e.what());
        return 1;
    }
    return 0;
}
