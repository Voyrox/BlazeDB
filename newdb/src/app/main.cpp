#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "config/config.h"
#include "core/db.h"
#include "net/serverTcp.h"

using std::cout;
using std::string;
using std::cerr;
using std::endl;

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
        cout << "blazedbd 0.1" << endl;
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
        cerr << e.what() << endl;
        return 1;
    }

    cout << "blazedbd starting configPath=" << configPath << endl;

    auto db = std::make_shared<blazeDb::Db>(settings);

    cout << "blazedbd config host=" << settings.host
              << " port=" << settings.port
              << " dataDir=" << db->dataDir().string()
              << " walFsync=" << settings.walFsync
              << " walFsyncIntervalMs=" << settings.walFsyncIntervalMs
              << " walFsyncBytes=" << settings.walFsyncBytes
              << " maxLineBytes=" << settings.maxLineBytes
              << " maxConnections=" << settings.maxConnections
              << endl;

    blazeDb::ServerTcp server(db, settings.host, settings.port, settings.maxLineBytes, settings.maxConnections);

    try
    {
        server.run();
    }
    catch (const std::exception &e)
    {
        cerr << e.what() << endl;
        return 1;
    }
    return 0;
}
