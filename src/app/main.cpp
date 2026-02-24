#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "util/log.h"

#include "config/config.h"
#include "core/db.h"
#include "net/serverTcp.h"

using std::string;

static string getArgValue(int argc, char** argv, const string& name, const string& defaultValue) {
    for (int i = 1; i + 1 < argc; i++) {
        if (string(argv[i]) == name) {
            return string(argv[i + 1]);
        }
    }
    return defaultValue;
}

static bool hasArg(int argc, char** argv, const string& name) {
    for (int i = 1; i < argc; i++) {
        if (string(argv[i]) == name) {
            return true;
        }
    }
    return false;
}

int main(int argc, char** argv) {
    if (hasArg(argc, argv, "--version")) {
        std::cout << "xeondbd 0.1" << std::endl;
        return 0;
    }

    string configPath = getArgValue(argc, argv, "--config", "config/settings.yml");
    xeondb::Settings settings;
    try {
        settings = xeondb::loadSettings(configPath);
    } catch (const std::exception& e) {
        xeondb::log(xeondb::LogLevel::ERROR, e.what());
        return 1;
    }

    xeondb::log(xeondb::LogLevel::INFO, std::string("Loading configPath=") + configPath);

    if (settings.quotaEnforcementEnabled && (settings.authUsername.empty() || settings.authPassword.empty())) {
        xeondb::log(xeondb::LogLevel::WARN, "Quota enforcement enabled but auth is disabled; quotas will not be enforced");
    }

    auto db = std::make_shared<xeondb::Db>(settings);
    try {
        db->bootstrapAuthSystem();
    } catch (const std::exception& e) {
        xeondb::log(xeondb::LogLevel::ERROR, e.what());
        return 1;
    }

    xeondb::log(xeondb::LogLevel::CONFIG,
            std::string("Host=") + settings.host + " port=" + std::to_string(settings.port) + " dataDir=" + db->dataDir().string() +
                    " walFsync=" + settings.walFsync + " walFsyncIntervalMs=" + std::to_string(settings.walFsyncIntervalMs) +
                    " walFsyncBytes=" + std::to_string(settings.walFsyncBytes) + " maxLineBytes=" + std::to_string(settings.maxLineBytes) + " maxConnections=" +
                    std::to_string(settings.maxConnections) + " quota=" + std::string(settings.quotaEnforcementEnabled ? "enabled" : "disabled") +
                    " auth=" + ((!settings.authUsername.empty() && !settings.authPassword.empty()) ? "enabled" : "disabled"));

    xeondb::ServerTcp server(db, settings.host, settings.port, settings.maxLineBytes, settings.maxConnections, settings.authUsername, settings.authPassword);

    try {
        server.run();
    } catch (const std::exception& e) {
        xeondb::log(xeondb::LogLevel::ERROR, e.what());
        return 1;
    }
    return 0;
}
