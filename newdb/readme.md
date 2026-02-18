# BlazeDB

## What is BlazeDB?
BlazeDB is a real-time big data database that is designed to be fast, efficient, and easy to use. It is built on top of a custom storage engine that is optimized for high performance and low latency.

## Build Prerequisites
BlazeDB is fussy about the build environment. It requires a C++15 compliant compiler and CMake 3.20 or higher. A pre-configured Docker image is available for those who want to avoid the hassle of setting up the build environment. You also have the option to build the project natively on your machine, but be prepared for some potential configuration challenges.

## Building BlazeDB


Building BlazeDB is straightforward with CMake + Ninja. The provided Docker image is also available for a hassle-free build process.

### Using Ninja
```bash
# From inside newdb/
cmake -S . -B build -G Ninja  # Configure project for Ninja
ninja -C build                # Build the project

# Common targets
ninja -C build test           # Run test suite
ninja -C build lint           # Run clang-tidy
ninja -C build formatCheck    # Check formatting
ninja -C build format         # Apply formatting
ninja -C build run            # Run the server (config/settings.yml)

# Cleaning
ninja -C build clean          # Remove build outputs only
ninja -C build wipeData       # Wipe database file contents (/var/lib/... and ./var/lib/...)
ninja -C build purge          # wipeData + delete build contents

# Optional: Full build (build + lint + tests)
ninja -C build build
```

### Using Docker
```bash
docker build -t blazedb . # Builds the Docker image for BlazeDB
docker run --name blazedb # Runs the BlazeDB server in a Docker container
```

## Configuration
BlazeDB can be configured using a configuration file. The configuration file allows you to specify various settings such as the server port, data directory, and logging options. Environment variables can also be used to override specific configuration settings without modifying the configuration file.

```yml
network:
  host: 0.0.0.0
  port: 9876

storage:
  dataDir: /var/lib/blazedb/data

limits:
  maxLineBytes: 1048576
  maxConnections: 1024

wal:
  walFsync: periodic
  walFsyncIntervalMs: 50
  walFsyncBytes: 1048576

memtable:
  memtableMaxBytes: 33554432

sstable:
  sstableIndexStride: 16
```

### Testing

BlazeDB tests are `pytest` and are available via CTest (Ninja/CMake).

With Ninja/CMake:
```bash
ninja -C build test
```


## Documentation
The documentation for BlazeDB is currently a work in progress. We are actively working on creating comprehensive documentation to help users get started and make the most of BlazeDB. In the meantime, you can refer to the source code and comments for insights into how BlazeDB works.

## Contributing
If you want to report a bug, suggest a feature, or contribute to the development of BlazeDB, please feel free to open an issue or submit a pull request on our GitHub repository. We welcome contributions from the community and are always looking for ways to improve BlazeDB.
