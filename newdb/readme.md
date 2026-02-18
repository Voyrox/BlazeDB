# BlazeDB

## What is BlazeDB?
BlazeDB is a real-time big data database that is designed to be fast, efficient, and easy to use. It is built on top of a custom storage engine that is optimized for high performance and low latency.

## Build Prerequisites
BlazeDB is fussy about the build environment. It requires a C++15 compliant compiler and CMake 3.20 or higher. A pre-configured Docker image is available for those who want to avoid the hassle of setting up the build environment. You also have the option to build the project natively on your machine, but be prepared for some potential configuration challenges.

## Building BlazeDB

Building BlazeDB is straightforward with CMake. You can use the provided Docker image for a hassle-free build process, or you can choose to build it natively on your machine if you prefer.

### Using CMake
```bash
make build # Builds the project using CMake and runs tests
make run   # Runs the BlazeDB server
```

### Using Docker
```bash
docker build -t blazedb . # Builds the Docker image for BlazeDB
docker run --rm -it blazedb # Runs the BlazeDB server in a Docker container
```

### Testing

BlazeDB auto runs tests during the build process. You can also run tests separately using CMake:
```bash
make test # Runs all tests for BlazeDB
```

## Documentation
The documentation for BlazeDB is currently a work in progress. We are actively working on creating comprehensive documentation to help users get started and make the most of BlazeDB. In the meantime, you can refer to the source code and comments for insights into how BlazeDB works.

## Contributing
If you want to report a bug, suggest a feature, or contribute to the development of BlazeDB, please feel free to open an issue or submit a pull request on our GitHub repository. We welcome contributions from the community and are always looking for ways to improve BlazeDB.