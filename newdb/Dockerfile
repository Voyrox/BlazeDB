FROM ubuntu:22.04

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    cmake \
    gcc \
    g++ \
    ninja-build \
    build-essential \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN mkdir -p build
WORKDIR /app/build

RUN cmake .. -G Ninja && ninja

CMD ["/bin/bash"]
