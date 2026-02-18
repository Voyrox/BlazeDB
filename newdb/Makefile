SHELL := /usr/bin/env bash

cxx = g++
cxxFlags = -std=c++20 -O2 -Wall -Wextra -Wpedantic -pthread
includeFlags = -Iinclude

clangTidy = clang-tidy
clangFormat = clang-format

srcFiles = $(shell find src -type f -name '*.cpp' -print)
objFiles = $(patsubst src/%.cpp,build/%.o,$(srcFiles))

hdrFiles = $(shell find include -type f -name '*.h' -print)

build: lint build/blazedbd
	$(MAKE) test

lint:
	command -v $(clangTidy) >/dev/null 2>&1 || (echo "clang-tidy not installed" && exit 1)
	$(clangTidy) $(srcFiles) -- -std=c++20 -pthread $(includeFlags) || true

formatCheck:
	command -v $(clangFormat) >/dev/null 2>&1 || (echo "clang-format not installed" && exit 1)
	$(clangFormat) --dry-run -Werror $(srcFiles) $(hdrFiles)

format:
	command -v $(clangFormat) >/dev/null 2>&1 || (echo "clang-format not installed" && exit 1)
	$(clangFormat) -i $(srcFiles) $(hdrFiles)

build/blazedbd: $(objFiles)
	$(cxx) $(cxxFlags) -o $@ $^

build/%.o: src/%.cpp
	mkdir -p $(dir $@)
	$(cxx) $(cxxFlags) $(includeFlags) -c $< -o $@

run: build/blazedbd
	./build/blazedbd --config config/settings.yml

clean:
	rm -rf build

test: build/blazedbd
	py=python3; if [ -x .venv/bin/python3 ]; then py=.venv/bin/python3; fi; \
	$$py -c "import pytest" >/dev/null 2>&1 || (echo "pytest not installed. recommended: python3 -m venv .venv && .venv/bin/pip install -r tests/requirementsDev.txt" && exit 1); \
	$$py -m pytest -vv --durations=10
