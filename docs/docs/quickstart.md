# Quick Start

This quick start builds the database server, runs it locally, and executes a few example queries over TCP.

## Build

```bash
cmake -S . -B build -G Ninja
ninja -C build
```

## Run the server

Use the built-in run target (it uses `config/settings.yml` by default):

```bash
ninja -C build run
```

If you want a custom config, run the binary directly:

```bash
./build/xeondb --config path/to/settings.yml
```

## Example queries

To keep this page short, the full set of examples lives in [Query Examples](query-examples.md).

Quick smoke check:

```sql
PING;
```

## Run tests

```bash
ninja -C build test
```
