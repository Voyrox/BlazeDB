# Quick Start

This quick start builds the `newdb/` server, runs it locally, and executes a few example queries over TCP.

## Build

From the `newdb/` directory:

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

## Try queries

BlazeDB speaks a simple line-based TCP protocol: send one SQL statement per line, receive one JSON response per line.

With `nc` (netcat):

```bash
printf 'PING;\n' | nc 127.0.0.1 9876
```

Create a keyspace + table:

```bash
printf 'CREATE KEYSPACE IF NOT EXISTS myapp;\n' | nc 127.0.0.1 9876
printf 'USE myapp;\n' | nc 127.0.0.1 9876
printf 'CREATE TABLE IF NOT EXISTS users (id int64, name varchar, active boolean, PRIMARY KEY (id));\n' | nc 127.0.0.1 9876
```

Insert and read a row:

```bash
printf 'INSERT INTO users (id,name,active) VALUES (1,"alice",true);\n' | nc 127.0.0.1 9876
printf 'SELECT * FROM users WHERE id=1;\n' | nc 127.0.0.1 9876
```

Update (UPSERT) and read it back:

```bash
printf 'UPDATE users SET name="alice2" WHERE id=1;\n' | nc 127.0.0.1 9876
printf 'SELECT * FROM users WHERE id=1;\n' | nc 127.0.0.1 9876

printf 'UPDATE users SET name="bob", active=false WHERE id=2;\n' | nc 127.0.0.1 9876
printf 'SELECT * FROM users WHERE id=2;\n' | nc 127.0.0.1 9876
```

Delete:

```bash
printf 'DELETE FROM users WHERE id=1;\n' | nc 127.0.0.1 9876
printf 'SELECT * FROM users WHERE id=1;\n' | nc 127.0.0.1 9876
```

## Run tests

```bash
ninja -C build test
```
