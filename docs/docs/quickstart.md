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

Create a keyspace + table:

```sql
CREATE KEYSPACE IF NOT EXISTS myapp;
USE myapp;
CREATE TABLE IF NOT EXISTS users (id int64, name varchar, active boolean, PRIMARY KEY (id));
```

Insert and read a row:

```sql
INSERT INTO users (id,name,active) VALUES (1,"alice",true);
SELECT * FROM users WHERE id=1;
```

Scan a table (ordered by primary key):

```sql
SELECT * FROM users ORDER BY id ASC;
SELECT * FROM users ORDER BY id DESC;
```

Update (UPSERT) and read it back:

```sql
UPDATE users SET name="alice2" WHERE id=1;
SELECT * FROM users WHERE id=1;

UPDATE users SET name="bob", active=false WHERE id=2;
SELECT * FROM users WHERE id=2;
```

Delete:

```sql
DELETE FROM users WHERE id=1;
SELECT * FROM users WHERE id=1;
```

## Run tests

```bash
ninja -C build test
```
