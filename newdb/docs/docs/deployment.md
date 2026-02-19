# Deployment

This page focuses on the basics: keeping your data durable, configuring the server, and running it via Docker or as a native binary.

## Option 1: Run the native binary

Build `Xeondb` (see [Quick Start](quickstart.md)), then run it with a config file:

```bash
./build/xeondb --config /etc/xeondb/settings.yml
```

Make sure the configured `dataDir` points to persistent storage.

## Option 2: Run with Docker

```bash
docker build -t xeondb .
docker run --name xeondb
```

For real deployments you will typically also:

- publish a port (`-p hostPort:containerPort`)
- mount a persistent volume for `dataDir`
- mount a config file into the container

## Configuration

Xeondb is configured via YAML. The config controls networking, storage, limits, and storage-engine tuning.

Example:

```yml
network:
  host: 0.0.0.0
  port: 9876

storage:
  dataDir: /var/lib/xeondb/data

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

## Durability and restarts

- Data persistence is tied to `storage.dataDir`. If you change or wipe it, your data is gone.
- For best durability, keep WAL fsync enabled (defaults are set in config).
- If you want to force memtable contents to disk, run:

```sql
FLUSH myapp.users;
```
