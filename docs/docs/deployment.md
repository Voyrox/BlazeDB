# Deployment

This page focuses on the basics: keeping your data durable, configuring the server, and running it via Docker or as a native binary.

## Option 1: Run the install script
The install script sets up Xeondb as a systemd service on Linux, with sane defaults for durability and performance.

```bash
sudo ./install.sh
```

## Option 2: Run the native binary

Build `Xeondb` (see [Quick Start](quickstart.md)), then run it with a config file:

```bash
ninja -C build run
```

Make sure the configured `dataDir` points to persistent storage.

## Option 3: Run with Docker

```bash
docker build -t xeondb .
docker run --name xeondb (timestamp)
```

For real deployments you will typically also:

- publish a port (`-p hostPort:containerPort`)
- mount a persistent volume for `dataDir`
- mount a config file into the container

## Option 4: ISO
Coming soon: a standalone ISO image with everything included, for easy deployment on bare metal or in the cloud.

## Configuration

Xeondb is configured via YAML. The config controls networking, storage, limits, and storage-engine tuning.

Example:

```yml
# XeonDB server configuration

# Network configuration
# - host: IP address to bind to (0.0.0.0 = all interfaces)
# - port: TCP port to listen on
network:
  host: 0.0.0.0
  port: 9876

# Storage configuration
# - Directory where all keyspaces/tables/WAL live
storage:
  dataDir: /var/lib/xeondb/data

# Limits / safety valves
# - maxLineBytes: maximum bytes per request line (SQL + newline)
# - maxConnections: max concurrent TCP connections
limits:
  maxLineBytes: 1048576
  maxConnections: 1024
  # Optional quota enforcement (auth-enabled deployments only)
  # - quotaEnforcementEnabled: when true, enforce per keyspace quota rows in SYSTEM.KEYSPACE_QUOTAS and reject writes that exceed the quota with "quota_exceeded".
  # - quotaBytesUsedCacheTtlMs: Limit the size that a keyspace can be.
  quotaEnforcementEnabled: false
  quotaBytesUsedCacheTtlMs: 2000

# Write-ahead log (WAL)
# - walFsync: "always" or "periodic" (periodic is faster, slightly less durable)
# - walFsyncIntervalMs: periodic fsync interval
# - walFsyncBytes: optional size hint for fsync batching
wal:
  walFsync: periodic
  walFsyncIntervalMs: 50
  walFsyncBytes: 1048576

# In-memory write buffer
# - Max bytes in memtable before flush is needed
memtable:
  memtableMaxBytes: 33554432

# SSTable configuration
# - Index entry frequency (smaller = faster reads, larger = smaller files)
sstable:
  sstableIndexStride: 16

# Optional authentication.
# - If both username and password are set, clients must authenticate first.
# - If either is missing/empty, auth is disabled.
# - Client must send: AUTH "<username>" "<password>";
auth:
  # username: root
  # password: change-me
```

When auth is enabled, the configured credentials become the system/root account (`level=0`).
See [Permissions](permissions.md) for how keyspace access and user management works.

## Durability and restarts

- Data persistence is tied to `storage.dataDir`. If you change or wipe it, your data is gone.
- For best durability, keep WAL fsync enabled (defaults are set in config).
- If you want to force memtable contents to disk, run:

```sql
FLUSH myapp.users;
```
