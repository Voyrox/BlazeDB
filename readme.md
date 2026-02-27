<img src="banner.png" alt="Xeondb Logo" width="100%"/>
<p align="center">
    <a href="https://github.com/xeondb/Xeondb"><img src="https://img.shields.io/github/v/release/xeondb/Xeondb?color=%23ff00a0&include_prereleases&label=Version&sort=semver&style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/xeondb/Xeondb"><img src="https://img.shields.io/badge/Built_with-c++-dca282.svg?style=flat-square"></a>
    &nbsp;
	<a href="https://github.com/xeondb/Xeondb/actions"><img src="https://img.shields.io/github/actions/workflow/status/xeondb/Xeondb/ci.yml?style=flat-square&branch=main"></a>
    &nbsp;
    <a href="https://xeondb.com"><img src="https://img.shields.io/uptimerobot/ratio/7/m784409192-e472ca350bb615372ededed7?label=cloud%20uptime&style=flat-square"></a>
    &nbsp;
    <a href="https://hub.docker.com/r/xeondb/Xeondb"><img src="https://img.shields.io/docker/pulls/xeondb/Xeondb?style=flat-square"></a>
    &nbsp;
    <a href="https://github.com/xeondb/Xeondb/blob/master/LICENSE.txt"><img src="https://img.shields.io/badge/license-MIT-00bfff.svg?style=flat-square"></a>
</p>

## What is Xeondb?
Xeondb is a real-time big data database that is designed to be fast, efficient, and easy to use. It is built on top of a custom storage engine that is optimized for high performance and low latency.

## Build Prerequisites
Xeondb is fussy about the build environment. It requires a C++15 compliant compiler and CMake 3.20 or higher. A pre-configured Docker image is available for those who want to avoid the hassle of setting up the build environment. You also have the option to build the project natively on your machine, but be prepared for some potential configuration challenges.

## Building Xeondb

Building Xeondb is straightforward with CMake + Ninja. The provided Docker image is also available for a hassle-free build process.

### Using Ninja
```bash
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
docker build -t xeondb . # Builds the Docker image for Xeondb
docker run --name xeondb # Runs the Xeondb server in a Docker container
```

## Configuration
Xeondb can be configured using a configuration file. The configuration file allows you to specify various settings such as the server port, data directory, and logging options. Environment variables can also be used to override specific configuration settings without modifying the configuration file.

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
  # username: admin
  # password: change-me
```

### Testing

Xeondb tests are `pytest` and are available via CTest (Ninja/CMake).

With Ninja/CMake:
```bash
ninja -C build test
```

## Documentation
Docs can be found [here](https://voyrox.github.io/Xeondb/). The documentation includes a quick start guide, deployment instructions, and detailed information about the database's features and configuration options.

## Keyspace Quotas

XeonDB can enforce per-keyspace storage quotas by switching the server into a "read-only when over quota" mode.

Config (`limits:` section):

```yml
limits:
  quotaEnforcementEnabled: true
  quotaBytesUsedCacheTtlMs: 2000
```

Quota rows live in `SYSTEM.KEYSPACE_QUOTAS` (requires auth enabled). If a keyspace has no quota row (or `quota_bytes <= 0`), it is treated as unlimited.

Set a quota (example: 500MB):

```sql
INSERT INTO SYSTEM.KEYSPACE_QUOTAS (keyspace,quota_bytes,updated_at)
VALUES ("myks", 524288000, 0);
```

Update a quota (example: 1GB):

```sql
UPDATE SYSTEM.KEYSPACE_QUOTAS
SET quota_bytes=1073741824
WHERE keyspace="myks";
```

Remove a quota (unlimited):

```sql
DELETE FROM SYSTEM.KEYSPACE_QUOTAS
WHERE keyspace="myks";
```

When a keyspace is over quota, XeonDB rejects writes that increase storage (`INSERT`, `UPDATE`, `CREATE TABLE`, etc.) with `quota_exceeded`, but still allows reads and recovery operations (`TRUNCATE`, `DROP`).

## Contributing
If you want to report a bug, suggest a feature, or contribute to the development of Xeondb, please feel free to open an issue or submit a pull request on our GitHub repository. We welcome contributions from the community and are always looking for ways to improve Xeondb.

## References
- Patrick O’Neil, Edward Cheng, Dieter Gawlick, and Elizabeth O’Neil. 1996. The log-structured merge-tree (LSM-tree). Acta Inf. 33, 4 (Jun 1996), 351–385. https://doi.org/10.1007/s002360050048
- Chang, F. et al. (2006). Bigtable: A Distributed Storage System for Structured Data. OSDI. https://research.google/pubs/pub27898/
- Mohan, C., Haderle, D., Lindsay, B., Pirahesh, H., & Schwarz, P. (1992). ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging. ACM TODS. https://dl.acm.org/doi/10.1145/128765.128770
- Gray, J., & Reuter, A. (1992). Transaction Processing: Concepts and Techniques. Morgan Kaufmann. (classic WAL/recovery book reference)
- Rosenblum, M., & Ousterhout, J. (1992). The Design and Implementation of a Log-Structured File System. ACM TOCS. https://dl.acm.org/doi/10.1145/146941.146943
- Lakshman, A., & Malik, P. (2009). Cassandra: A Decentralized Structured Storage System. LADIS. https://www.cs.cornell.edu/projects/ladis2009/papers/lakshman-ladis2009.pdf
- DeCandia, G. et al. (2007). Dynamo: Amazon’s Highly Available Key-value Store. SOSP. (partitioning/consistent-hashing lineage) https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
- Bloom, B. (1970). Space/Time Trade-offs in Hash Coding with Allowable Errors. CACM. https://dl.acm.org/doi/10.1145/362686.362692
- https://github.com/PeterScott/murmur3
- Sears, R., & Ramakrishnan, R. (2012). bLSM: a general purpose log structured merge tree. SIGMOD. https://doi.org/10.1145/2213836.2213862
- Dayan, N., Athanassoulis, M., & Idreos, S. (2018). Optimal Bloom Filters and Adaptive Merging for LSM-Trees. ACM TODS. https://doi.org/10.1145/3276980
- Lu, L., Pillai, T. S., Arpaci-Dusseau, A. C., & Arpaci-Dusseau, R. H. (2016). WiscKey: Separating Keys from Values in SSD-conscious Storage. FAST. https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf
- Guo, L., Teng, D., Lee, R., Chen, F., Ma, S., & Zhang, X. (2016). Re-enabling high-speed caching for LSM-trees (dLSM). arXiv. https://arxiv.org/abs/1606.02015
- Zhong, W., Chen, C., Wu, X., & Jiang, S. (2020). REMIX: Efficient Range Query for LSM-trees. arXiv. https://arxiv.org/abs/2010.12734
- Mishra, S. (2024). A survey of LSM-Tree based Indexes, Data Systems and KV-stores. arXiv. https://arxiv.org/abs/2402.10460
- Lv, Y., Li, Q., Xu, Q., Gao, C., Yang, C., Wang, X., & Xue, C. J. (2025). Rethinking LSM-tree based Key-Value Stores: A Survey. arXiv. https://arxiv.org/abs/2507.09642
- Huynh, A., Chaudhari, H. A., Terzi, E., & Athanassoulis, M. (2023). Towards Flexibility and Robustness of LSM Trees. arXiv. https://arxiv.org/abs/2311.10005
- Cooper, B. F., Silberstein, A., Tam, E., Ramakrishnan, R., & Sears, R. (2010). Benchmarking cloud serving systems with YCSB. SoCC. https://doi.org/10.1145/1807128.1807152
- Karger, D. R., Lehman, E., Leighton, F. T., Panigrahy, R., Levine, M. S., & Lewin, D. (1997). Consistent Hashing and Random Trees: Distributed Caching Protocols for Relieving Hot Spots on the World Wide Web. STOC. https://doi.org/10.1145/258533.258660 (alt PDF: https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf)
- Ghemawat, S., Gobioff, H., & Leung, S.-T. (2003). The Google File System. SOSP. https://storage.googleapis.com/gweb-research2023-media/pubtools/4446.pdf
