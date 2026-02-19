# Xeondb

Xeondb is a blazing fast NoSQL database built for modern applications. It aims to be lightweight, easy to run, and straightforward to integrate into your app.

This docs site covers the `newdb/` server: how to build it, run it locally, and send SQL over the TCP protocol.

## What you can do

- Run a single binary with a small YAML config.
- Create keyspaces and tables.
- Read and write rows by primary key.
- Use a small SQL subset:
  - `PING`, `USE`
  - `CREATE KEYSPACE`, `CREATE TABLE`
  - `INSERT`, `SELECT`, `UPDATE`, `DELETE`
  - `FLUSH`

## Next steps

- Start here: [Quick Start](quickstart.md)
- Running it somewhere real: [Deployment](deployment.md)
