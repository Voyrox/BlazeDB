# Permissions

This page explains how Xeondb permissions work when authentication is enabled.

## When permissions apply

- Auth disabled: no authentication is required and the server does not enforce permissions.
- Auth enabled: clients must authenticate first and keyspace access is checked per user.

Auth is enabled when both `auth.username` and `auth.password` are set in the server config.

## System/root account (level 0)

When auth is enabled, the credentials configured as `auth.username` / `auth.password` are used to bootstrap a system/root user with `level=0`.

Capabilities:

- Can access every keyspace, including the reserved `SYSTEM` keyspace.
- Can `CREATE KEYSPACE` / `DROP KEYSPACE`.
- Can manage users and keyspace access by writing to tables in `SYSTEM`.

## Regular users (level != 0)

Regular users are subject to keyspace-level access checks:

- Cannot access the `SYSTEM` keyspace.
- Cannot `CREATE KEYSPACE` / `DROP KEYSPACE`.
- Can access a keyspace only if they are the owner or have an explicit grant.

There are no table-level permissions today; access is enforced at the keyspace level.

## How access is stored (SYSTEM tables)

When auth is enabled, Xeondb creates a reserved `SYSTEM` keyspace that stores security metadata.

### SYSTEM.USERS

Stores user credentials and status.

Columns:

- `username` (primary key)
- `password`
- `level` (int32)
- `enabled` (boolean)
- `created_at`

### SYSTEM.KEYSPACE_OWNERS

Stores ownership for each keyspace.

Columns:

- `keyspace` (primary key)
- `owner_username`
- `created_at`

Notes:

- New keyspaces are owned by the user who created them.
- Only `level=0` can create keyspaces, so ownership is typically transferred to regular users after creation.

### SYSTEM.KEYSPACE_GRANTS

Stores explicit access grants.

Columns:

- `keyspace_username` (primary key), formatted as `"<keyspace>#<username>"`
- `created_at` (timestamp)

Example value: `"myapp#alice"`.

## Managing permissions (SQL examples)

All of the examples below assume auth is enabled.

### Authenticate

Until you authenticate, any non-`AUTH` command returns `unauthorized`.

```sql
AUTH "<root-username>" "<root-password>";
```

### Create a regular user

```sql
INSERT INTO SYSTEM.USERS (username,password,level,enabled) VALUES ("alice","change-me",1,true);
```

### Disable (or re-enable) a user

```sql
UPDATE SYSTEM.USERS SET password="change-me", level=1, enabled=false WHERE username="alice";
```

Note: for `UPDATE SYSTEM.USERS`, Xeondb currently expects `password`, `level`, and `enabled` to be set together for the in-memory auth cache to update immediately.

### Delete a user

```sql
DELETE FROM SYSTEM.USERS WHERE username="alice";
```

### Create a keyspace (root only)

```sql
CREATE KEYSPACE IF NOT EXISTS myapp;
```

### Transfer keyspace ownership

```sql
UPDATE SYSTEM.KEYSPACE_OWNERS SET owner_username="alice" WHERE keyspace="myapp";
```

### Grant and revoke keyspace access

Grant access:

```sql
INSERT INTO SYSTEM.KEYSPACE_GRANTS (keyspace_username) VALUES ("myapp#bob");
```

Revoke access:

```sql
DELETE FROM SYSTEM.KEYSPACE_GRANTS WHERE keyspace_username="myapp#bob";
```

## What changes for listing keyspaces

- With auth disabled: `SHOW KEYSPACES;` lists all keyspaces on disk.
- With auth enabled:
  - `level=0` sees all keyspaces.
  - Regular users see only keyspaces they own or have been granted access to.
  - `SYSTEM` is never visible to regular users.

## Reserved SYSTEM keyspace

`SYSTEM` is reserved:

- It cannot be created or dropped via normal SQL.
- When auth is enabled, only the system/root account can access it.
