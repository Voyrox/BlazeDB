# Query Examples

## Create a keyspace and table

```sql
CREATE KEYSPACE IF NOT EXISTS myapp;
CREATE TABLE IF NOT EXISTS myapp.users (id int64, name varchar, active boolean, PRIMARY KEY (id));
```

## Show keyspaces and tables

List keyspaces:

```sql
SHOW KEYSPACES;
```

Response shape:

```json
{"ok":true,"keyspaces":["myapp"]}
```

List tables in a keyspace:

```sql
SHOW TABLES IN myapp;
```

Response shape:

```json
{"ok":true,"tables":["users"]}
```

## Describe schema / show create

```sql
DESCRIBE TABLE myapp.users;
SHOW CREATE TABLE myapp.users;
```

`DESCRIBE TABLE` response shape:

```json
{"ok":true,"keyspace":"myapp","table":"users","primaryKey":"id","columns":[{"name":"id","type":"int64"},{"name":"name","type":"varchar"}]}
```

`SHOW CREATE TABLE` response shape:

```json
{"ok":true,"create":"CREATE TABLE myapp.users (id int64, name varchar, PRIMARY KEY (id));"}
```

## Insert rows (single + multi-row)

```sql
INSERT INTO myapp.users (id,name,active) VALUES (1,"alice",true);
INSERT INTO myapp.users (id,name,active) VALUES (2,"bob",false), (3,"carol",true);
```

## Select by primary key

`SELECT ... WHERE` currently supports primary-key lookups.

```sql
SELECT * FROM myapp.users WHERE id=1;
```

Response shape:

```json
{"ok":true,"found":true,"row":{"id":1,"name":"alice","active":true}}
```

If the row doesn't exist:

```json
{"ok":true,"found":false}
```

## Scan + ORDER BY (ASC/DESC)

To return multiple rows, omit `WHERE` and use `ORDER BY` on the primary key.

```sql
SELECT * FROM myapp.users ORDER BY id ASC;
SELECT * FROM myapp.users ORDER BY id DESC;
```

Response shape:

```json
{"ok":true,"rows":[{"id":1,"name":"alice","active":true},{"id":2,"name":"bob","active":false}]}
```

Notes:

- `ASC` is the default if you omit it.
- `ORDER BY` must use the primary key column.

## Update (upsert)

`UPDATE` acts like an upsert (creates the row if missing).

```sql
UPDATE myapp.users SET name="alice2" WHERE id=1;
UPDATE myapp.users SET name="dave", active=true WHERE id=10;
```

## Delete

```sql
DELETE FROM myapp.users WHERE id=2;
```

## Flush

Force the table's memtable contents to disk:

```sql
FLUSH myapp.users;
```

## Truncate

Delete all rows in a table but keep its schema:

```sql
TRUNCATE TABLE myapp.users;
```

## Drop

Drop a table:

```sql
DROP TABLE myapp.users;
DROP TABLE IF EXISTS myapp.users;
```

Drop a keyspace:

```sql
DROP KEYSPACE myapp;
DROP KEYSPACE IF EXISTS myapp;
```
