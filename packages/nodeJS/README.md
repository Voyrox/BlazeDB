# XeonDB NodeJS driver
This package provides a NodeJS driver for XeonDB, allowing you to connect to a XeonDB instance and execute SQL queries from your NodeJS applications.

## Highlights
- **Lightweight and efficient**: Minimal dependencies and optimized for performance.
- **Full SQL support**: Execute a wide range of SQL commands supported by XeonDB.
- **Easy connection management**: Simple API for connecting, disconnecting, and handling connection errors.
- **Asynchronous operations**: All database interactions are asynchronous, leveraging Promises for better performance and scalability.
- **Comprehensive error handling**: Detailed error messages to help you debug issues quickly

## Commands supported
Please refer to the [documentation](https://voyrox.github.io/Xeondb/) for a complete list of supported SQL commands and their syntax. The NodeJS driver supports executing any SQL command that XeonDB recognizes.

## queryTable vs query

- QueryTable: This method is specifically designed for executing SQL statements that interact with tables. It is optimized for handling operations such as creating tables, inserting data, selecting records, updating existing entries, and deleting records. The `queryTable` method typically returns results in a structured format that is easy to work with when dealing with tabular data.
  - Example usage of `queryTable`:
    ```javascript
    const result = await client.queryTable('SELECT * FROM my_table;');
    console.log(result);
    ```

- Query: This method is a more general-purpose function that can be used for executing any SQL statement, including those that may not directly interact with tables (e.g., administrative commands, configuration changes). The `query` method may return results in a less structured format, depending on the nature of the command being executed.
  - Example usage of `query`:
    ```javascript
    const result = await client.query('PING;');
    console.log(result);
    ```

## Example usage
```javascript
const { XeondbClient } = require("xeondb-driver");

async function main() {
    const host = process.env.Xeondb_HOST || '127.0.0.1';
    const port = Number(process.env.Xeondb_PORT || 9876);
    // Optional auth (only required if the server has auth enabled)
    const username = process.env.Xeondb_USERNAME;
    const password = process.env.Xeondb_PASSWORD;
    const client = new XeondbClient({ host, port, username, password });

    const keyspace = 'testKeyspace';
    const table = 'testTable';

    try {
        console.log(`Connecting to Xeondb at ${host}:${port}...`);
        const connected = await client.connect();
        if (!connected) {
            throw new Error('Unable to establish a connection to the database.');
        }
        console.log('Successfully connected to the database.');

        await client.queryTable(`CREATE KEYSPACE IF NOT EXISTS ${keyspace};`);
        await client.selectKeyspace(keyspace);
        console.log(`Using keyspace: ${keyspace}`);

        await client.queryTable(
            `CREATE TABLE IF NOT EXISTS ${table} (id int64, value varchar, PRIMARY KEY (id));`
        );
        console.log(`Table '${table}' is ready.`);

        await client.queryTable(
            `INSERT INTO ${table} (id, value) VALUES (1, "hello"), (2, "world");`
        );
        console.log('Inserted initial records.');

        await client.queryTable(`UPDATE ${table} SET value = "HELLO" WHERE id = 1;`);
        let result = await client.queryTable(`SELECT * FROM ${table} WHERE id = 1;`);
        console.log('After update (id=1):', result);

        await client.queryTable(`UPDATE ${table} SET value = "new" WHERE id = 3;`);
        result = await client.queryTable(`SELECT * FROM ${table} WHERE id = 3;`);
        console.log('After update (id=3):', result);

        result = await client.queryTable(`SELECT * FROM ${table} WHERE id = 1;`);
        console.log('Current (id=1):', result);

        await client.queryTable(`DELETE FROM ${table} WHERE id = 1;`);
        result = await client.queryTable(`SELECT * FROM ${table} WHERE id = 1;`);
        console.log('After delete (id=1):', result);

        result = await client.queryTable(`SELECT * FROM ${table} WHERE id = 2;`);
        console.log('Current (id=2):', result);

        console.log('All operations completed successfully.');
    } catch (error) {
        console.error('An error occurred during the test execution:', error);
        process.exitCode = 1;
    } finally {
        client.close();
        console.log('Database connection closed.');
    }
}

main();
``` 

## Authentication

Server-side (config snippet):

```yml
auth:
  username: admin
  password: change-me
```

Client-side:

```javascript
const client = new XeondbClient({ host, port, username: 'admin', password: 'change-me' });
await client.connect();

// or, if you want to auth explicitly
await client.auth('admin', 'change-me');
```


## Devnotes

### Run before publishing
```bash
npm version patch
```

### Publish changes to npm
```bash
npm publish --access public
```
