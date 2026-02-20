const { XeondbClient } = require('./nodeJS');

async function main() {
    const host = process.env.Xeondb_HOST || '127.0.0.1';
    const port = Number(process.env.Xeondb_PORT || 9876);
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
