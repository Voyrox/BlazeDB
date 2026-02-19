const { XeondbClient } = require('./nodeJS');

async function runTest() {
    const host = process.env.Xeondb_HOST || '127.0.0.1';
    const port = Number(process.env.Xeondb_PORT || 9876);
    const client = new XeondbClient({ host, port });
    try {
        const connected = await client.connect();
        if (!connected) {
            console.error('Failed to connect to the database.');
            return;
        }
        console.log('Connected to the database.');

        const keyspace = 'testKeyspace';
        const table = 'testTable';

        await client.queryTable(`CREATE KEYSPACE IF NOT EXISTS ${keyspace};`);
        await client.selectKeyspace(keyspace);

        await client.queryTable(
            `CREATE TABLE IF NOT EXISTS ${table} (id int64, value varchar, PRIMARY KEY (id));`
        );

        await client.queryTable(
            `INSERT INTO ${table} (id,value) VALUES (1,"hello"), (2,"world");`
        );

        await client.queryTable(`UPDATE ${table} SET value="HELLO" WHERE id=1;`);
        await client.queryTable(`SELECT * FROM ${table} WHERE id=1;`);

        await client.queryTable(`UPDATE ${table} SET value="new" WHERE id=3;`);
        await client.queryTable(`SELECT * FROM ${table} WHERE id=3;`);

        await client.queryTable(`SELECT * FROM ${table} WHERE id=1;`);
        await client.queryTable(`DELETE FROM ${table} WHERE id=1;`);
        await client.queryTable(`SELECT * FROM ${table} WHERE id=1;`);
        await client.queryTable(`SELECT * FROM ${table} WHERE id=2;`);

    } catch (err) {
        console.error('Test failed:', err);
    } finally {
        client.close();
        console.log('Connection closed.');
    }
}

runTest();
