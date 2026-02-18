const { BlazeDBClient } = require('./nodeJS');

async function runTest() {
    const host = process.env.BLAZEDB_HOST || '127.0.0.1';
    const port = Number(process.env.BLAZEDB_PORT || 9876);
    const client = new BlazeDBClient({ host, port });
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
