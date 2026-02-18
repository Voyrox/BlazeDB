const { BlazeDBClient } = require('./nodeJS');

async function runTest() {
    const client = new BlazeDBClient({ host: '127.0.0.1', port: 9876 });
    try {
        const connected = await client.connect();
        if (!connected) {
            console.error('Failed to connect to BlazeDB server!');
            return;
        }
        console.log('Connected to BlazeDB server!');

        let buffer = '';
        let pending = null;

        client.socket.on('data', (data) => {
            buffer += data.toString('utf8');
            if (!pending) return;
            const nl = buffer.indexOf('\n');
            if (nl === -1) return;
            const line = buffer.slice(0, nl).trim();
            buffer = buffer.slice(nl + 1);
            const { resolve } = pending;
            pending = null;
            resolve(line);
        });

        client.socket.on('error', (err) => {
            if (!pending) return;
            const { reject } = pending;
            pending = null;
            reject(err);
        });

        async function query(cmd) {
            return new Promise((resolve, reject) => {
                if (pending) return reject(new Error('Query already in flight'));
                pending = { resolve, reject };
                client.socket.write(cmd.trimEnd() + '\n', (err) => {
                    if (err) {
                        pending = null;
                        reject(err);
                    }
                });
            });
        }

        const keyspace = 'testKeyspace';
        const table = 'testTable';

        let res = await query(`CREATE KEYSPACE IF NOT EXISTS ${keyspace};`);
        console.log('Create keyspace result:', res);

        res = await query(
            `CREATE TABLE IF NOT EXISTS ${keyspace}.${table} (id int64, value varchar, PRIMARY KEY (id));`
        );
        console.log('Create table result:', res);

        res = await query(
            `INSERT INTO ${keyspace}.${table} (id,value) VALUES (1,"hello"), (2,"world");`
        );
        console.log('Insert data result:', res);

        res = await query(`SELECT * FROM ${keyspace}.${table} WHERE id=1;`);
        console.log('Select id=1 result:', res);

        res = await query(`DELETE FROM ${keyspace}.${table} WHERE id=1;`);
        console.log('Delete id=1 result:', res);

        res = await query(`SELECT * FROM ${keyspace}.${table} WHERE id=1;`);
        console.log('Select id=1 after delete:', res);

        res = await query(`SELECT * FROM ${keyspace}.${table} WHERE id=2;`);
        console.log('Select id=2 result:', res);

    } catch (err) {
        console.error('Test failed:', err);
    } finally {
        client.close();
        console.log('Connection closed.');
    }
}

runTest();
