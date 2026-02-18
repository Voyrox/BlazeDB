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
        await client.selectKeyspace('test_keyspace');
        console.log('Keyspace selected successfully');

        async function query(cmd) {
            return new Promise((resolve, reject) => {
                let result = '';
                client.socket.once('data', (data) => {
                    result = data.toString();
                    resolve(result);
                });
                client.socket.write(cmd + '\n', (err) => {
                    if (err) reject(err);
                });
            });
        }

        const createTable = 'CREATE TABLE test_table (id INT PRIMARY KEY, value TEXT)';
        let res = await query(createTable);
        console.log('Create table result:', res);

        const insertData = "INSERT INTO test_table (id, value) VALUES (1, 'hello'), (2, 'world')";
        res = await query(insertData);
        console.log('Insert data result:', res);

        const selectData = 'SELECT * FROM test_table';
        res = await query(selectData);
        console.log('Select data result:', res);

        const deleteData = 'DELETE FROM test_table WHERE id = 1';
        res = await query(deleteData);
        console.log('Delete data result:', res);

        res = await query(selectData);
        console.log('Select after delete:', res);

    } catch (err) {
        console.error('Test failed:', err);
    } finally {
        client.close();
        console.log('Connection closed.');
    }
}

runTest();
