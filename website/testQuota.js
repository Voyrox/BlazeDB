
const { XeondbClient } = require("xeondb-driver");

// Drop the db data
// DROP TABLE xeon_free_fad01e.test_table;

const client = new XeondbClient({
	host: 'au-central.xeondb.com',
    port: 9876,
	username: 'xeon_fad01e30f7f5a0446a6c4f23',
	password: 'd408c21da42c29d71c7a72947e35face',
	keyspace: 'xeon_free_fad01e',
});

async function pushLargeData() {
	const keyspace = 'xeon_free_fad01e';
	const table = 'test_table';
	let pushedChunks = 0;
	try {
		await client.connect();
		console.log('Connected to Xeondb.');

		await client.selectKeyspace(keyspace);

		await client.query(`CREATE TABLE IF NOT EXISTS ${table} (key varchar, value varchar, PRIMARY KEY (key));`);

		try {
			const metrics = await client.query(`SHOW METRICS IN ${keyspace};`);
			if (metrics && metrics.ok === true) {
				console.log('Metrics:', metrics);
			}
		} catch {
			// ignore
		}


		const chunkSize = 512 * 1024; // 512KB
		const totalChunks = Math.ceil((600 * 1024 * 1024) / chunkSize); // 600MB
		const dataChunk = Buffer.alloc(chunkSize, 'a');

		for (let i = 0; i < totalChunks; i++) {
			const key = `test_key_${i}`;
			const base64 = dataChunk.toString('base64');
			const cmd = `INSERT INTO ${table} (key, value) VALUES ("${key}", "${base64}");`;
			const res = await client.query(cmd);
			if (!res || res.ok !== true) {
				console.error(`Error inserting chunk ${i + 1}:`, (res && res.error) ? res.error : 'Query failed');
				break;
			}
			pushedChunks++;
			console.log(`Pushed chunk ${i + 1}/${totalChunks}`);
		}
		console.log(`Done. Successfully pushed ${pushedChunks} chunk(s) (~${(pushedChunks * chunkSize) / (1024 * 1024)} MB raw).`);
		client.close();
	} catch (err) {
		console.error('Error during data push:', err);
	}
}

pushLargeData();
