const { XeondbClient } = require("xeondb-driver");
const { ensureUsersTable } = require("./table/user.js");
const { ensureInstancesTable } = require("./table/instances.js");
const { ensureWhitelistTable } = require('./table/whitelist.js');
const { ensureBackupsTable } = require('./table/backups.js');

const client = new XeondbClient({ host: process.env.DB_HOST, port: process.env.DB_PORT });
const keyspace = process.env.DB_KEYSPACE;

function isIdentifier(s) {
    return typeof s === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
}

async function connectToDb() {
    try {
        if (!isIdentifier(keyspace)) {
            throw new Error(`Invalid DB_KEYSPACE: '${keyspace}'`);
        }

        console.log(`Connecting to Xeondb at ${process.env.DB_HOST}:${process.env.DB_PORT}...`);
        const connected = await client.connect();
        if (!connected) {
            throw new Error('Unable to establish a connection to the database.');
        }
        console.log('Successfully connected to the database.');

        const res = await client.query(`CREATE KEYSPACE IF NOT EXISTS ${keyspace};`);
        if (!res || res.ok !== true) {
            throw new Error(`Failed to create or access keyspace: ${(res && res.error) || 'Unknown error'}`);
        }
        await client.selectKeyspace(keyspace);
        console.log(`Using keyspace: ${keyspace}`);

        await ensureUsersTable(client);
        await ensureInstancesTable(client);
        await ensureWhitelistTable(client);
        await ensureBackupsTable(client);

        return client;
    } catch (error) {
        console.error('Error connecting to the database:', error.message);
        process.exit(1);
    }
}

module.exports = {
    connectToDb
};
