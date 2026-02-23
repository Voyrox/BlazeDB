const { XeondbClient } = require("xeondb-driver");
const { createTables } = require("./table/user.js");

const client = new XeondbClient({ host: process.env.DB_HOST, port: process.env.DB_PORT });
const keyspace = 'testKeyspace';

async function connectToDb() {
        try {
        console.log(`Connecting to Xeondb at ${process.env.DB_HOST}:${process.env.DB_PORT}...`);
        const connected = await client.connect();
        if (!connected) {
            throw new Error('Unable to establish a connection to the database.');
        }
        console.log('Successfully connected to the database.');

        const keyspaces = await client.listKeyspaces();
        if (!keyspaces.includes(keyspace)) {
            console.log(`Keyspace "${keyspace}" not found. Creating...`);
            await client.createKeyspace(keyspace);
            console.log(`Keyspace "${keyspace}" created successfully.`);
        }
        await client.selectKeyspace(keyspace);
        createTables(client);
    } catch (error) {
        console.error('Error connecting to the database:', error.message);
        process.exit(1);
    }
}

module.exports = {
    connectToDb
};