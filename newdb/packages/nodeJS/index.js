const net = require('net');

class BlazeDBClient {
	/**
	 * @param {Object} options
	 * @param {string} options.host - The database server IP or hostname
	 * @param {number} options.port - The database server port
	 */
	constructor({ host = '127.0.0.1', port = 8080 } = {}) {
		this.host = host;
		this.port = port;
		this.socket = null;
		this.keyspace = null;
		this.connected = false;
	}

	/**
	 * Connect to the BlazeDB server
	 * @returns {Promise<boolean>} true if connected, false otherwise
	 */
	connect() {
		return new Promise((resolve) => {
			this.socket = net.createConnection({ host: this.host, port: this.port }, () => {
				this.connected = true;
				resolve(true);
			});
			this.socket.on('error', () => {
				this.connected = false;
				resolve(false);
			});
		});
	}

	/**
	 * Select a keyspace to use
	 * @param {string} keyspace
	 * @returns {Promise<void>}
	 */
	selectKeyspace(keyspace) {
		return new Promise((resolve, reject) => {
			if (!this.connected) return reject(new Error('Not connected'));
			const cmd = `USE ${keyspace}\n`;
			this.socket.write(cmd, (err) => {
				if (err) return reject(err);
				this.keyspace = keyspace;
				resolve();
			});
		});
	}

	/**
	 * Close the connection
	 */
	close() {
		if (this.socket) {
			this.socket.end();
			this.connected = false;
		}
	}
}

module.exports = { BlazeDBClient };
