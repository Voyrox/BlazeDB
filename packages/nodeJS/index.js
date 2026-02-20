const net = require('net');

class XeondbClient {
	/**
	 * @param {Object} options
	 * @param {string} options.host - The database server IP or hostname
	 * @param {number} options.port - The database server port
	 * @param {string} [options.username] - Optional username (enables auth)
	 * @param {string} [options.password] - Optional password (enables auth)
	 */
	constructor({ host = '127.0.0.1', port = 9876, username = undefined, password = undefined } = {}) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.socket = null;
		this.keyspace = null;
		this.connected = false;
		this._buffer = '';
		this._pending = [];
	}

	static _sqlQuoted(v) {
		const s = String(v);
		return (
			'"' +
			s
				.replace(/\\/g, '\\\\')
				.replace(/\"/g, '\\"')
				.replace(/\n/g, '\\n')
				.replace(/\r/g, '\\r')
				.replace(/\t/g, '\\t') +
			'"'
		);
	}

	/**
	 * Connect to the Xeondb server
	 * @returns {Promise<boolean>} true if connected, false otherwise
	 */
	connect() {
		return new Promise((resolve) => {
			this.socket = net.createConnection({ host: this.host, port: this.port }, async () => {
				this.connected = true;
				this._installSocketHandlers();
				try {
					const hasUser = this.username !== undefined && this.username !== null && String(this.username) !== '';
					const hasPass = this.password !== undefined && this.password !== null && String(this.password) !== '';
					if (hasUser || hasPass) {
						const res = await this.auth(this.username || '', this.password || '');
						if (!res || res.ok !== true) {
							throw new Error(res && res.error ? res.error : 'Authentication failed');
						}
					}
					resolve(true);
				} catch {
					this.close();
					resolve(false);
				}
			});
			this.socket.on('error', () => {
				this.connected = false;
				resolve(false);
			});
		});
	}

	async auth(username, password) {
		const u = XeondbClient._sqlQuoted(username);
		const p = XeondbClient._sqlQuoted(password);
		return await this.query(`AUTH ${u} ${p};`);
	}

	_installSocketHandlers() {
		if (!this.socket) return;
		this._buffer = '';

		this.socket.on('data', (data) => {
			this._buffer += data.toString('utf8');
			while (true) {
				const nl = this._buffer.indexOf('\n');
				if (nl === -1) return;
				const line = this._buffer.slice(0, nl).trim();
				this._buffer = this._buffer.slice(nl + 1);
				const pending = this._pending.shift();
				if (!pending) continue;
				pending.resolve(line);
			}
		});

		this.socket.on('error', (err) => {
			this.connected = false;
			this._rejectAllPending(err);
		});

		this.socket.on('close', () => {
			this.connected = false;
			this._rejectAllPending(new Error('Connection closed'));
		});
	}

	_rejectAllPending(err) {
		while (this._pending.length) {
			const p = this._pending.shift();
			p.reject(err);
		}
	}

	/**
	 * Select a keyspace to use
	 * @param {string} keyspace
	 * @returns {Promise<void>}
	 */
	async selectKeyspace(keyspace) {
		if (!this.connected) throw new Error('Not connected');
		if (!XeondbClient._isIdentifier(keyspace)) throw new Error('Invalid keyspace');
		const res = await this.query(`USE ${keyspace};`);
		if (!res || res.ok !== true) throw new Error(res && res.error ? res.error : 'Failed to select keyspace');
		this.keyspace = keyspace;
	}

	static _isIdentifier(s) {
		return typeof s === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
	}

	queryRaw(cmd) {
		return new Promise((resolve, reject) => {
			if (!this.connected || !this.socket) return reject(new Error('Not connected'));
			const sql = String(cmd || '').trim();
			if (!sql) return reject(new Error('Empty query'));
			this._pending.push({ resolve, reject });
			this.socket.write(sql + '\n', (err) => {
				if (err) {
					this._pending.pop();
					reject(err);
				}
			});
		});
	}

	async query(cmd) {
		const raw = await this.queryRaw(cmd);
		try {
			return JSON.parse(raw);
		} catch {
			return { ok: false, error: 'Bad JSON', raw };
		}
	}

	static _cellString(v) {
		if (v === null || v === undefined) return '';
		if (typeof v === 'string') return v;
		if (typeof v === 'number' || typeof v === 'boolean') return String(v);
		try {
			return JSON.stringify(v);
		} catch {
			return String(v);
		}
	}

	static _truncateCell(s, maxLen = 60) {
		if (s.length <= maxLen) return s;
		return s.slice(0, maxLen - 3) + '...';
	}

	static _printTable(headers, rows) {
		const widths = headers.map((h) => h.length);
		for (const r of rows) {
			for (let i = 0; i < headers.length; i++) {
				widths[i] = Math.max(widths[i], r[i].length);
			}
		}
		const line = '+' + widths.map((w) => '-'.repeat(w + 2)).join('+') + '+';
		const fmtRow = (cols) =>
			'|' + cols.map((c, i) => ` ${c}${' '.repeat(widths[i] - c.length)} `).join('|') + '|';
		console.log(line);
		console.log(fmtRow(headers));
		console.log(line);
		for (const r of rows) console.log(fmtRow(r));
		console.log(line);
	}

	async queryTable(cmd) {
		const res = await this.query(cmd);
		if (res && typeof res === 'object' && Array.isArray(res.rows)) {
			if (res.rows.length === 0) {
				console.log('(no rows)');
				return res;
			}
			const headers = Object.keys(res.rows[0] || {});
			const rows = res.rows.map((row) =>
				headers.map((h) => XeondbClient._truncateCell(XeondbClient._cellString(row ? row[h] : undefined)))
			);
			XeondbClient._printTable(headers, rows);
			return res;
		}
		if (res && typeof res === 'object' && Object.prototype.hasOwnProperty.call(res, 'found')) {
			if (!res.found) {
				console.log('(no rows)');
				return res;
			}
			const row = res.row || {};
			const headers = Object.keys(row);
			const values = headers.map((h) => XeondbClient._truncateCell(XeondbClient._cellString(row[h])));
			XeondbClient._printTable(headers, [values]);
			return res;
		}

		const entries = res && typeof res === 'object' ? Object.entries(res) : [['result', res]];
		const rows = entries.map(([k, v]) => [k, XeondbClient._truncateCell(XeondbClient._cellString(v))]);
		XeondbClient._printTable(['key', 'value'], rows);
		return res;
	}

	async execTable(cmd) {
		const res = await this.queryTable(cmd);
		return !!(res && typeof res === 'object' && res.ok === true);
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

module.exports = { XeondbClient };
