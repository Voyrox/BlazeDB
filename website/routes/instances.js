const router = require('express').Router();
const crypto = require('crypto');
const { XeondbClient } = require('xeondb-driver');
const { createInstance, getInstancesByUser, getInstanceById } = require('../database/table/instances.js');
const { cleanSQL, cleanEmail, isIdentifier, getReqDb } = require('../lib/shared');
const {
  addWhitelistEntry,
  listWhitelistByInstance,
  removeWhitelistEntry,
  deleteWhitelistEntryById,
  cleanIp,
  cleanCidr
} = require('../database/table/whitelist');
const {
  listBackupsByInstance,
  createBackupRow,
  deleteBackupRow
} = require('../database/table/backups');

async function loadOwnedInstance(req, id) {
  const db = getReqDb(req);
  if (!db) throw new Error('Database not ready');

  const email = cleanEmail(req.user && req.user.email);
  if (!email) {
    const err = new Error('Not authenticated');
    err.status = 401;
    throw err;
  }

  const instance = await getInstanceById(db, id);
  if (!instance) {
    const err = new Error('Instance not found');
    err.status = 404;
    throw err;
  }
  if (cleanEmail(instance.user_email) !== email) {
    const err = new Error('Forbidden');
    err.status = 403;
    throw err;
  }
  return instance;
}

function instancePool(envValue) {
  if (!envValue) return [];
  try {
    let parsed = null;
    try {
      parsed = JSON.parse(envValue);
    } catch {
      const relaxed = String(envValue)
        .trim()
        .replace(/([{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:/g, '$1"$2":');
      parsed = JSON.parse(relaxed);
    }
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((v) => ({
        host: typeof v.host === 'string' ? v.host : null,
        port: typeof v.port === 'number' ? v.port : Number(v.port),
        username: typeof v.username === 'string' ? v.username : null,
        password: typeof v.password === 'string' ? v.password : null
      }))
      .filter((v) => !!v.host && Number.isFinite(v.port) && v.port > 0);
  } catch {
    return [];
  }
}

function findRootCredsForInstance(instance) {
  const host = instance && instance.host ? String(instance.host) : '';
  const port = instance && instance.port ? Number(instance.port) : NaN;
  if (!host || !Number.isFinite(port)) return null;

  const plan = String(instance && instance.plan ? instance.plan : '').trim().toLowerCase() === 'pro' ? 'pro' : 'free';

  const freePool = instancePool(process.env.FREE_INSTANCES);
  const paidPool = instancePool(process.env.PAID_INSTANCES);

  const pool = plan === 'pro' ? paidPool : freePool;
  const match = pool.find((p) => String(p.host) === host && Number(p.port) === port);
  if (match) return match;

  const match2 = [...freePool, ...paidPool].find((p) => String(p.host) === host && Number(p.port) === port);
  return match2 || null;
}

async function connectRootClientForInstance(instance) {
  const creds = findRootCredsForInstance(instance);
  if (!creds || !creds.username || !creds.password) {
    throw new Error('Missing root credentials for this instance host/port');
  }

  const client = new XeondbClient({
    host: String(creds.host),
    port: Number(creds.port),
    username: String(creds.username),
    password: String(creds.password)
  });
  const connected = await client.connect();
  if (!connected) {
    try {
      client.close();
    } catch {
      // ignore
    }
    throw new Error('Unable to connect to instance as root');
  }
  return client;
}

router.get('/', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const email = cleanEmail(req.user && req.user.email);
  if (!email) return res.status(401).json({ ok: false, error: 'Not authenticated' });

  try {
    const instances = await getInstancesByUser(db, email);
    return res.json({ ok: true, instances });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err && err.message ? err.message : 'Failed to list instances' });
  }
});

router.post('/', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const email = cleanEmail(req.user && req.user.email);
  if (!email) return res.status(401).json({ ok: false, error: 'Not authenticated' });

  const plan = String(req.body && req.body.plan ? req.body.plan : 'free').trim().toLowerCase() === 'pro' ? 'pro' : 'free';

  try {
    const instance = await createInstance(db, { userEmail: email, plan });

    try {
      await addWhitelistEntry(db, { instanceId: instance.id, cidr: '0.0.0.0/0', kind: 'default' });
    } catch {
      // ignore
    }

    try {
      const ip = cleanIp(req.ip);
      if (ip) {
        const cidr = ip.includes(':') ? `${ip}/128` : `${ip}/32`;
        await addWhitelistEntry(db, { instanceId: instance.id, cidr, kind: 'auto' });
      }
    } catch {
      // ignore
    }

    return res.status(200).json({ ok: true, instance });
  } catch (err) {
    return res.status(400).json({ ok: false, error: err && err.message ? err.message : 'Failed to create instance' });
  }
});

router.delete('/:id', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const id = String(req.params.id || '');
  try {
    await loadOwnedInstance(req, id);

    const data = await listWhitelistByInstance(db, id);
    for (const wl of data) {
      try {
        await deleteWhitelistEntryById(db, wl.id);
      } catch {
        // ignore
      }
    }

    const backups = await listBackupsByInstance(db, id);
    for (const b of backups) {
      try {
        await deleteBackupRow(db, { instanceId: id, id: b.id });
      } catch {
        // ignore
      }
    }

    {
      const q = `DELETE FROM instances WHERE id=${cleanSQL(id)};`;
      const del = await db.query(q);
      if (!del || del.ok !== true) {
        throw new Error((del && del.error) || 'Failed to delete instance');
      }
    }

    return res.json({ ok: true });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.get('/:id/credentials', async (req, res) => {
  try {
    const instance = await loadOwnedInstance(req, String(req.params.id || ''));
    return res.json({
      ok: true,
      credentials: {
        host: instance.host,
        port: instance.port,
        keyspace: instance.keyspace,
        username: instance.db_username,
        password: instance.db_password
      }
    });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.get('/:id/db-users', async (req, res) => {
  const id = String(req.params.id || '');
  let client = null;
  try {
    const instance = await loadOwnedInstance(req, id);
    const keyspace = instance && instance.keyspace ? String(instance.keyspace).trim() : '';
    if (!keyspace || !isIdentifier(keyspace)) {
      return res.status(400).json({ ok: false, error: 'Invalid instance keyspace' });
    }

    client = await connectRootClientForInstance(instance);

    const ownerRes = await client.query(
      `SELECT * FROM SYSTEM.KEYSPACE_OWNERS WHERE keyspace=${cleanSQL(keyspace)};`
    );
    if (!ownerRes || ownerRes.ok !== true) {
      throw new Error((ownerRes && ownerRes.error) || 'Failed to load keyspace owner');
    }
    const ownerRow = Object.prototype.hasOwnProperty.call(ownerRes, 'found')
      ? (ownerRes.found ? (ownerRes.row || null) : null)
      : (ownerRes.row || null);
    const ownerUsername = ownerRow && ownerRow.owner_username ? String(ownerRow.owner_username) : '';

    const grantsRes = await client.query('SELECT * FROM SYSTEM.KEYSPACE_GRANTS ORDER BY keyspace_username ASC;');
    if (!grantsRes || grantsRes.ok !== true) {
      throw new Error((grantsRes && grantsRes.error) || 'Failed to load grants');
    }
    const grantRows = Array.isArray(grantsRes.rows) ? grantsRes.rows : [];

    const accessByUser = new Map();
    if (ownerUsername) {
      accessByUser.set(ownerUsername, 'owner');
    }
    for (const r of grantRows) {
      const ksu = r && (r.keyspace_username || r.keyspaceUsername) ? String(r.keyspace_username || r.keyspaceUsername) : '';
      if (!ksu) continue;
      const prefix = keyspace + '#';
      if (!ksu.startsWith(prefix)) continue;
      const username = ksu.slice(prefix.length);
      if (!username) continue;
      if (!accessByUser.has(username)) {
        accessByUser.set(username, 'granted');
      }
    }

    const users = [];
    for (const [username, access] of accessByUser.entries()) {
      const uRes = await client.query(`SELECT * FROM SYSTEM.USERS WHERE username=${cleanSQL(username)};`);
      if (!uRes || uRes.ok !== true) {
        throw new Error((uRes && uRes.error) || 'Failed to load user');
      }
      const uRow = Object.prototype.hasOwnProperty.call(uRes, 'found') ? (uRes.found ? (uRes.row || null) : null) : (uRes.row || null);
      users.push({
        username,
        access,
        enabled: uRow && Object.prototype.hasOwnProperty.call(uRow, 'enabled') ? uRow.enabled : null,
        level: uRow && Object.prototype.hasOwnProperty.call(uRow, 'level') ? uRow.level : null,
        created_at: uRow && Object.prototype.hasOwnProperty.call(uRow, 'created_at') ? uRow.created_at : null
      });
    }

    users.sort((a, b) => {
      if (a.access === 'owner' && b.access !== 'owner') return -1;
      if (b.access === 'owner' && a.access !== 'owner') return 1;
      return String(a.username).localeCompare(String(b.username));
    });

    return res.json({ ok: true, keyspace, users });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  } finally {
    try {
      if (client) client.close();
    } catch {
      // ignore
    }
  }
});

router.post('/:id/db-users', async (req, res) => {
  const id = String(req.params.id || '');
  let client = null;
  try {
    const instance = await loadOwnedInstance(req, id);
    const keyspace = instance && instance.keyspace ? String(instance.keyspace).trim() : '';
    if (!keyspace || !isIdentifier(keyspace)) {
      return res.status(400).json({ ok: false, error: 'Invalid instance keyspace' });
    }

    const usernameRaw = req.body && typeof req.body.username === 'string' ? req.body.username : '';
    const username = String(usernameRaw || '').trim();
    if (!username || !isIdentifier(username)) {
      return res.status(400).json({ ok: false, error: 'Invalid username' });
    }

    const passwordRaw = req.body && typeof req.body.password === 'string' ? req.body.password : '';
    const password = String(passwordRaw || '').trim() || crypto.randomBytes(16).toString('hex');
    const createdAt = Date.now();

    client = await connectRootClientForInstance(instance);

    const existing = await client.query(`SELECT * FROM SYSTEM.USERS WHERE username=${cleanSQL(username)};`);
    if (!existing || existing.ok !== true) {
      throw new Error((existing && existing.error) || 'Failed to check user');
    }
    const exists = Object.prototype.hasOwnProperty.call(existing, 'found') ? !!existing.found : !!existing.row;
    if (exists) {
      return res.status(409).json({ ok: false, error: 'User already exists' });
    }

    const qUser =
      `INSERT INTO SYSTEM.USERS (username,password,level,enabled,created_at) VALUES (` +
      `${cleanSQL(username)}, ${cleanSQL(password)}, 1, true, ${createdAt});`;
    const r1 = await client.query(qUser);
    if (!r1 || r1.ok !== true) {
      throw new Error((r1 && r1.error) || 'Failed to create user');
    }

    const ksu = `${keyspace}#${username}`;
    const qGrant =
      `INSERT INTO SYSTEM.KEYSPACE_GRANTS (keyspace_username,created_at) VALUES (` +
      `${cleanSQL(ksu)}, ${createdAt});`;
    const r2 = await client.query(qGrant);
    if (!r2 || r2.ok !== true) {
      throw new Error((r2 && r2.error) || 'Failed to grant access');
    }

    return res.json({ ok: true, keyspace, user: { username, access: 'granted', enabled: true, level: 1, created_at: createdAt }, password });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  } finally {
    try {
      if (client) client.close();
    } catch {
      // ignore
    }
  }
});

router.delete('/:id/db-users/:username', async (req, res) => {
  const id = String(req.params.id || '');
  const username = String(req.params.username || '').trim();
  let client = null;
  try {
    const instance = await loadOwnedInstance(req, id);
    const keyspace = instance && instance.keyspace ? String(instance.keyspace).trim() : '';
    if (!keyspace || !isIdentifier(keyspace)) {
      return res.status(400).json({ ok: false, error: 'Invalid instance keyspace' });
    }
    if (!username || !isIdentifier(username)) {
      return res.status(400).json({ ok: false, error: 'Invalid username' });
    }

    client = await connectRootClientForInstance(instance);

    const ownerRes = await client.query(
      `SELECT * FROM SYSTEM.KEYSPACE_OWNERS WHERE keyspace=${cleanSQL(keyspace)};`
    );
    if (!ownerRes || ownerRes.ok !== true) {
      throw new Error((ownerRes && ownerRes.error) || 'Failed to load keyspace owner');
    }
    const ownerRow = Object.prototype.hasOwnProperty.call(ownerRes, 'found')
      ? (ownerRes.found ? (ownerRes.row || null) : null)
      : (ownerRes.row || null);
    const ownerUsername = ownerRow && ownerRow.owner_username ? String(ownerRow.owner_username) : '';
    if (ownerUsername && ownerUsername === username) {
      return res.status(400).json({ ok: false, error: 'Cannot remove the keyspace owner' });
    }

    const ksu = `${keyspace}#${username}`;
    const del = await client.query(
      `DELETE FROM SYSTEM.KEYSPACE_GRANTS WHERE keyspace_username=${cleanSQL(ksu)};`
    );
    if (!del || del.ok !== true) {
      throw new Error((del && del.error) || 'Failed to revoke access');
    }
    return res.json({ ok: true });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  } finally {
    try {
      if (client) client.close();
    } catch {
      // ignore
    }
  }
});

router.post('/:id/query', async (req, res) => {
  const id = String(req.params.id || '');
  const keyspaceRaw = req.body && typeof req.body.keyspace === 'string' ? req.body.keyspace : '';
  const keyspace = String(keyspaceRaw || '').trim();
  const queryRaw = req.body && typeof req.body.query === 'string' ? req.body.query : '';
  let query = String(queryRaw || '').trim();

  if (!query) return res.status(400).json({ ok: false, error: 'Query is required' });
  if (!query.endsWith(';')) query += ';';
  if (keyspace && !isIdentifier(keyspace)) return res.status(400).json({ ok: false, error: 'Invalid keyspace' });

  let client = null;
  try {
    const instance = await loadOwnedInstance(req, id);
    client = new XeondbClient({
      host: instance.host,
      port: Number(instance.port),
      username: instance.db_username,
      password: instance.db_password
    });

    const connected = await client.connect();
    if (!connected) return res.status(502).json({ ok: false, error: 'Unable to connect to instance' });

    const instanceKeyspace = instance && typeof instance.keyspace === 'string' ? String(instance.keyspace || '').trim() : '';
    const effectiveKeyspace = instanceKeyspace || keyspace;
    if (effectiveKeyspace) {
      if (!isIdentifier(effectiveKeyspace)) return res.status(400).json({ ok: false, error: 'Invalid keyspace' });
      await client.selectKeyspace(effectiveKeyspace);
    }

    const result = await client.query(query);
    if (!result || result.ok !== true) {
      return res.status(400).json({ ok: false, error: (result && result.error) || 'Query failed' });
    }
    return res.json({ ok: true, result });
  } catch (err) {
    return res.status(400).json({ ok: false, error: err && err.message ? err.message : 'Query failed' });
  } finally {
    try {
      if (client) client.close();
    } catch {
      // ignore
    }
  }
});

router.get('/:id/metrics', async (req, res) => {
  const id = String(req.params.id || '');
  let client = null;
  try {
    const instance = await loadOwnedInstance(req, id);
    const keyspace = instance && typeof instance.keyspace === 'string' ? String(instance.keyspace || '').trim() : '';
    if (!keyspace || !isIdentifier(keyspace)) {
      return res.status(400).json({ ok: false, error: 'Invalid keyspace' });
    }

    client = new XeondbClient({
      host: instance.host,
      port: Number(instance.port),
      username: instance.db_username,
      password: instance.db_password
    });

    const connected = await client.connect();
    if (!connected) return res.status(502).json({ ok: false, error: 'Unable to connect to instance' });

    const q = `SHOW METRICS IN ${keyspace};`;
    const result = await client.query(q);
    if (!result || result.ok !== true) {
      return res.status(400).json({ ok: false, error: (result && result.error) || 'Failed to load metrics' });
    }
    return res.json({ ok: true, metrics: result });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  } finally {
    try {
      if (client) client.close();
    } catch {
      // ignore
    }
  }
});

router.get('/:id/whitelist', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const id = String(req.params.id || '');
  try {
    await loadOwnedInstance(req, id);

    try {
      await addWhitelistEntry(db, { instanceId: id, cidr: '0.0.0.0/0', kind: 'default' });
    } catch {
      // ignore
    }

    const whitelist = await listWhitelistByInstance(db, id);
    const ip = cleanIp(req.ip);
    const clientCidr = ip ? (ip.includes(':') ? `${ip}/128` : `${ip}/32`) : '';
    return res.json({ ok: true, whitelist, clientCidr });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.post('/:id/whitelist', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const id = String(req.params.id || '');
  const cidrInput = req.body && typeof req.body.cidr === 'string' ? req.body.cidr : '';

  try {
    await loadOwnedInstance(req, id);
    const cidr = cleanCidr(cidrInput);
    const entry = await addWhitelistEntry(db, { instanceId: id, cidr, kind: 'custom' });
    return res.json({ ok: true, entry });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.delete('/:id/whitelist/:wlId', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const id = String(req.params.id || '');
  const wlId = String(req.params.wlId || '');
  try {
    await loadOwnedInstance(req, id);
    await removeWhitelistEntry(db, { instanceId: id, id: wlId });
    return res.json({ ok: true });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.get('/:id/backups', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const id = String(req.params.id || '');
  try {
    await loadOwnedInstance(req, id);
    const backups = await listBackupsByInstance(db, id);
    return res.json({ ok: true, backups });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.post('/:id/backups', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const id = String(req.params.id || '');
  const dir = req.body && typeof req.body.dir === 'string' ? req.body.dir : '';
  try {
    await loadOwnedInstance(req, id);
    const backup = await createBackupRow(db, { instanceId: id, dir });
    return res.json({ ok: true, backup });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.delete('/:id/backups/:backupId', async (req, res) => {
  const db = getReqDb(req);
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const id = String(req.params.id || '');
  const backupId = String(req.params.backupId || '');
  try {
    await loadOwnedInstance(req, id);
    await deleteBackupRow(db, { instanceId: id, id: backupId });
    return res.json({ ok: true });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

module.exports = router;
