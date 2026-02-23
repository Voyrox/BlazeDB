const router = require('express').Router();
const { XeondbClient } = require('xeondb-driver');
const { createInstance, getInstancesByUser, getInstanceById } = require('../database/table/instances.js');
const {
  addWhitelistEntry,
  listWhitelistByInstance,
  removeWhitelistEntry,
  deleteWhitelistEntryById,
  normalizeIp,
  normalizeCidr
} = require('../database/table/whitelist');
const {
  listBackupsByInstance,
  createBackupRow,
  deleteBackupRow
} = require('../database/table/backups');

function sqlQuoted(v) {
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

function normalizeEmail(s) {
  return String(s || '').trim().toLowerCase();
}

function isIdentifier(s) {
  return typeof s === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
}

async function loadOwnedInstance(req, id) {
  const db = req.app && req.app.locals ? req.app.locals.db : null;
  if (!db) throw new Error('Database not ready');

  const email = normalizeEmail(req.user && req.user.email);
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
  if (normalizeEmail(instance.user_email) !== email) {
    const err = new Error('Forbidden');
    err.status = 403;
    throw err;
  }
  return instance;
}

router.get('/', async (req, res) => {
  const db = req.app && req.app.locals ? req.app.locals.db : null;
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const email = normalizeEmail(req.user && req.user.email);
  if (!email) return res.status(401).json({ ok: false, error: 'Not authenticated' });

  try {
    const instances = await getInstancesByUser(db, email);
    return res.json({ ok: true, instances });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err && err.message ? err.message : 'Failed to list instances' });
  }
});

router.post('/', async (req, res) => {
  const db = req.app && req.app.locals ? req.app.locals.db : null;
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const email = normalizeEmail(req.user && req.user.email);
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
      const ip = normalizeIp(req.ip);
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
  const db = req.app && req.app.locals ? req.app.locals.db : null;
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

    const q = `DELETE FROM instances WHERE id=${sqlQuoted(id)};`;
    const del = await db.query(q);
    if (!del || del.ok !== true) {
      throw new Error((del && del.error) || 'Failed to delete instance');
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
        username: instance.db_username,
        password: instance.db_password
      }
    });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
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

    if (keyspace) {
      await client.selectKeyspace(keyspace);
    }

    const result = await client.query(query);
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

router.get('/:id/whitelist', async (req, res) => {
  const db = req.app && req.app.locals ? req.app.locals.db : null;
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
    const ip = normalizeIp(req.ip);
    const clientCidr = ip ? (ip.includes(':') ? `${ip}/128` : `${ip}/32`) : '';
    return res.json({ ok: true, whitelist, clientCidr });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.post('/:id/whitelist', async (req, res) => {
  const db = req.app && req.app.locals ? req.app.locals.db : null;
  if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

  const id = String(req.params.id || '');
  const cidrInput = req.body && typeof req.body.cidr === 'string' ? req.body.cidr : '';

  try {
    await loadOwnedInstance(req, id);
    const cidr = normalizeCidr(cidrInput);
    const entry = await addWhitelistEntry(db, { instanceId: id, cidr, kind: 'custom' });
    return res.json({ ok: true, entry });
  } catch (err) {
    return res.status(err && err.status ? err.status : 400).json({ ok: false, error: err && err.message ? err.message : 'Failed' });
  }
});

router.delete('/:id/whitelist/:wlId', async (req, res) => {
  const db = req.app && req.app.locals ? req.app.locals.db : null;
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
  const db = req.app && req.app.locals ? req.app.locals.db : null;
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
  const db = req.app && req.app.locals ? req.app.locals.db : null;
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
  const db = req.app && req.app.locals ? req.app.locals.db : null;
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
