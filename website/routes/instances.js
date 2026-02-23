const router = require('express').Router();
const { XeondbClient } = require('xeondb-driver');
const { createInstance, getInstancesByUser, getInstanceById } = require('../database/table/instances.js');

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
    return res.status(200).json({ ok: true, instance });
  } catch (err) {
    return res.status(400).json({ ok: false, error: err && err.message ? err.message : 'Failed to create instance' });
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

module.exports = router;
