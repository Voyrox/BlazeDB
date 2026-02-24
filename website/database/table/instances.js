const crypto = require('crypto');

const { XeondbClient } = require('xeondb-driver');

const { cleanSQL, isIdentifier } = require('../../lib/shared');

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

function pickFromPool(pool) {
  if (!pool || pool.length === 0) return null;
  const num = Math.floor(Math.random() * pool.length);
  return pool[num] || null;
}

function generateInstanceId() {
  return crypto.randomBytes(12).toString('hex');
}

function generateDbPassword() {
  return crypto.randomBytes(16).toString('hex');
}

function generateDbUsername(instanceId) {
  return `xeon_${String(instanceId || '').trim()}`;
}

function generateDisplayName(plan) {
  const suffix = crypto.randomBytes(3).toString('hex');
  return `${plan}-${suffix}`;
}

function generateKeyspaceName(plan, instanceId) {
  const p = String(plan || '').trim().toLowerCase() === 'pro' ? 'pro' : 'free';
  const idPart = String(instanceId || '').trim().slice(0, 6);
  const suffix = idPart || crypto.randomBytes(3).toString('hex');
  return `xeon_${p}_${suffix}`;
}

async function querySingleRow(db, q, notFoundValue = null) {
  const res = await db.query(q);
  if (!res || res.ok !== true) throw new Error((res && res.error) || 'Query failed');
  if (Object.prototype.hasOwnProperty.call(res, 'found')) return res.found ? (res.row || null) : notFoundValue;
  if (Array.isArray(res.rows)) return res.rows[0] || notFoundValue;
  return res.row || notFoundValue;
}

async function queryList(db, q) {
  const res = await db.query(q);
  if (!res || res.ok !== true) throw new Error((res && res.error) || 'Query failed');
  return Array.isArray(res.rows) ? res.rows : [];
}

async function ensureInstancesTable(db) {
  const ddlV2 =
    'CREATE TABLE IF NOT EXISTS instances (id varchar, user_email varchar, name varchar, plan varchar, host varchar, port int64, keyspace varchar, db_username varchar, db_password varchar, status varchar, created_at int64, PRIMARY KEY (id));';
  const res2 = await db.query(ddlV2);
  if (!res2 || res2.ok !== true) throw new Error((res2 && res2.error) || 'Failed to ensure instances table');
}

async function getInstanceById(db, id) {
  const q2 = `SELECT * FROM instances WHERE id=${cleanSQL(id)};`;
  return querySingleRow(db, q2, null);
}

async function getInstancesByUser(db, email) {
  const needle = String(email || '').trim().toLowerCase();

  let rows = [];
  try {
    rows = await queryList(db, `SELECT * FROM instances ORDER BY id DESC;`);
  } catch {
    rows = [];
  }

  const out = Array.isArray(rows) ? rows : [];
  out.sort((a, b) => String(b.id || '').localeCompare(String(a.id || '')));
  return out.filter((r) => String(r.user_email || '').trim().toLowerCase() === needle);
}

async function provisionInstanceOnTarget(target, keyspace, dbUsername, dbPassword, quotaBytes) {
  if (!target || !target.host || !Number.isFinite(Number(target.port))) {
    throw new Error('Invalid target instance');
  }
  if (!target.username || !target.password) {
    throw new Error('Instance pool entry missing username/password');
  }
  if (!isIdentifier(keyspace)) throw new Error('Generated keyspace is invalid');
  if (!isIdentifier(dbUsername)) throw new Error('Generated username is invalid');

  const client = new XeondbClient({
    host: target.host,
    port: Number(target.port),
    username: target.username,
    password: target.password
  });
  try {
    const connected = await client.connect();
    if (!connected) throw new Error('Unable to connect to instance');

    const createdAt = Date.now();
    const q1 = `CREATE KEYSPACE IF NOT EXISTS ${keyspace};`;
    const r1 = await client.query(q1);
    if (!r1 || r1.ok !== true) throw new Error((r1 && r1.error) || 'Failed to create keyspace');

    const qUser =
      `INSERT INTO SYSTEM.USERS (username,password,level,enabled,created_at) VALUES (` +
      `${cleanSQL(dbUsername)}, ${cleanSQL(dbPassword)}, 1, true, ${createdAt});`;
    const r2 = await client.query(qUser);
    if (!r2 || r2.ok !== true) throw new Error((r2 && r2.error) || 'Failed to create db user');

    const qOwner =
      `INSERT INTO SYSTEM.KEYSPACE_OWNERS (keyspace,owner_username,created_at) VALUES (` +
      `${cleanSQL(keyspace)}, ${cleanSQL(dbUsername)}, ${createdAt});`;
    const r3 = await client.query(qOwner);
    if (!r3 || r3.ok !== true) throw new Error((r3 && r3.error) || 'Failed to set keyspace owner');

    const qQuota =
      `INSERT INTO SYSTEM.KEYSPACE_QUOTAS (keyspace,quota_bytes,updated_at) VALUES (` +
      `${cleanSQL(keyspace)}, ${Number(quotaBytes)}, ${createdAt});`;
    const r4 = await client.query(qQuota);
    if (!r4 || r4.ok !== true) throw new Error((r4 && r4.error) || 'Failed to set keyspace quota');
  } finally {
    try {
      client.close();
    } catch {
      // ignore
    }
  }
}

async function createInstance(db, data) {
  const userEmail = String(data.userEmail || '').trim().toLowerCase();
  const plan = String(data.plan || '').trim().toLowerCase() === 'pro' ? 'pro' : 'free';

  const maxPerUser = Number(process.env.MAX_DATABASES_PER_USER || 0);
  if (maxPerUser > 0) {
    const userInstances = await getInstancesByUser(db, userEmail);
    if (userInstances.length >= maxPerUser) {
      throw new Error(`You have reached the maximum allowed databases (${maxPerUser})`);
    }
  }

  const freePool = instancePool(process.env.FREE_INSTANCES);
  const paidPool = instancePool(process.env.PAID_INSTANCES);
  const pool = plan === 'pro' ? paidPool : freePool;
  const target = pickFromPool(pool);
  if (!target) {
    throw new Error(`No ${plan} instances available`);
  }

  const id = generateInstanceId();
  const name = generateDisplayName(plan);
  const keyspace = generateKeyspaceName(plan, id);
  const dbUsername = generateDbUsername(id);
  const dbPassword = generateDbPassword();
  const createdAt = Date.now();
  const status = 'online';

  const quotaBytes = plan === 'pro' ? (Number(process.env.PAID_INSTANCE_STORAGE || 100) * 1024 * 1024 * 1024) : (Number(process.env.FREE_INSTANCE_STORAGE || 500) * 1024 * 1024);
  await provisionInstanceOnTarget(target, keyspace, dbUsername, dbPassword, quotaBytes);

  const q = `INSERT INTO instances (id, user_email, name, plan, host, port, keyspace, db_username, db_password, status, created_at) VALUES (${cleanSQL(
    id
  )}, ${cleanSQL(userEmail)}, ${cleanSQL(name)}, ${cleanSQL(plan)}, ${cleanSQL(target.host)}, ${Number(target.port)}, ${cleanSQL(
    keyspace
  )}, ${cleanSQL(dbUsername)}, ${cleanSQL(dbPassword)}, ${cleanSQL(status)}, ${createdAt});`;
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to create instance');
  }

  return {
    id,
    user_email: userEmail,
    name,
    plan,
    host: target.host,
    port: Number(target.port),
    keyspace,
    db_username: dbUsername,
    db_password: dbPassword,
    status,
    created_at: createdAt
  };
}

module.exports = {
  ensureInstancesTable,
  createInstance,
  getInstancesByUser,
  getInstanceById
};
