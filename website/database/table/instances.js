const crypto = require('crypto');

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

function parseInstancePool(envValue) {
  if (!envValue) return [];
  try {
    const parsed = JSON.parse(envValue);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((v) => ({
        host: typeof v.host === 'string' ? v.host : null,
        port: typeof v.port === 'number' ? v.port : Number(v.port)
      }))
      .filter((v) => !!v.host && Number.isFinite(v.port) && v.port > 0);
  } catch {
    return [];
  }
}

function pickFromPool(pool) {
  if (!pool || pool.length === 0) return null;
  const idx = Math.floor(Math.random() * pool.length);
  return pool[idx] || null;
}

function generateInstanceId() {
  return crypto.randomBytes(12).toString('hex');
}

function generateDbPassword() {
  return crypto.randomBytes(16).toString('hex');
}

function generateDbUsername(userEmail) {
  return `xeon-${String(userEmail || '').trim().toLowerCase()}`;
}

function generateDisplayName(plan) {
  const suffix = crypto.randomBytes(3).toString('hex');
  return `${plan}-${suffix}`;
}

async function ensureInstancesTable(db) {
  const ddl =
    'CREATE TABLE IF NOT EXISTS instances (id varchar, user_email varchar, name varchar, plan varchar, host varchar, port int64, db_username varchar, db_password varchar, status varchar, created_at int64, PRIMARY KEY (id));';
  const res = await db.query(ddl);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to ensure instances table');
  }
}

async function getInstanceById(db, id) {
  const q = `SELECT * FROM instances WHERE id=${sqlQuoted(id)};`;
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to fetch instance');
  }
  if (Object.prototype.hasOwnProperty.call(res, 'found')) {
    return res.found ? (res.row || null) : null;
  }
  if (Array.isArray(res.rows)) return res.rows[0] || null;
  return res.row || null;
}

async function getInstancesByUser(db, email) {
  const q = 'SELECT * FROM instances ORDER BY id DESC;';
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to list instances');
  }
  const rows = Array.isArray(res.rows) ? res.rows : [];
  const needle = String(email || '').trim().toLowerCase();
  return rows.filter((r) => String(r.user_email || '').trim().toLowerCase() === needle);
}

async function createInstance(db, data) {
  const userEmail = String(data.userEmail || '').trim().toLowerCase();
  const plan = String(data.plan || '').trim().toLowerCase() === 'pro' ? 'pro' : 'free';

  const freePool = parseInstancePool(process.env.FREE_INSTANCES);
  const paidPool = parseInstancePool(process.env.PAID_INSTANCES);
  const pool = plan === 'pro' ? paidPool : freePool;
  const target = pickFromPool(pool);
  if (!target) {
    throw new Error(`No ${plan} instances available`);
  }

  const id = generateInstanceId();
  const name = generateDisplayName(plan);
  const dbUsername = generateDbUsername(userEmail);
  const dbPassword = generateDbPassword();
  const createdAt = Date.now();
  const status = 'online';

  const q = `INSERT INTO instances (id, user_email, name, plan, host, port, db_username, db_password, status, created_at) VALUES (${sqlQuoted(
    id
  )}, ${sqlQuoted(userEmail)}, ${sqlQuoted(name)}, ${sqlQuoted(plan)}, ${sqlQuoted(target.host)}, ${Number(
    target.port
  )}, ${sqlQuoted(dbUsername)}, ${sqlQuoted(dbPassword)}, ${sqlQuoted(status)}, ${createdAt});`;
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
