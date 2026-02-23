const crypto = require('crypto');
const net = require('net');

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

function newId() {
  return crypto.randomBytes(12).toString('hex');
}

function normalizeIp(ip) {
  let s = String(ip || '').trim();
  if (!s) return '';
  if (s.startsWith('::ffff:')) s = s.slice(7);
  return s;
}

function normalizeCidr(input) {
  const raw = String(input || '').trim();
  if (!raw) throw new Error('CIDR is required');

  const parts = raw.split('/');
  const ip = normalizeIp(parts[0]);
  const ipType = net.isIP(ip);
  if (!ipType) throw new Error('Invalid IP');

  let mask = null;
  if (parts.length === 1) {
    mask = ipType === 4 ? 32 : 128;
  } else if (parts.length === 2) {
    mask = Number(parts[1]);
    if (!Number.isInteger(mask)) throw new Error('Invalid CIDR mask');
    if (ipType === 4 && (mask < 0 || mask > 32)) throw new Error('Invalid IPv4 mask');
    if (ipType === 6 && (mask < 0 || mask > 128)) throw new Error('Invalid IPv6 mask');
  } else {
    throw new Error('Invalid CIDR');
  }

  return `${ip}/${mask}`;
}

async function ensureWhitelistTable(db) {
  const ddl =
    'CREATE TABLE IF NOT EXISTS instance_whitelist (id varchar, instance_id varchar, cidr varchar, kind varchar, created_at int64, PRIMARY KEY (id));';
  const res = await db.query(ddl);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to ensure instance_whitelist table');
  }
}

async function listWhitelistByInstance(db, instanceId) {
  const q = 'SELECT * FROM instance_whitelist ORDER BY id DESC;';
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to list whitelist');
  }
  const rows = Array.isArray(res.rows) ? res.rows : [];
  const needle = String(instanceId || '').trim();
  return rows.filter((r) => String(r.instance_id || '').trim() === needle);
}

async function addWhitelistEntry(db, data) {
  const instanceId = String(data.instanceId || '').trim();
  const cidr = normalizeCidr(data.cidr);
  const kind = String(data.kind || 'custom').trim().toLowerCase();
  const createdAt = Date.now();

  if (!instanceId) throw new Error('instanceId is required');
  if (!['default', 'auto', 'custom'].includes(kind)) throw new Error('Invalid whitelist kind');

  const existing = await listWhitelistByInstance(db, instanceId);
  const dupe = existing.find((e) => String(e.cidr || '').trim() === cidr);
  if (dupe) return dupe;

  const id = newId();
  const q = `INSERT INTO instance_whitelist (id, instance_id, cidr, kind, created_at) VALUES (${sqlQuoted(
    id
  )}, ${sqlQuoted(instanceId)}, ${sqlQuoted(cidr)}, ${sqlQuoted(kind)}, ${createdAt});`;
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to add whitelist entry');
  }

  return { id, instance_id: instanceId, cidr, kind, created_at: createdAt };
}

async function removeWhitelistEntry(db, data) {
  const instanceId = String(data.instanceId || '').trim();
  const id = String(data.id || '').trim();
  if (!instanceId) throw new Error('instanceId is required');
  if (!id) throw new Error('id is required');

  const entries = await listWhitelistByInstance(db, instanceId);
  const entry = entries.find((e) => String(e.id || '').trim() === id);
  if (!entry) throw new Error('Whitelist entry not found');
  if (String(entry.kind || '').toLowerCase() === 'default') throw new Error('Cannot remove default entry');

  const q = `DELETE FROM instance_whitelist WHERE id=${sqlQuoted(id)};`;
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to remove whitelist entry');
  }
  return true;
}

async function deleteWhitelistEntryById(db, id) {
  const wlId = String(id || '').trim();
  if (!wlId) throw new Error('id is required');
  const q = `DELETE FROM instance_whitelist WHERE id=${sqlQuoted(wlId)};`;
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to delete whitelist entry');
  }
  return true;
}

module.exports = {
  ensureWhitelistTable,
  listWhitelistByInstance,
  addWhitelistEntry,
  removeWhitelistEntry,
  deleteWhitelistEntryById,
  normalizeCidr,
  normalizeIp
};
