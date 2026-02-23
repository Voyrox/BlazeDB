const crypto = require('crypto');

const { cleanSQL } = require('../../lib/shared');

function newId() {
  return crypto.randomBytes(12).toString('hex');
}

async function ensureBackupsTable(db) {
  const ddl =
    'CREATE TABLE IF NOT EXISTS instance_backups (id varchar, instance_id varchar, dir varchar, created_at int64, PRIMARY KEY (id));';
  const res = await db.query(ddl);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to ensure instance_backups table');
  }
}

async function listBackupsByInstance(db, instanceId) {
  const q = 'SELECT * FROM instance_backups ORDER BY id DESC;';
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to list backups');
  }
  const rows = Array.isArray(res.rows) ? res.rows : [];
  const needle = String(instanceId || '').trim();
  return rows.filter((r) => String(r.instance_id || '').trim() === needle);
}

async function createBackupRow(db, data) {
  const instanceId = String(data.instanceId || '').trim();
  if (!instanceId) throw new Error('instanceId is required');

  const id = newId();
  const createdAt = Date.now();
  const dir = String(data.dir || '').trim() || `backups/${instanceId}/${createdAt}`;

  const q = `INSERT INTO instance_backups (id, instance_id, dir, created_at) VALUES (${cleanSQL(
    id
  )}, ${cleanSQL(instanceId)}, ${cleanSQL(dir)}, ${createdAt});`;
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to create backup');
  }
  return { id, instance_id: instanceId, dir, created_at: createdAt };
}

async function deleteBackupRow(db, data) {
  const instanceId = String(data.instanceId || '').trim();
  const id = String(data.id || '').trim();
  if (!instanceId) throw new Error('instanceId is required');
  if (!id) throw new Error('id is required');

  const backups = await listBackupsByInstance(db, instanceId);
  const found = backups.find((b) => String(b.id || '').trim() === id);
  if (!found) throw new Error('Backup not found');

  const q = `DELETE FROM instance_backups WHERE id=${cleanSQL(id)};`;
  const res = await db.query(q);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to delete backup');
  }
  return true;
}

module.exports = {
  ensureBackupsTable,
  listBackupsByInstance,
  createBackupRow,
  deleteBackupRow
};
