const bcrypt = require('bcryptjs');

function cleanInput(v) {
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

async function ensureUsersTable(db) {
  const cmd =
    'CREATE TABLE IF NOT EXISTS users (email varchar, password_hash varchar, first_name varchar, last_name varchar, company varchar, marketing_opt_in boolean, created_at int64, PRIMARY KEY (email));';
  const res = await db.query(cmd);
  if (!res || res.ok !== true) {
    throw new Error((res && res.error) || 'Failed to ensure users table');
  }
}

async function getUserByEmail(db, email) {
  const cmd = `SELECT * FROM users WHERE email=${cleanInput(email)};`;
  const res = await db.query(cmd);
  if (!res || res.ok !== true) throw new Error((res && res.error) || 'Failed to fetch user');

  if (Object.prototype.hasOwnProperty.call(res, 'found')) return res.found ? (res.row || null) : null;
  if (Array.isArray(res.rows)) return res.rows[0] || null;
  if (res.row) return res.row;
  return null;
}

async function createUser(db, data) {
  const email = String(data.email || '').trim().toLowerCase();
  const password = String(data.password || '');
  const firstName = String(data.firstName || '').trim();
  const lastName = String(data.lastName || '').trim();
  const companyName = String(data.companyName || '').trim();
  const marketingOptIn = !!data.marketingOptIn;

  const passwordHash = bcrypt.hashSync(password, 10);
  const createdAt = Date.now();

  const cmd = `INSERT INTO users (email, password_hash, first_name, last_name, company, marketing_opt_in, created_at) VALUES (${cleanInput(
    email
  )}, ${cleanInput(passwordHash)}, ${cleanInput(firstName)}, ${cleanInput(lastName)}, ${cleanInput(
    companyName
  )}, ${marketingOptIn ? 'true' : 'false'}, ${createdAt});`;
  const res = await db.query(cmd);
  if (!res || res.ok !== true) throw new Error((res && res.error) || 'Failed to create user');

  return {
    email,
    firstName,
    lastName,
    companyName,
    marketingOptIn,
    createdAt
  };
}

async function verifyUser(db, email, password) {
  const user = await getUserByEmail(db, email);
  if (!user) throw new Error('User not found');

  const hash = user.password_hash || user.passwordHash || user.password;
  const isPasswordValid = bcrypt.compareSync(String(password || ''), String(hash || ''));
  if (!isPasswordValid) throw new Error('Invalid password');

  return user;
}

module.exports = {
  ensureUsersTable,
  getUserByEmail,
  createUser,
  verifyUser
};
