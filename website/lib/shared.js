function cleanSQL(v) {
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

function cleanEmail(s) {
  return String(s || '').trim().toLowerCase();
}

function isEmail(s) {
  return typeof s === 'string' && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s.trim());
}

function isIdentifier(s) {
  return typeof s === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(s);
}

function getReqDb(req) {
  return req && req.app && req.app.locals ? req.app.locals.db : null;
}

function isCookieSecure() {
  return process.env.COOKIE_SECURE === 'true' || process.env.NODE_ENV === 'production';
}

function issueAuthCookie(res, token) {
  const secure = isCookieSecure();
  res.cookie('auth-token', token, {
    httpOnly: true,
    sameSite: 'lax',
    secure,
    maxAge: 7 * 24 * 60 * 60 * 1000,
    path: '/'
  });
  res.set('auth-token', token);
}

function clearAuthCookie(res) {
  const secure = isCookieSecure();
  res.cookie('auth-token', '', {
    httpOnly: true,
    sameSite: 'lax',
    secure,
    maxAge: 0,
    path: '/'
  });
}

module.exports = {
  cleanSQL,
  cleanEmail,
  isEmail,
  isIdentifier,
  getReqDb,
  isCookieSecure,
  issueAuthCookie,
  clearAuthCookie
};
