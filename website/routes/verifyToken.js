const jwt = require('jsonwebtoken');

function parseCookies(cookieHeader) {
    const cookies = {};
    if (!cookieHeader || typeof cookieHeader !== 'string') return cookies;
    const cookiePairs = cookieHeader.split(';');
    for (const pair of cookiePairs) {
        const separator = pair.indexOf('=');
        if (separator === -1) continue;
        const name = pair.slice(0, separator).trim();
        const value = pair.slice(separator + 1).trim();
        if (!name) continue;
        try {
            cookies[name] = decodeURIComponent(value);
        } catch (error) {
            cookies[name] = value;
        }
    }
    return cookies;
}

function getToken(req) {
    const cookies = parseCookies(req.headers && req.headers.cookie);
    const raw = cookies['auth-token'] || req.header('auth-token') || req.header('authorization');
    if (!raw) return null;
    const s = String(raw);
    if (s.toLowerCase().startsWith('bearer ')) return s.slice(7).trim();
    return s;
}

const authenticateUser = (req, res, next) => {
    const token = getToken(req);
    const url = String(req.originalUrl || req.url || '');
    if (!token) {
        if (url.startsWith('/api/')) return res.status(401).json({ ok: false, error: 'Access denied' });
        return res.redirect('/login');
    }

    try {
        const verified = jwt.verify(token, process.env.JWT_SECRET);
        req.user = verified;
        return next();
    } catch (error) {
        if (url.startsWith('/api/')) return res.status(401).json({ ok: false, error: 'Invalid token' });
        return res.redirect('/login');
    }
}

module.exports= authenticateUser;
