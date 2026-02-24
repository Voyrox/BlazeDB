const router = require('express').Router();
const jwt = require('jsonwebtoken');
const { createUser, getUserByEmail, verifyUser } = require('../database/table/user.js');

const { isEmail, cleanEmail, issueAuthCookie, getReqDb } = require('../lib/shared');

router.post('/register', async (req, res) => {
    const db = getReqDb(req);
    if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

    const email = cleanEmail(req.body.email);
    const password = String(req.body.password || '');
    const firstName = String(req.body.firstName || '').trim();
    const lastName = String(req.body.lastName || '').trim();
    const companyName = String(req.body.companyName || '').trim();
    const marketingOptIn = !!req.body.marketingOptIn;

    if (!isEmail(email)) return res.status(400).json({ ok: false, error: 'Invalid email' });
    if (password.length < 8) return res.status(400).json({ ok: false, error: 'Password must be at least 8 characters' });
    if (!firstName) return res.status(400).json({ ok: false, error: 'First name is required' });
    if (!lastName) return res.status(400).json({ ok: false, error: 'Last name is required' });
    if (!companyName) return res.status(400).json({ ok: false, error: 'Company name is required' });

    try {
        const existing = await getUserByEmail(db, email);
        if (existing) return res.status(409).json({ ok: false, error: 'User already exists' });

        await createUser(db, { email, password, firstName, lastName, companyName, marketingOptIn });

        const token = jwt.sign({ email }, process.env.JWT_SECRET, { expiresIn: '7d' });
        issueAuthCookie(res, token);
        return res.status(200).json({ ok: true, token });
    } catch (err) {
        return res.status(500).json({ ok: false, error: err && err.message ? err.message : 'Registration failed' });
    }
});

router.post('/login', async (req, res) => {
    const db = getReqDb(req);
    if (!db) return res.status(500).json({ ok: false, error: 'Database not ready' });

    const email = cleanEmail(req.body.email);
    const password = String(req.body.password || '');
    if (!isEmail(email)) return res.status(400).json({ ok: false, error: 'Invalid email' });
    if (!password) return res.status(400).json({ ok: false, error: 'Password is required' });

    try {
        const user = await verifyUser(db, email, password);
        const token = jwt.sign({ email: user.email || email, name: user.first_name || null, company: user.company || null }, process.env.JWT_SECRET, { expiresIn: '7d' });
        issueAuthCookie(res, token);
        return res.status(200).json({ ok: true, token });
    } catch (err) {
        return res.status(400).json({ ok: false, error: err && err.message ? err.message : 'Login failed' });
    }
});

module.exports = router;
