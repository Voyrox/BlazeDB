const router = require('express').Router();
const jwt = require('jsonwebtoken');
const { registerValidation, loginValidation } = require('./validations.js');
const { createUser, verifyUser } = require('../database/table/user.js');

router.post('/register', async (req, res) => {
    const { error } = registerValidation(req.body);
    if (error) return res.status(400).send(error.details[0].message);
    
    createUser(req.db, req.body);
    res.status(200).send("User registered successfully");
});

router.post('/login', async (req, res) => {
    const { error } = loginValidation(req.body);
    if (error) return res.status(400).send(error.details[0].message);
    
    verifyUser(req.db, req.body.email, req.body.password, (err, user) => {
        if (err) {
            return res.status(400).send(err.message);
        }
        const token = jwt.sign({ id: user.id }, process.env.JWT_SECRET, { expiresIn: '1h' });
        res.header('auth-token', token).send(token);
    });
});

module.exports = router;