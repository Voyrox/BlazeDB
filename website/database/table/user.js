const bcrypt = require('bcryptjs');

function createTables(db) {
  const query = `
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE NOT NULL,
      email TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `;
  db.query(query, (err) => {
    if (err) {
      console.error("Error creating users table:", err.message);
    } else {
      console.log("Users table created or already exists");
    }
  });
}

function createUser(db, data) {
    const { username, email, password } = data;
    const hashedPassword = bcrypt.hashSync(password, 10);
    const query = `INSERT INTO users (username, email, password) VALUES (?, ?, ?)`;
    db.query(query, [username, email, hashedPassword], (err) => {
        if (err) {
            console.error("Error creating user:", err.message);
        } else {
            console.log("User created successfully");
        }
    });
}

function verifyUser(db, email, password, callback) {
    const query = `SELECT * FROM users WHERE email = ${email}`;
    db.query(query, (err, results) => {
        if (err) {
            console.error("Error fetching user:", err.message);
            return callback(err);
        }
        if (results.length === 0) {
            return callback(new Error("User not found"));
        }
        const user = results[0];
        const isPasswordValid = bcrypt.compareSync(password, user.password);
        if (!isPasswordValid) {
            return callback(new Error("Invalid password"));
        }
        callback(null, user);
    });
}

module.exports = { createTables, createUser, verifyUser };