const express = require("express");
const app = express();
const cors = require("cors");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, '.env') });
const { connectToDb } = require("./database/db");
const authApi = require("./routes/auth");
const requireAuth = require("./routes/verifyToken");
const instancesApi = require('./routes/instances');
const { getUserByEmail } = require('./database/table/user');
const { getInstancesByUser, getInstanceById } = require('./database/table/instances');

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.engine(".ejs", require("ejs").__express);
app.set("view engine", "ejs");
app.use(express.static(path.join(__dirname, "/public")));
app.set("views", __dirname + "/views");

app.use('/api/auth', authApi);
app.use('/api/instances', requireAuth, instancesApi);

app.get("/", (req, res) => {
  res.render("index", {
  });
});

app.get("/login", (req, res) => {
  res.render("login", {
  });
});

app.get('/logout', (req, res) => {
  res.cookie('auth-token', '', {
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.COOKIE_SECURE === 'true' || process.env.NODE_ENV === 'production',
    maxAge: 0,
    path: '/'
  });
  res.redirect('/login');
});

app.get("/dashboard", requireAuth, async (req, res) => {
  const db = req.app && req.app.locals ? req.app.locals.db : null;
  const email = String(req.user && req.user.email ? req.user.email : '').trim().toLowerCase();

  let name = 'ME';
  let instances = [];

  try {
    if (db && email) {
      const user = await getUserByEmail(db, email);
      const first = user && user.first_name ? String(user.first_name).trim() : '';
      const last = user && user.last_name ? String(user.last_name).trim() : '';
      const initials = (first[0] || '') + (last[0] || '');
      name = (initials || email.slice(0, 2) || 'ME').toUpperCase();
      instances = await getInstancesByUser(db, email);
    }
  } catch {
    // ignore and render defaults
  }

  res.render("dashboard", { name, instances });
});

app.get("/dashboard/:id", requireAuth, async (req, res) => {
  const db = req.app && req.app.locals ? req.app.locals.db : null;
  const email = String(req.user && req.user.email ? req.user.email : '').trim().toLowerCase();
  const id = String(req.params.id || '');

  if (!db) return res.status(500).send('Database not ready');

  try {
    const instance = await getInstanceById(db, id);
    if (!instance) return res.status(404).send('Instance not found');
    if (String(instance.user_email || '').trim().toLowerCase() !== email) {
      return res.status(403).send('Forbidden');
    }
    const safeInstance = { ...instance };
    delete safeInstance.db_password;
    res.render("manage", { name: (email.slice(0, 2) || 'ME').toUpperCase(), instance: safeInstance });
  } catch (err) {
    return res.status(500).send(err && err.message ? err.message : 'Failed to load instance');
  }
});

(async () => {
  const db = await connectToDb();
  app.locals.db = db;

  app.listen(process.env.PORT, () => {
    console.log(`Application running at http://localhost:${process.env.PORT}/`);
  });
})();
