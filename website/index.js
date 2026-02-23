const express = require("express");
const app = express();
const cors = require("cors");
require("dotenv").config();
const path = require("path");
const { connectToDb } = require("./database/db");
const authRoute = require("./routes");

//connectToDb();

app.use(cors());
app.use(express.json());
app.engine(".ejs", require("ejs").__express);
app.set("view engine", "ejs");
app.use(express.static(path.join(__dirname, "/public")));
app.set("views", __dirname + "/views");

app.get("/", (req, res) => {
  res.render("index", {
  });
});

app.get("/login", (req, res) => {
  res.render("login", {
  });
});

app.get("/dashboard", authRoute, (req, res) => {
  res.render("dashboard", {
  });
});

app.get("/dashboard/:id", authRoute, (req, res) => {
  res.render("manage", {
  });
});

app.listen(process.env.PORT, () => {
  console.log(`Application running at http://localhost:${process.env.PORT}/`);
});