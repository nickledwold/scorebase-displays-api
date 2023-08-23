const express = require("express");
const sqlite3 = require('sqlite3').verbose();
const cors = require("cors");
const NodeCache = require("node-cache");
const app = express();
app.use(cors());

const args = process.argv.slice(2); // Skip the first two arguments which are node and script file paths

if (args.length < 2) {
  console.error('Usage: node api.js <port> <databasePath>');
  process.exit(1);
}

const port = parseInt(args[0]);
const databasePath = args[1];

const db = new sqlite3.Database(databasePath, sqlite3.OPEN_READONLY, err => {
  if (err) {
    console.error('Error opening database:', err.message);
  } else {
    console.log('Connected to the SQLite database');
  }
});

const cache = new NodeCache({ stdTTL: 60 * 5 });

app.get("/api/panelStatus", (req, res) => {
  const panelNumber = req.query.panelNumber;
  const query = 'SELECT * FROM PanelStatus WHERE PanelNo = ?';

  db.all(query, [panelNumber], (err, rows) => {
    if (err) {
      console.error('Error executing query:', err.message);
      res.status(500).json({ error: 'Internal Server Error' });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/latestScore", (req, res) => {
  const panelNumber = req.query.panelNumber;
  const query = 'SELECT * FROM DisplayScreen WHERE PanelNo = ? ORDER BY LastUpdatedTimestamp DESC LIMIT 1';

  db.all(query, [panelNumber], (err, rows) => {
    if (err) {
      console.error('Error executing query:', err.message);
      res.status(500).json({ error: 'Internal Server Error' });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/categoryRoundExercises", (req, res) => {
  const categoryId = req.query.catId;
  const exerciseNumber = req.query.exerciseNumber;
  const cacheKey = `categoryRoundExercises_${categoryId}_${exerciseNumber}`;

  const cachedData = cache.get(cacheKey);
  if (cachedData) {
    console.log("Returning cached data");
    res.json(cachedData);
    return;
  }

  const query = 'SELECT * FROM CategoryRoundExercises where CategoryId = "'+categoryId+'" and ExerciseNumber = '+exerciseNumber;

  db.all(query, [], (err, rows) => {
    if (err) {
      console.error('Error executing query:', err.message);
      res.status(500).json({ error: 'Internal Server Error' });
    } else {
      cache.set(cacheKey, rows);
      res.json(rows);
    }
  });
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
