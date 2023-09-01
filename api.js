const express = require("express");
const sqlite = require("better-sqlite3");
const cors = require("cors");
const NodeCache = require("node-cache");
const app = express();
app.use(cors());

const args = process.argv.slice(2); // Skip the first two arguments which are node and script file paths

if (args.length < 2) {
  console.error("Usage: node api.js <port> <databasePath>");
  process.exit(1);
}

const port = parseInt(args[0]);
const databasePath = args[1];

const db = sqlite(databasePath, {
  fileMustExist: true,
});
db.pragma("journal_mode = WAL");

const cache = new NodeCache({ stdTTL: 60 * 5 });

const MAX_RETRY_ATTEMPTS = 3; // Maximum number of retry attempts
const RETRY_DELAY_MS = 500; // Delay between retry attempts in milliseconds

// Function to perform a database query with retries
function performDatabaseQueryWithRetry(
  query,
  params,
  callback,
  retryCount = 0
) {
  if (retryCount > MAX_RETRY_ATTEMPTS) {
    console.error(`Max retry attempts reached for query: ${query}`);
    callback(new Error("Database query failed after max retry attempts."));
    return;
  }

  try {
    const stmt = db.prepare(query);
    const rows = stmt.all(params);
    callback(null, rows);
  } catch (err) {
    if (retryCount == MAX_RETRY_ATTEMPTS) {
      console.error(`Error executing query: ${query}, Error: ${err.message}`);
      // Retry the query with a delay
      console.log(`Retry count ${retryCount}`);
    }
    setTimeout(() => {
      performDatabaseQueryWithRetry(query, params, callback, retryCount + 1);
    }, RETRY_DELAY_MS);
  }
}

app.get("/api/serverClock", (req, res) => {
  var currentDate = new Date();
  var hours = currentDate.getHours().toString().padStart(2, "0");
  var minutes = currentDate.getMinutes().toString().padStart(2, "0");
  var serverTime = hours + ":" + minutes;
  res.json({ time: serverTime });
});

app.get("/api/panelStatus", (req, res) => {
  const panelNumber = req.query.panelNumber;
  let query = "";
  let params = [];
  if (panelNumber) {
    query = "SELECT * FROM PanelStatus WHERE PanelNo = ?";
    params = [panelNumber];
  } else {
    query = "SELECT * FROM PanelStatus";
  }
  performDatabaseQueryWithRetry(query, params, (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/latestScore", (req, res) => {
  const panelNumber = req.query.panelNumber;
  const query =
    "SELECT * FROM DisplayScreen WHERE PanelNo = ? ORDER BY LastUpdatedTimestamp DESC LIMIT 1";

  performDatabaseQueryWithRetry(query, [panelNumber], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/exerciseNumbers", (req, res) => {
  const categoryId = req.query.catId;
  const query =
    "SELECT ExerciseNumber, RoundName FROM DisplayScreenRoundTotals WHERE CompetitorId IN (SELECT CompetitorId FROM(SELECT CompetitorId, Max(ExerciseNumber) Exercises FROM DisplayScreenRoundTotals WHERE CatId = ? ORDER BY Exercises DESC LIMIT 1)) ORDER BY ExerciseNumber";

  performDatabaseQueryWithRetry(query, [categoryId], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/rounds", (req, res) => {
  const categoryId = req.query.catId;
  const query = "SELECT * FROM Rounds WHERE CategoryId = ?";

  const cacheKey = `rounds_${categoryId}`;
  const cachedData = cache.get(cacheKey);
  if (cachedData) {
    res.json(cachedData);
    return;
  }

  performDatabaseQueryWithRetry(query, [categoryId], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      cache.set(cacheKey, rows);
      res.json(rows);
    }
  });
});

app.get("/api/categories", (req, res) => {
  const categoryId = req.query.catId;
  let query = "";
  let params = [];
  if (categoryId) {
    query = "SELECT * FROM Categories WHERE CatId = ?";
    params = [categoryId];
  } else {
    query = "SELECT * FROM Categories";
  }
  const cacheKey = `categories_${categoryId}`;
  const cachedData = cache.get(cacheKey);
  if (cachedData) {
    res.json(cachedData);
    return;
  }

  performDatabaseQueryWithRetry(query, params, (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      cache.set(cacheKey, rows);
      res.json(rows);
    }
  });
});

app.get("/api/competitorRanks", (req, res) => {
  const categoryId = req.query.catId;
  const compType = req.query.compType;

  let query = "";
  if (compType == 0) {
    query =
      "SELECT DISTINCT CompetitorId, FirstName1, FirstName2, Surname1, Surname2, DisplayClub, ZeroRank, DisplayZeroRank, DisplayCumulativeRank FROM DisplayScreenRoundTotals WHERE CatId = ? ORDER BY ZeroRank LIMIT 8";
  } else {
    query =
      "SELECT DISTINCT CompetitorId, FirstName1, FirstName2, Surname1, Surname2, DisplayClub, ZeroRank, DisplayZeroRank, DisplayCumulativeRank FROM DisplayScreenRoundTotals WHERE CatId = ? ORDER BY CumulativeRank LIMIT 8";
  }
  performDatabaseQueryWithRetry(query, [categoryId], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/qualifyingStartList", (req, res) => {
  const categoryId = req.query.catId;
  const query =
    "SELECT DISTINCT CompetitorId, FirstName1, FirstName2, Surname1, Surname2, DisplayClub FROM DisplayScreen WHERE CatId= ? ORDER BY Q1StartNo LIMIT 8";

  performDatabaseQueryWithRetry(query, [categoryId], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/competitorRoundTotal", (req, res) => {
  const competitorId = !isNaN(req.query.competitorId)
    ? Number(req.query.competitorId)
    : "";
  const query =
    "SELECT DISTINCT * FROM DisplayScreenRoundTotals WHERE CompetitorId = ? ORDER BY ExerciseNumber";

  performDatabaseQueryWithRetry(query, [competitorId], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/displayCategories", (req, res) => {
  const query = "SELECT * FROM Categories WHERE Categories.Display=1";
  performDatabaseQueryWithRetry(query, [], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/categoryRoundExercises", (req, res) => {
  const categoryId = req.query.catId;
  const exerciseNumber = !isNaN(req.query.exerciseNumber)
    ? Number(req.query.exerciseNumber)
    : "";
  const cacheKey = `categoryRoundExercises_${categoryId}_${exerciseNumber}`;

  const cachedData = cache.get(cacheKey);
  if (cachedData) {
    res.json(cachedData);
    return;
  }

  const query =
    "SELECT * FROM CategoryRoundExercises where CategoryId = ? and ExerciseNumber = ?";

  performDatabaseQueryWithRetry(
    query,
    [categoryId, exerciseNumber],
    (err, rows) => {
      if (err) {
        console.error("Error executing query:", err.message);
        res.status(500).json({ error: "Internal Server Error" });
      } else {
        cache.set(cacheKey, rows);
        res.json(rows);
      }
    }
  );
});

app.get("/api/liveResults", (req, res) => {
  const categoryId = req.query.catId;
  const compType = req.query.compType;

  let query = "";
  if (compType == 0) {
    query =
      "SELECT * FROM DisplayScreen WHERE CatId = ? AND (Withdrawn IS NULL OR Withdrawn != 1) ORDER BY (CASE WHEN ZeroRank IS NULL THEN 1 ELSE 0 END), ZeroRank, Q1Flight, Q1StartNo";
  } else {
    query =
      "SELECT * FROM DisplayScreen WHERE CatId = ? AND (Withdrawn IS NULL OR Withdrawn != 1) ORDER BY (CASE WHEN CumulativeRank IS NULL THEN 1 ELSE 0 END), CumulativeRank, Q1Flight, Q1StartNo";
  }
  performDatabaseQueryWithRetry(query, [categoryId], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
