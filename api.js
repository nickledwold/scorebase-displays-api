const express = require("express");
const { Pool } = require("pg");
const cors = require("cors");
const NodeCache = require("node-cache");
const app = express();
app.use(cors());
const fs = require("fs");
const path = require("path");

const args = process.argv.slice(2); // Skip the first two arguments which are node and script file paths

if (args.length < 2) {
  console.error("Usage: node api.js <host> <schema>");
  process.exit(1);
}

const port = 3000;
const dbport = 5432;
const host = args[0];
const user = "postgres";
const database = "scorebase";
const password = "scorebase";
const schema = args[1];

/*const pool = new Pool({
  connectionString,
});*/

const pool = new Pool({
  user: user,
  host: host, // Typically 'localhost' if running on the same machine
  database: database,
  password: password, // Omit this if not using password authentication
  port: dbport, // PostgreSQL default port
});

const cache = new NodeCache({ stdTTL: 60 * 5 });

const MAX_RETRY_ATTEMPTS = 3; // Maximum number of retry attempts
const RETRY_DELAY_MS = 500; // Delay between retry attempts in milliseconds

// Function to perform a database query with retries
async function performDatabaseQueryWithRetry(
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
    const client = await pool.connect();
    const result = await client.query(query, params);
    client.release();
    callback(null, result.rows);
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
    query = `SELECT * FROM "${schema}"."PanelStatus" WHERE "PanelNo" = $1`;
    params = [panelNumber];
  } else {
    query = `SELECT * FROM "${schema}"."PanelStatus" ORDER BY "PanelNo" ASC`;
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
  const query = `SELECT * FROM "${schema}"."DisplayScreen" WHERE "PanelNo" = $1 ORDER BY "LastUpdatedTimestamp" DESC NULLS LAST LIMIT 1`;

  performDatabaseQueryWithRetry(query, [panelNumber], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/latest", (req, res) => {
  const panelNumber = req.query.panelNumber;
  const query = `SELECT * FROM "${schema}"."DisplayScreen" WHERE "PanelNo" = $1 AND "Withdrawn" IS NOT TRUE ORDER BY "LastUpdatedTimestamp" DESC NULLS LAST LIMIT 1`;

  performDatabaseQueryWithRetry(query, [panelNumber], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      if (rows.length > 0) {
        const competitorData = rows[0];

        const categoryId = competitorData.CatId;
        const cacheKey = `categoryRoundExercises_${categoryId}`;

        let categoryRoundExerciseRows = [];

        const cachedData = cache.get(cacheKey);
        if (cachedData) {
          categoryRoundExerciseRows = cachedData;
        } else {
          const query = `SELECT * FROM "${schema}"."CategoryRoundExercises" where "CategoryId" = $1`;

          performDatabaseQueryWithRetry(query, [categoryId], (err, rows2) => {
            if (err) {
              console.error("Error executing query:", err.message);
              res.status(500).json({ error: "Internal Server Error" });
            } else {
              cache.set(cacheKey, rows2);
              categoryRoundExerciseRows = rows2;
            }
          });
        }
        const exercises = [
          { exerciseNumber: 5, propertyPrefix: "Ex5" },
          { exerciseNumber: 4, propertyPrefix: "Ex4" },
          { exerciseNumber: 3, propertyPrefix: "Ex3" },
          { exerciseNumber: 2, propertyPrefix: "Ex2" },
          { exerciseNumber: 1, propertyPrefix: "Ex1" },
        ];

        let tempLatestExercise = {};

        for (const exercise of exercises) {
          if (!exercise) continue;
          const totalProperty = `${exercise.propertyPrefix}Total`;
          if (!isValueNullOrEmpty(competitorData[totalProperty])) {
            let categoryRoundExercise = categoryRoundExerciseRows.find(
              (categoryRoundExercise) =>
                categoryRoundExercise.ExerciseNumber === exercise.exerciseNumber
            );
            tempLatestExercise = {
              Exercise: exercise.exerciseNumber,
              RoundName: categoryRoundExercise.RoundName,
              Execution: competitorData[`${exercise.propertyPrefix}E`],
              Difficulty: competitorData[`${exercise.propertyPrefix}D`],
              Bonus: competitorData[`${exercise.propertyPrefix}B`],
              HorizontalDisplacement:
                competitorData[`${exercise.propertyPrefix}HD`],
              TimeOfFlight: competitorData[`${exercise.propertyPrefix}ToF`],
              Synchronisation: competitorData[`${exercise.propertyPrefix}S`],
              Penalty: competitorData[`${exercise.propertyPrefix}Pen`],
              Total: competitorData[totalProperty],
            };
            break;
          }
        }
        competitorData.Exercise = tempLatestExercise;
        for (let i = 1; i <= 5; i++) {
          const keysToRemove = [
            `Ex${i}E`,
            `Ex${i}D`,
            `Ex${i}B`,
            `Ex${i}HD`,
            `Ex${i}ToF`,
            `Ex${i}S`,
            `Ex${i}Pen`,
            `Ex${i}Total`,
            `Ex${i}Rank`,
          ];
          for (const key of keysToRemove) {
            if (competitorData.hasOwnProperty(key)) {
              delete competitorData[key];
            }
          }
        }
        if (!competitorData.ZeroRank) {
          competitorData.Rank = "-";
        } else {
          if (competitorData.CompType === 0 && competitorData.F1Total > 0) {
            competitorData.Rank = competitorData.DisplayZeroRank;
          } else {
            competitorData.Rank = competitorData.DisplayCumulativeRank;
          }
        }
        const keysToRemove = [
          `ZeroRank`,
          `CumulativeRank`,
          `DisplayZeroRank`,
          `DisplayCumulativeRank`,
          `Club`,
          `Q1StartNo`,
          `Q1Flight`,
          `Q1Scoring`,
        ];
        for (const key of keysToRemove) {
          if (competitorData.hasOwnProperty(key)) {
            delete competitorData[key];
          }
        }
        res.json(competitorData);
      } else {
        res.json({});
      }
    }
  });
});

function isValueNullOrEmpty(value) {
  return (value == null || value == "" || value == undefined) && value != 0;
}

app.get("/api/exerciseNumbers", (req, res) => {
  const categoryId = req.query.catId;
  const query = `SELECT "ExerciseNumber", "RoundName" FROM "${schema}"."DisplayScreenRoundTotals" WHERE "CompetitorId" IN (SELECT "CompetitorId" FROM (SELECT "CompetitorId", Max("ExerciseNumber") "Exercises" FROM "${schema}"."DisplayScreenRoundTotals" WHERE "CatId" = $1  GROUP BY "CompetitorId" ORDER BY "Exercises" DESC LIMIT 1)) ORDER BY "ExerciseNumber"`;

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
  const query = `SELECT * FROM "${schema}"."Rounds" WHERE "CategoryId" = $1  ORDER BY "RoundOrder"`;

  performDatabaseQueryWithRetry(query, [categoryId], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/categories", (req, res) => {
  const categoryId = req.query.catId;
  let query = "";
  let params = [];
  if (categoryId) {
    query = `SELECT * FROM "${schema}"."Categories" WHERE "CatId" = $1`;
    params = [categoryId];
  } else {
    query = `SELECT * FROM "${schema}"."Categories" ORDER BY "No" ASC`;
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
    query = `SELECT DISTINCT "CompetitorId", "FirstName1", "FirstName2", "Surname1", "Surname2", "Nation", "DisplayClub", "ZeroRank", "CumulativeRank", "DisplayZeroRank", "DisplayCumulativeRank" FROM (SELECT * FROM "${schema}"."DisplayScreenRoundTotals" WHERE "CatId" = $1) ORDER BY "ZeroRank" LIMIT 8`;
  } else {
    query = `SELECT DISTINCT "CompetitorId", "FirstName1", "FirstName2", "Surname1", "Surname2", "Nation", "DisplayClub", "ZeroRank", "CumulativeRank", "DisplayZeroRank", "DisplayCumulativeRank" FROM (SELECT * FROM "${schema}"."DisplayScreenRoundTotals" WHERE "CatId" = $1) ORDER BY "CumulativeRank" LIMIT 8`;
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
  const query = `SELECT DISTINCT "CompetitorId", "FirstName1", "FirstName2", "Surname1", "Surname2", "Nation", "DisplayClub", "Q1Flight", "Q1StartNo" FROM (SELECT * FROM "${schema}"."DisplayScreen" WHERE "CatId" = $1 AND "Withdrawn" IS NOT TRUE) ORDER BY "Q1Flight", "Q1StartNo" LIMIT 8`;

  performDatabaseQueryWithRetry(query, [categoryId], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/roundStartList", (req, res) => {
  const categoryId = req.query.catId;
  const roundName = req.query.roundName;
  const query = `SELECT "CompetitorId" FROM "${schema}"."RoundCompetitors" rc INNER JOIN "${schema}"."Rounds" r on rc."RoundId" = r."RoundId" WHERE "CategoryId" = $1 and "RoundName" = $2 ORDER BY "FlightId", "StartNo"`;

  performDatabaseQueryWithRetry(query, [categoryId, roundName], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/roundStartListCompetitors", (req, res) => {
  const categoryId = req.query.catId;
  const roundName = req.query.roundName;
  const query = `SELECT 
        c."FirstName1", c."FirstName2", c."Surname1", c."Surname2", c."DisplayClub", f."FlightNumber", rc."StartNo"
      FROM 
        "${schema}"."Competitors" c
      INNER JOIN 
        "${schema}"."RoundCompetitors" rc ON c."CompetitorId" = rc."CompetitorId"
      INNER JOIN 
        "${schema}"."Rounds" r ON rc."RoundId" = r."RoundId" AND r."CategoryId" = $1 AND r."RoundName" = $2
      INNER JOIN 
        "${schema}"."Flights" f ON f."FlightId" = rc."FlightId"
      ORDER BY 
        f."FlightNumber", rc."StartNo"`;

  performDatabaseQueryWithRetry(query, [categoryId, roundName], (err, rows) => {
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
  const query = `SELECT DISTINCT * FROM "${schema}"."DisplayScreenRoundTotals" WHERE "CompetitorId" = $1 ORDER BY "ExerciseNumber"`;

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
  const query = `SELECT * FROM "${schema}"."Categories" WHERE "Display" = 1`;
  performDatabaseQueryWithRetry(query, [], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

function performDatabaseQueryWithRetryAsync(query, params) {
  return new Promise((resolve, reject) => {
    performDatabaseQueryWithRetry(query, params, (err, rows) => {
      if (err) {
        reject(err);
      } else {
        resolve(rows);
      }
    });
  });
}

app.get("/api/bg/latest", async (req, res) => {
  const query = `SELECT * FROM (SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY "PanelNo" ORDER BY "LastUpdatedTimestamp" DESC NULLS LAST) AS rn
    FROM "${schema}"."DisplayScreen" WHERE "LastUpdatedTimestamp" IS NOT NULL
) AS ranked
WHERE rn <= 1) latest
         INNER JOIN "${schema}"."PanelStatus" p on p."PanelNo"::int = latest."PanelNo"
ORDER BY p."PanelNo" ASC`;

  const rows = await performDatabaseQueryWithRetryAsync(query, []);

  if (rows.length > 0) {
    await Promise.all(
      rows.map(async (competitor) => {
        const categoryId = competitor.CatId;
        const cacheKey = `categoryRoundExercises_${categoryId}`;

        let categoryRoundExerciseRows = [];

        const cachedData = cache.get(cacheKey);
        if (cachedData) {
          categoryRoundExerciseRows = cachedData;
          updateCompetitor(competitor, categoryRoundExerciseRows);
        } else {
          const query = `SELECT * FROM "${schema}"."CategoryRoundExercises" where "CategoryId" = $1`;
          categoryRoundExerciseRows = await performDatabaseQueryWithRetryAsync(
            query,
            [categoryId]
          );
          cache.set(cacheKey, categoryRoundExerciseRows);
          updateCompetitor(competitor, categoryRoundExerciseRows);
        }
      })
    );
    // Sample usage with your rows
    const transformedData = transformData(rows); // rows is your original query result
    res.json(transformedData);
  } else {
    res.json({});
  }
});

function updateCompetitor(competitor, categoryRoundExerciseRows) {
  const exercises = [
    { exerciseNumber: 5, propertyPrefix: "Ex5" },
    { exerciseNumber: 4, propertyPrefix: "Ex4" },
    { exerciseNumber: 3, propertyPrefix: "Ex3" },
    { exerciseNumber: 2, propertyPrefix: "Ex2" },
    { exerciseNumber: 1, propertyPrefix: "Ex1" },
  ];

  let tempLatestExercise = {};

  for (const exercise of exercises) {
    if (!exercise) continue;
    const totalProperty = `${exercise.propertyPrefix}Total`;
    if (!isValueNullOrEmpty(competitor[totalProperty])) {
      let categoryRoundExercise = categoryRoundExerciseRows.find(
        (categoryRoundExercise) =>
          categoryRoundExercise.ExerciseNumber == exercise.exerciseNumber
      );
      if (!categoryRoundExercise.RoundName) {
        console.log(competitor.CompetitorId);
        console.log(competitor.CatId);
        console.log(categoryRoundExerciseRows);
      }
      tempLatestExercise = {
        Exercise: exercise.exerciseNumber,
        RoundName: categoryRoundExercise.RoundName,
        Execution: competitor[`${exercise.propertyPrefix}E`],
        Difficulty: competitor[`${exercise.propertyPrefix}D`],
        Bonus: competitor[`${exercise.propertyPrefix}B`],
        HorizontalDisplacement: competitor[`${exercise.propertyPrefix}HD`],
        TimeOfFlight: competitor[`${exercise.propertyPrefix}ToF`],
        Synchronisation: competitor[`${exercise.propertyPrefix}S`],
        Penalty: competitor[`${exercise.propertyPrefix}Pen`],
        Total: competitor[totalProperty],
      };
      break;
    }
  }
  competitor.Exercise = tempLatestExercise;
  for (let i = 1; i <= 5; i++) {
    const keysToRemove = [
      `Ex${i}E`,
      `Ex${i}D`,
      `Ex${i}B`,
      `Ex${i}HD`,
      `Ex${i}ToF`,
      `Ex${i}S`,
      `Ex${i}Pen`,
      `Ex${i}Total`,
      `Ex${i}Rank`,
    ];
    for (const key of keysToRemove) {
      if (competitor.hasOwnProperty(key)) {
        delete competitor[key];
      }
    }
  }
  if (!competitor.ZeroRank) {
    competitor.Rank = "-";
  } else {
    if (competitor.CompType === 0 && competitor.F1Total > 0) {
      competitor.Rank = competitor.DisplayZeroRank;
    } else {
      competitor.Rank = competitor.DisplayCumulativeRank;
    }
  }
  const keysToRemove = [
    `ZeroRank`,
    `CumulativeRank`,
    `DisplayZeroRank`,
    `DisplayCumulativeRank`,
    `Club`,
    `Q1StartNo`,
    `Q1Flight`,
    `Q1Scoring`,
  ];
  for (const key of keysToRemove) {
    if (competitor.hasOwnProperty(key)) {
      delete competitor[key];
    }
  }
}

// Function to transform the data
function transformData(rows) {
  // Group rows by panel number
  const groupedPanels = rows.reduce((acc, row) => {
    const panelNo = row.PanelNo;
    if (!acc[panelNo]) {
      acc[panelNo] = [];
    }
    acc[panelNo].push(row);
    return acc;
  }, {});
  // Transform the grouped panels into the desired format
  const scores = Object.keys(groupedPanels).map((panelNo) => {
    const panelRows = groupedPanels[panelNo];

    // Find currentScore (rn = 1) and previousScore (rn = 2)
    //const previousScoreRow = panelRows.find(row => row.rn == 2);
    const currentScoreRow = panelRows.find((row) => row.rn == 1);

    /*console.log(previousScoreRow);
    console.log(currentScoreRow);*/
    return {
      panel: parseInt(panelNo),
      //previousScore: previousScoreRow ? formatScore(previousScoreRow) : null,
      currentGymnast: {
        name:
          currentScoreRow.Status == 1
            ? currentScoreRow.Discipline == "TRS"
              ? (currentScoreRow.NextToCompeteSurname1?.toUpperCase() || "") +
                (currentScoreRow.NextToCompeteSurname2
                  ? ", " + currentScoreRow.NextToCompeteSurname2.toUpperCase()
                  : "")
              : (currentScoreRow.NextToCompeteSurname1?.toUpperCase() || "") +
                (currentScoreRow.NextToCompeteFirstName1
                  ? " " + currentScoreRow.NextToCompeteFirstName1
                  : "")
            : currentScoreRow.Status == 0
            ? currentScoreRow.Discipline == "TRS"
              ? (currentScoreRow.LastToCompeteSurname1?.toUpperCase() || "") +
                (currentScoreRow.LastToCompeteSurname2
                  ? ", " + currentScoreRow.LastToCompeteSurname2.toUpperCase()
                  : "")
              : (currentScoreRow.LastToCompeteSurname1?.toUpperCase() || "") +
                (currentScoreRow.LastToCompeteFirstName1
                  ? " " + currentScoreRow.LastToCompeteFirstName1
                  : "")
            : null,

        club:
          currentScoreRow.Status == 1
            ? currentScoreRow.NextToCompeteClub || ""
            : currentScoreRow.Status == 0
            ? currentScoreRow.LastToCompeteClub || ""
            : null,

        category:
          currentScoreRow.Status == 1
            ? (currentScoreRow.NextToCompeteDiscipline || "") +
              " " +
              (currentScoreRow.NextToCompeteCategory || "")
            : currentScoreRow.Status == 0
            ? (currentScoreRow.LastToCompeteDiscipline || "") +
              " " +
              (currentScoreRow.LastToCompeteCategory || "")
            : null,
      },
      currentScore: currentScoreRow ? formatScore(currentScoreRow) : null,
    };
  });

  return { scores };
}

// Helper function to format each score object
function formatScore(row) {
  return {
    name:
      row.Discipline == "TRS"
        ? row.Surname1.toUpperCase() + ", " + row.Surname2.toUpperCase()
        : row.Surname1.toUpperCase() + " " + row.FirstName1, // Assuming `Name` is the field for the competitor's name
    club: row.DisplayClub,
    category: row.Discipline + " " + row.Category,
    round: row.Exercise.RoundName,
    exercise: {
      execution: row.Exercise.Execution,
      difficulty: row.Exercise.Difficulty,
      timeOfFlight: row.Exercise.TimeOfFlight,
      synchronisation: row.Exercise.Synchronisation,
      horizontalDisplacement: row.Exercise.HorizontalDisplacement,
      bonus: row.Exercise.Bonus,
      penalty: row.Exercise.Penalty,
      total: row.Exercise.Total,
    },
    rank: row.Rank,
  };
}

app.get("/api/bg/test/latest", (req, res) => {
  res.json({
    scores: [
      {
        panel: 1,
        previousScore: {
          name: "DOE John",
          club: "Example Club",
          category: "TRA Youth Men",
          round: "Q2",
          exercise: {
            execution: "16.4",
            difficulty: "0.4",
            timeOfFlight: "6.45",
            synchronisation: null,
            horizontalDisplacement: "7.6",
            bonus: null,
            penalty: "0.0",
            total: "30.85",
          },
          rank: "2",
        },
        currentScore: {
          name: "SMITH Michael",
          club: "Example Club",
          category: "TRA Youth Men",
          round: "Q2",
          exercise: {
            execution: "17.4",
            difficulty: "0.4",
            timeOfFlight: "5.35",
            synchronisation: null,
            horizontalDisplacement: "7.9",
            bonus: null,
            penalty: "0.0",
            total: "31.05",
          },
          rank: "1",
        },
      },
      {
        panel: 2,
        previousScore: {
          name: "DOE Jane",
          club: "Example Club",
          category: "TRA Youth Women",
          round: "Q2",
          exercise: {
            execution: "17.4",
            difficulty: "0.4",
            timeOfFlight: "6.45",
            synchronisation: null,
            horizontalDisplacement: "7.6",
            bonus: null,
            penalty: "0.0",
            total: "31.85",
          },
          rank: "2",
        },
        currentScore: {
          name: "ROGERS Mary",
          club: "Example Club",
          category: "TRA Youth Women",
          round: "Q2",
          exercise: {
            execution: "19.4",
            difficulty: "0.4",
            timeOfFlight: "5.35",
            synchronisation: null,
            horizontalDisplacement: "7.9",
            bonus: null,
            penalty: "0.0",
            total: "33.05",
          },
          rank: "1",
        },
      },
    ],
  });
});

app.get("/api/bg/rankings", (req, res) => {

  let query = `SELECT DISTINCT "FirstName1", "FirstName2", "Surname1", "Surname2", "DisplayClub", "Discipline", "Category", "RoundName", "RoundTotal",
              CASE WHEN "CompType" = 0 THEN "DisplayZeroRank"
            WHEN "CompType" = 1 THEN "DisplayCumulativeRank"
END AS "DisplayRank",
CASE WHEN "CompType" = 0 THEN "ZeroRank"
            WHEN "CompType" = 1 THEN "CumulativeRank"
END AS "Rank"
FROM "${schema}"."DisplayScreenRoundTotals" dsrt
INNER JOIN (
    SELECT "CatId", MAX("RoundOrder") AS LatestRoundOrder
    FROM "${schema}"."DisplayScreenRoundTotals"
    GROUP BY "CatId"
) latestRounds
ON dsrt."CatId" = latestRounds."CatId"
AND dsrt."RoundOrder" = latestRounds.LatestRoundOrder
INNER JOIN "${schema}"."Categories" c on dsrt."CatId" = c."CatId"
ORDER BY "Discipline","Category","Rank"`;
  
  performDatabaseQueryWithRetry(query, [], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(transformToRankings(rows));
    }
  });
});

function transformToRankings(data) {
  const rankings = [];

  // Group data by Category and RoundName
  const groupedData = data.reduce((acc, row) => {
    const key = `${row.Discipline}-${row.Category}-${row.RoundName}`;
    
    if (!acc[key]) {
      acc[key] = {
        category: `${row.Discipline} ${row.Category}`,
        round: row.RoundName,
        competitors: [],
      };
    }

    // Build competitor's full name
    const name = row.Discipline == "TRS" ? `${row.Surname1.toUpperCase()}, ${row.Surname1.toUpperCase()}` : `${row.Surname1.toUpperCase()} ${row.FirstName1}`;

    // Add competitor info
    acc[key].competitors.push({
      name: name.trim(),
      club: row.DisplayClub,
      total: row.RoundTotal,
      rank: row.DisplayRank,
    });

    return acc;
  }, {});

  // Convert grouped data into rankings array
  for (let key in groupedData) {
    rankings.push(groupedData[key]);
  }

  return { rankings };
}

app.get("/api/bg/test/rankings", (req, res) => {
  res.json({
    rankings: [
      {
        category: "TRA Youth Women",
        round: "Q2",
        competitors: [
          {
            name: "ROGERS Mary",
            club: "Example Club 1",
            total: "33.05",
            rank: "1",
          },
          {
            name: "DOE Jane",
            club: "Example Club 2",
            total: "31.85",
            rank: "2",
          },
          {
            name: "SMITH Millie",
            club: "Example Club 3",
            total: "30.20",
            rank: "3",
          },
          {
            name: "HOWARD Catherine",
            club: "Example Club 4",
            total: "28.00",
            rank: "4",
          },
          {
            name: "BOND Chelsea",
            club: "Example Club 5",
            total: "27.35",
            rank: "5",
          },
          {
            name: "PARKES Kate",
            club: "Example Club 6",
            total: "25.05",
            rank: "6",
          },
          {
            name: "WHELAN Helen",
            club: "Example Club 7",
            total: "24.95",
            rank: "7",
          },
          {
            name: "HALPIN Hannah",
            club: "Example Club 8",
            total: "23.20",
            rank: "8",
          },
          {
            name: "MURNAGHAN Wendy",
            club: "Example Club 9",
            total: "14.55",
            rank: "9",
          },
          {
            name: "BILOTTA Maria",
            club: "Example Club 10",
            total: "5.05",
            rank: "10",
          },
        ],
      },
      {
        category: "TRA Youth Men",
        round: "Q2",
        competitors: [
          {
            name: "SMITH Michael",
            club: "Example Club 1",
            total: "31.05",
            rank: "1",
          },
          {
            name: "DOE John",
            club: "Example Club 2",
            total: "30.85",
            rank: "2",
          },
          {
            name: "SAMUELS Mark",
            club: "Example Club 3",
            total: "30.10",
            rank: "3",
          },
          {
            name: "HART Charlie",
            club: "Example Club 4",
            total: "23.40",
            rank: "4",
          },
          {
            name: "BROWN Connor",
            club: "Example Club 5",
            total: "22.05",
            rank: "5",
          },
          {
            name: "POWERS Kieran",
            club: "Example Club 6",
            total: "14.35",
            rank: "6",
          },
          {
            name: "JONES Dan",
            club: "Example Club 7",
            total: "12.10",
            rank: "7",
          },
          {
            name: "COMLEY Nick",
            club: "Example Club 8",
            total: "7.30",
            rank: "8",
          },
          {
            name: "SACH Will",
            club: "Example Club 9",
            total: "4.35",
            rank: "9",
          },
          {
            name: "WEAVER Joe",
            club: "Example Club 10",
            total: "0.00",
            rank: "10",
          },
        ],
      },
    ],
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

  const query = `SELECT * FROM "${schema}"."CategoryRoundExercises" where "CategoryId" = $1 and "ExerciseNumber" = $2`;

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

app.get("/api/onlineResults", (req, res) => {
  const categoryId = req.query.catId;
  const compType = req.query.compType;

  let query = "";
  if (compType == 0) {
    query = `SELECT * FROM "${schema}"."DisplayScreen" WHERE "CatId" = $1 AND "Withdrawn" IS NOT TRUE ORDER BY (CASE WHEN "ZeroRank" IS NULL THEN 1 ELSE 0 END), "ZeroRank", "Q1Flight", "Q1StartNo"`;
  } else {
    query = `SELECT * FROM "${schema}"."DisplayScreen" WHERE "CatId" = $1 AND "Withdrawn" IS NOT TRUE ORDER BY (CASE WHEN "CumulativeRank" IS NULL THEN 1 ELSE 0 END), "CumulativeRank", "Q1Flight", "Q1StartNo"`;
  }
  performDatabaseQueryWithRetry(query, [categoryId], (err, competitorRows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      query = `SELECT * FROM "${schema}"."DisplayScreenExerciseTotals" WHERE "CompetitorId" IN (SELECT "CompetitorId" FROM "${schema}"."DisplayScreen" WHERE "CatId" = $1 AND "Withdrawn" IS NOT TRUE) ORDER BY "ExerciseNumber" ASC`;
      performDatabaseQueryWithRetry(
        query,
        [categoryId],
        (err, exerciseRows) => {
          if (err) {
            console.error("Error executing query:", err.message);
            res.status(500).json({ error: "Internal Server Error" });
          } else {
            query = `SELECT * FROM "${schema}"."RoundTotals" WHERE "CompetitorId" IN (SELECT "CompetitorId" FROM "${schema}"."DisplayScreen" WHERE "CatId" = $1 AND "Withdrawn" IS NOT TRUE)`;
            performDatabaseQueryWithRetry(
              query,
              [categoryId],
              (err, roundTotalRows) => {
                if (err) {
                  console.error("Error executing query:", err.message);
                  res.status(500).json({ error: "Internal Server Error" });
                } else {
                  query = `SELECT * FROM "${schema}"."ExerciseVideos" WHERE "CompetitorId" IN (SELECT "CompetitorId" FROM "${schema}"."DisplayScreen" WHERE "CatId" = $1 AND "Withdrawn" IS NOT TRUE) ORDER BY "Angle" ASC`;
                  performDatabaseQueryWithRetry(
                    query,
                    [categoryId],
                    (err, videoRows) => {
                      if (err) {
                        console.error("Error executing query:", err.message);
                        res
                          .status(500)
                          .json({ error: "Internal Server Error" });
                      } else {
                        query = `SELECT * FROM "${schema}"."ExerciseMedians" where "CompetitorId" IN (SELECT "CompetitorId" FROM "${schema}"."DisplayScreen" WHERE "CatId" = $1 AND "Withdrawn" IS NOT TRUE);`;
                        performDatabaseQueryWithRetry(
                          query,
                          [categoryId],
                          (err, medianRows) => {
                            if (err) {
                              console.error(
                                "Error executing query:",
                                err.message
                              );
                              res
                                .status(500)
                                .json({ error: "Internal Server Error" });
                            } else {
                              query = `SELECT * FROM "${schema}"."ExerciseDeductions" where "CompetitorId" IN (SELECT "CompetitorId" FROM "${schema}"."DisplayScreen" WHERE "CatId" = $1 AND "Withdrawn" IS NOT TRUE);`;
                              performDatabaseQueryWithRetry(
                                query,
                                [categoryId],
                                (err, deductionRows) => {
                                  if (err) {
                                    console.error(
                                      "Error executing query:",
                                      err.message
                                    );
                                    res
                                      .status(500)
                                      .json({ error: "Internal Server Error" });
                                  } else {
                                    result = {};
                                    i = 0;
                                    for (
                                      let dataIndex = 0;
                                      dataIndex < competitorRows.length;
                                      dataIndex++
                                    ) {
                                      const competitorData =
                                        competitorRows[dataIndex];
                                      const competitorExercises =
                                        exerciseRows.filter(
                                          (exercise) =>
                                            exercise.CompetitorId ===
                                            competitorData.CompetitorId
                                        );
                                      competitorData.Exercises =
                                        competitorExercises;
                                      const competitorRoundTotals =
                                        roundTotalRows.filter(
                                          (roundTotal) =>
                                            roundTotal.CompetitorId ===
                                            competitorData.CompetitorId
                                        );
                                      if (competitorRoundTotals.length > 0) {
                                        competitorData.RoundTotals =
                                          competitorRoundTotals;
                                      }
                                      const competitorVideos = videoRows.filter(
                                        (video) =>
                                          video.CompetitorId ===
                                          competitorData.CompetitorId
                                      );
                                      const competitorMedians =
                                        medianRows.filter(
                                          (median) =>
                                            median.CompetitorId ===
                                            competitorData.CompetitorId
                                        );
                                      const competitorDeductions =
                                        deductionRows.filter(
                                          (deduction) =>
                                            deduction.CompetitorId ===
                                            competitorData.CompetitorId
                                        );
                                      if (competitorExercises.length > 0) {
                                        for (
                                          let dataIndex = 0;
                                          dataIndex <
                                          competitorData.Exercises.length;
                                          dataIndex++
                                        ) {
                                          const exerciseData =
                                            competitorData.Exercises[dataIndex];

                                          let exerciseMedians =
                                            competitorMedians.filter(
                                              (median) =>
                                                median.ExerciseNumber ===
                                                exerciseData.ExerciseNumber
                                            );
                                          let exerciseDeductions =
                                            competitorDeductions.filter(
                                              (deduction) =>
                                                deduction.ExerciseNumber ===
                                                exerciseData.ExerciseNumber
                                            );
                                          if (exerciseMedians.length > 0) {
                                            exerciseMedians =
                                              exerciseMedians.map((median) => {
                                                return {
                                                  ...median,
                                                  DeductionNumber:
                                                    (categoryId[0] == "I" ||
                                                      categoryId[0] == "S") &&
                                                    median.DeductionNumber == 11
                                                      ? "L"
                                                      : categoryId[0] == "U" &&
                                                        median.DeductionNumber ==
                                                          9
                                                      ? "L"
                                                      : categoryId[0] == "D" &&
                                                        median.DeductionNumber ==
                                                          3
                                                      ? "L"
                                                      : median.DeductionNumber,
                                                };
                                              });
                                            exerciseData.Medians =
                                              exerciseMedians;
                                          }
                                          if (exerciseDeductions.length > 0) {
                                            exerciseDeductions =
                                              exerciseDeductions.map(
                                                (deduction) => {
                                                  return {
                                                    ...deduction,
                                                    DeductionNumber:
                                                      (categoryId[0] == "I" ||
                                                        categoryId[0] == "S") &&
                                                      deduction.DeductionNumber ==
                                                        11
                                                        ? "L"
                                                        : categoryId[0] ==
                                                            "U" &&
                                                          deduction.DeductionNumber ==
                                                            9
                                                        ? "L"
                                                        : categoryId[0] ==
                                                            "D" &&
                                                          deduction.DeductionNumber ==
                                                            3
                                                        ? "L"
                                                        : deduction.DeductionNumber,
                                                  };
                                                }
                                              );
                                            exerciseData.Deductions =
                                              exerciseDeductions;
                                          }
                                          const exerciseVideos =
                                            competitorVideos.filter(
                                              (video) =>
                                                video.ExerciseNumber ===
                                                exerciseData.ExerciseNumber
                                            );
                                          if (exerciseVideos.length > 0) {
                                            exerciseData.Videos =
                                              exerciseVideos;
                                          }
                                        }
                                      }
                                      for (let i = 1; i <= 5; i++) {
                                        const keysToRemove = [
                                          `Ex${i}E`,
                                          `Ex${i}D`,
                                          `Ex${i}B`,
                                          `Ex${i}HD`,
                                          `Ex${i}ToF`,
                                          `Ex${i}S`,
                                          `Ex${i}Pen`,
                                          `Ex${i}Total`,
                                          `Ex${i}Rank`,
                                        ];
                                        for (const key of keysToRemove) {
                                          if (
                                            competitorData.hasOwnProperty(key)
                                          ) {
                                            delete competitorData[key];
                                          }
                                        }
                                      }
                                    }
                                    res.json(competitorRows);
                                  }
                                }
                              );
                            }
                          }
                        );
                      }
                    }
                  );
                }
              }
            );
          }
        }
      );
    }
  });
});

app.get("/api/startListRounds", (req, res) => {
  const query = `SELECT 
        r."CategoryId", 
        r."RoundName", 
        c."Discipline", 
        c."Category", 
        r1."NumberOfRounds" 
      FROM 
        "${schema}"."Rounds" r
      INNER JOIN 
        "${schema}"."Categories" c ON r."CategoryId" = c."CatId"
      INNER JOIN 
        (SELECT DISTINCT "CategoryId", COUNT(*) AS "NumberOfRounds" FROM "${schema}"."Rounds" GROUP BY "CategoryId") r1 
        ON r."CategoryId" = r1."CategoryId"
      WHERE
    r."RoundOrder" = (
        SELECT MIN(r2."RoundOrder")
        FROM "${schema}"."Rounds" r2
        WHERE r2."CategoryId" = r."CategoryId"
    )
    OR (r."RoundOrder" > (
        SELECT MIN(r2."RoundOrder")
        FROM "${schema}"."Rounds" r2
        WHERE r2."CategoryId" = r."CategoryId"
    )
    AND EXISTS (
        SELECT 1
        FROM "${schema}"."Rounds" prev
        WHERE prev."CategoryId" = r."CategoryId"
        AND prev."RoundOrder" = r."RoundOrder" - 1
        AND prev."SignedOff" = true
    ))
ORDER BY
    c."No", r."RoundOrder"`;
  performDatabaseQueryWithRetry(query, [], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/eventInfo", (req, res) => {
  const query = `SELECT * FROM "${schema}"."Event"`;
  performDatabaseQueryWithRetry(query, [], (err, rows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.json(rows);
    }
  });
});

app.get("/api/videoFile", (req, res) => {
  const event = req.query.event;
  const fileName = req.query.fileName;
  const variant = req.query.variant;
  const directory = "\\\\10.0.0.4\\Video-Drive";
  const filePath = path.join(directory, event, variant, fileName);
  res.sendFile(filePath, (err) => {
    if (err) {
      console.error("File failed to send:", err);
      res.status(500).send("Error sending file");
    }
  });
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
