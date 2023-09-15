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

app.get("/api/latest", (req, res) => {
  const panelNumber = req.query.panelNumber;
  const query =
    "SELECT * FROM DisplayScreen WHERE PanelNo = ? AND (Withdrawn IS NULL OR Withdrawn != 1) ORDER BY LastUpdatedTimestamp DESC LIMIT 1";

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
          const query =
            "SELECT * FROM CategoryRoundExercises where CategoryId = ?";

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
      "SELECT DISTINCT CompetitorId, FirstName1, FirstName2, Surname1, Surname2, Nation, DisplayClub, ZeroRank, DisplayZeroRank, DisplayCumulativeRank FROM DisplayScreenRoundTotals WHERE CatId = ? ORDER BY ZeroRank LIMIT 8";
  } else {
    query =
      "SELECT DISTINCT CompetitorId, FirstName1, FirstName2, Surname1, Surname2, Nation, DisplayClub, ZeroRank, DisplayZeroRank, DisplayCumulativeRank FROM DisplayScreenRoundTotals WHERE CatId = ? ORDER BY CumulativeRank LIMIT 8";
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
    "SELECT DISTINCT CompetitorId, FirstName1, FirstName2, Surname1, Surname2, Nation, DisplayClub FROM DisplayScreen WHERE CatId= ? AND (Withdrawn IS NULL OR Withdrawn != 1) ORDER BY Q1Flight, Q1StartNo LIMIT 8";

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
  const query =
    "SELECT CompetitorId FROM RoundCompetitors rc INNER JOIN Rounds r on rc.RoundId = r.RoundId WHERE CategoryId = ? and RoundName = ? ORDER BY FlightId, StartNo";

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
  const query =
    "SELECT c.FirstName1, c.FirstName2, c.Surname1, c.Surname2, c.DisplayClub FROM Competitors c INNER JOIN RoundCompetitors rc on c.CompetitorId = rc.CompetitorId INNER JOIN Rounds r on rc.RoundId = r.RoundId and r.CategoryId = ? and r.RoundName = ?  INNER JOIN Flights f on f.FlightId = rc.FlightId ORDER BY f.flightNumber, rc.StartNo";

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

app.get("/api/onlineResults", (req, res) => {
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
  performDatabaseQueryWithRetry(query, [categoryId], (err, competitorRows) => {
    if (err) {
      console.error("Error executing query:", err.message);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      query =
        "SELECT * FROM DisplayScreenExerciseTotals WHERE CompetitorId IN (SELECT CompetitorId FROM DisplayScreen WHERE CatId = ? AND (Withdrawn IS NULL OR Withdrawn != 1))";
      performDatabaseQueryWithRetry(
        query,
        [categoryId],
        (err, exerciseRows) => {
          if (err) {
            console.error("Error executing query:", err.message);
            res.status(500).json({ error: "Internal Server Error" });
          } else {
            query =
              "SELECT * FROM RoundTotals WHERE CompetitorId IN (SELECT CompetitorId FROM DisplayScreen WHERE CatId = ? AND (Withdrawn IS NULL OR Withdrawn != 1))";
            performDatabaseQueryWithRetry(
              query,
              [categoryId],
              (err, roundTotalRows) => {
                if (err) {
                  console.error("Error executing query:", err.message);
                  res.status(500).json({ error: "Internal Server Error" });
                } else {
                  query =
                    "SELECT * FROM ExerciseVideos WHERE CompetitorId IN (SELECT CompetitorId FROM DisplayScreen WHERE CatId = ? AND (Withdrawn IS NULL OR Withdrawn != 1))";
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
                        query =
                          "SELECT * FROM ExerciseMedians where CompetitorId IN (SELECT CompetitorId FROM DisplayScreen WHERE CatId = ? AND (Withdrawn IS NULL OR Withdrawn != 1));";
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
                              result = {};
                              i = 0;
                              for (
                                let dataIndex = 0;
                                dataIndex < competitorRows.length;
                                dataIndex++
                              ) {
                                const competitorData =
                                  competitorRows[dataIndex];
                                const competitorExercises = exerciseRows.filter(
                                  (exercise) =>
                                    exercise.CompetitorId ===
                                    competitorData.CompetitorId
                                );
                                competitorData.Exercises = competitorExercises;
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
                                const competitorMedians = medianRows.filter(
                                  (median) =>
                                    median.CompetitorId ===
                                    competitorData.CompetitorId
                                );
                                if (competitorExercises.length > 0) {
                                  for (
                                    let dataIndex = 0;
                                    dataIndex < competitorData.Exercises.length;
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
                                    if (exerciseMedians.length > 0) {
                                      exerciseMedians = exerciseMedians.map(
                                        (median) => {
                                          return {
                                            ...median,
                                            DeductionNumber:
                                              (categoryId[0] == "I" ||
                                                categoryId[0] == "S") &&
                                              median.DeductionNumber == 11
                                                ? "L"
                                                : categoryId[0] == "U" &&
                                                  median.DeductionNumber == 9
                                                ? "L"
                                                : categoryId[0] == "D" &&
                                                  median.DeductionNumber == 3
                                                ? "L"
                                                : median.DeductionNumber,
                                          };
                                        }
                                      );
                                      exerciseData.Medians = exerciseMedians;
                                    }
                                    const exerciseVideos =
                                      competitorVideos.filter(
                                        (video) =>
                                          video.ExerciseNumber ===
                                          exerciseData.ExerciseNumber
                                      );
                                    if (exerciseVideos.length > 0) {
                                      exerciseData.Videos = exerciseVideos;
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
                                    if (competitorData.hasOwnProperty(key)) {
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
  });
});

app.get("/api/startListRounds", (req, res) => {
  const categoryId = req.query.catId;
  const query = "SELECT r.CategoryId, r.RoundName, c.Discipline, c.Category FROM Rounds r INNER JOIN Categories c on r.CategoryId = c.CatId WHERE r.roundOrder = 1 OR (r.roundOrder > 1 AND EXISTS (SELECT 1 FROM Rounds prev WHERE prev.CategoryId = r.CategoryId AND prev.roundOrder = r.roundOrder - 1 AND prev.SignedOff = 1))";
  //TODO: ADD QUERY FOR ONLINE START LISTS
  performDatabaseQueryWithRetry(query, [], (err, rows) => {
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
