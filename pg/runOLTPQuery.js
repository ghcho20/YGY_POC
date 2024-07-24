import dotenv from "dotenv";
import fs from "fs";
import pg from "pg";

function loadConfig() {
  dotenv.config();
  dotenv.config({ path: "../.env" });
  return [
    process.env.PG_URI,
    process.env.PG_USR,
    process.env.PG_PWD,
    process.env.DB,
    process.env.CO,
    process.env.START_PARALLEL,
    process.env.MAX_PARALLEL,
    process.env.STEP_PARALLEL,
  ];
}

function sleep(waitms) {
  return new Promise((resolve) => setTimeout(resolve, waitms));
}

async function runQuery() {
  const { Pool } = pg;
  const [uri, user, pwd, db, tbl, startParallel, maxParallel, stepParallel] =
    loadConfig();
  const maxClient = 600;

  const pool = new Pool({
    host: uri,
    database: db,
    user: user,
    password: pwd,
    max: maxClient,
    idleTimeoutMillis: 30000,
  });

  let explain;
  const project =
    "_id, amount, funding_type, funding_option1," +
    "funding_option2, funding_option3";
  const query =
    "SELECT (" +
    project +
    ") " +
    `FROM ${tbl} WHERE ` +
    `funding_type='time' AND _id=`;

  let maxThruput = 0,
    minThruput = 1_000_000;
  for (
    let nParallel = parseInt(startParallel), thruput;
    nParallel <= parseInt(maxParallel);
    nParallel += parseInt(stepParallel)
  ) {
    console.log(
      "\n====================================================================",
    );
    console.log(`Running ${nParallel} Queries`);
    const start = Date.now();
    let end,
      nProcessed = 0;
    for (let i = 0; i < nParallel; i++) {
      pool.query(query + `(${nParallel + i})`).then((res) => {
        end = Date.now();
        nProcessed++;
      });
      // if (i !== 0 && i % 100 === 0) {
      //   if (i % 1000 === 0) {
      //     process.stdout.write(">");
      //   } else {
      //     process.stdout.write("-");
      //   }
      //   if (i % 10000 === 0) {
      //     process.stdout.write("\n" + i / 10000);
      //   }
      // }
    }
    while (nProcessed < nParallel) {
      await sleep(200);
    }
    console.log();
    const elapse = end - start;
    console.log(
      `ㄴ${new Date(start).toLocaleString("co-KR")} ~ ${new Date(
        end,
      ).toLocaleString("co-KR")}`,
    );
    console.log("   ㄴelapsed       :", elapse);
    console.log("   ㄴtotal queries :", nParallel);
    thruput = Math.floor(((nParallel * 1000) / elapse) * 100) / 100;
    maxThruput = Math.max(maxThruput, thruput);
    minThruput = Math.min(minThruput, thruput);
    console.log(`   ㄴthruput       : ${thruput} ops/sec`);
    console.log("   ㄴavg time/query:", elapse / nParallel, "ms");
  }
  console.log("-----");
  console.log("Max Thruput:", maxThruput);
  console.log("Min Thruput:", minThruput);

  await pool.end();
}

await runQuery();
