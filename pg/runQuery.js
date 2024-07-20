import dotenv from "dotenv";
import fs from "fs";
import pg from "pg";

function loadConfig() {
  dotenv.config();
  dotenv.config({ path: "../.env" });
  return [
    process.env.PG_URI,
    process.env.nLOOP,
    process.env.PG_USR,
    process.env.PG_PWD,
    process.env.DB,
    process.env.CO,
  ];
}

async function runQuery() {
  const filter500 = JSON.parse(fs.readFileSync("../filter500.json"));
  const filter1000 = JSON.parse(fs.readFileSync("../filter1000.json"));
  const { Client } = pg;
  const [uri, nLoop, user, pwd, db, tbl] = loadConfig();

  const client = new Client({
    host: uri,
    database: db,
    user: user,
    password: pwd,
  });
  await client.connect();

  let query, result, explain;
  const project =
    "_id, amount, funding_type, funding_option1," +
    "funding_option2, funding_option3";
  for (let filterIn of [filter500, filter1000]) {
    console.log(
      "\n====================================================================",
    );
    console.log(`Running Queries with Filter${filterIn.length}`);
    query =
      "SELECT (" +
      project +
      ") " +
      `FROM ${tbl} WHERE ` +
      `funding_type='time' AND _id IN (${filterIn.join()})`;

    const start = Date.now();
    process.stdout.write("0");
    for (let i = 0; i < nLoop; i++) {
      result = await client.query(query);
      if (i !== 0 && i % 100 === 0) {
        if (i % 1000 === 0) {
          process.stdout.write(">");
        } else {
          process.stdout.write("-");
        }
        if (i % 10000 === 0) {
          process.stdout.write("\n" + i / 10000);
        }
      }
    }
    console.log();
    const end = Date.now();
    const elapse = end - start;
    console.log(
      `ㄴ${new Date(start).toLocaleString("co-KR")} ~ ${new Date(
        end,
      ).toLocaleString("co-KR")}`,
    );
    console.log("   ㄴelapsed       :", elapse);
    console.log("   ㄴtotal queries :", nLoop);
    console.log(
      "   ㄴthruput       :",
      Math.floor(((nLoop * 1000) / elapse) * 100) / 100,
      "ops/sec",
    );
    console.log("   ㄴavg time/query:", elapse / nLoop, "ms");
    explain = await client.query(`EXPLAIN ANALYZE ${query}`);
    console.log("ㄴqueryPlan:", explain.rows[0]["QUERY PLAN"]);
    console.log("ㄴ" + explain.rows[4]["QUERY PLAN"]);
    console.log("ㄴ" + explain.rows[5]["QUERY PLAN"]);
  }

  client.end();
}

await runQuery();
