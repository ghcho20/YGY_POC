import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import fs from "fs";
import nopt from "nopt";

function loadConfig() {
  dotenv.config();
  return [
    process.env.ATLAS_URI,
    process.env.nDOC,
    process.env.nLOOP,
    process.env.DB,
    process.env.CO,
  ];
}

async function warmUpCache(coll) {
  console.log("Warming up cache");

  const nLimit = 5000;
  let cursor, nSkip, filter, nDocs;
  for (let type of ["time", "day", "week"]) {
    nSkip = 0;
    filter = { funding_type: type };
    nDocs = await coll.countDocuments(filter);
    for (nSkip = 0; nDocs > 0; nDocs -= nLimit, nSkip += nLimit) {
      cursor = coll.find(filter).skip(nSkip).limit(nLimit);
      await cursor.toArray();
      process.stdout.write(">");
    }
  }
  console.log();
}

async function runQuery(args) {
  const filter500 = JSON.parse(fs.readFileSync("./filter500.json"));
  const filter1000 = JSON.parse(fs.readFileSync("./filter1000.json"));
  let [uri, _, nLoop, dbName, collName] = loadConfig();

  const client = new MongoClient(uri);
  await client.connect();
  const coll = client.db(dbName).collection(collName);

  if (args["warmUp"]) {
    await warmUpCache(coll);
    await client.close();
    return;
  }

  let filter, cursor, explain;
  const project = {
    _id: 1,
    amount: 1,
    funding_type: 1,
    funding_option1: 1,
    funding_option2: 1,
    funding_option3: 1,
  };
  for (let filterIn of [filter500, filter1000]) {
    console.log(
      "\n====================================================================",
    );
    console.log(`Running Queries with Filter${filterIn.length}`);
    filterIn = filterIn.map((id) => new Date(id));
    filter = { funding_type: "time", _id: { $in: filterIn } };
    const start = Date.now();
    process.stdout.write("0");
    for (let i = 0; i < nLoop; i++) {
      cursor = coll.find(filter, project);
      await cursor.toArray();
      await cursor.close();
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
    cursor = coll.find(filter, project);
    explain = await cursor.explain("executionStats");
    await cursor.close();
    console.log("ㄴexplain:", explain);
  }

  await client.close();
}

const knownOpts = {
  warmUp: Boolean,
};
const shortHands = {
  w: ["--warmUp"],
};
await runQuery(nopt(knownOpts, shortHands));
