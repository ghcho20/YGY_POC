import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import fs from "fs";
import nopt from "nopt";

function loadConfig() {
  dotenv.config();
  return [
    process.env.ATLAS_URI,
    process.env.nDOC,
    process.env.DB,
    process.env.CO,
    process.env.START_PARALLEL,
    process.env.MAX_PARALLEL,
    process.env.STEP_PARALLEL,
  ];
}

async function warmUpCache(coll) {
  console.log("Warming up connection pool");

  const nLimit = 600;
  let cursors = [];
  for (let i = 0; i < nLimit; i++) {
    const cursor = coll.find({ _id: i + 1000 });
    cursors.push(cursor);
  }
  await Promise.all(cursors.map((cursor) => cursor.limit(1).toArray()));
  await Promise.all(cursors.map((cursor) => cursor.close()));
}

function sleep(waitms) {
  return new Promise((resolve) => setTimeout(resolve, waitms));
}

async function runQuery(args) {
  let [uri, _, dbName, collName, startParallel, maxParallel, stepParallel] =
    loadConfig();

  const client = new MongoClient(uri);
  const coll = client.db(dbName).collection(collName);

  await warmUpCache(coll);

  let cursor, explain;
  const project = {
    _id: 1,
    amount: 1,
    funding_type: 1,
    funding_option1: 1,
    funding_option2: 1,
    funding_option3: 1,
  };
  let filter = {
    funding_type: "time",
    _id: { $in: [] },
  };
  let cursors = [],
    minThruput = 1_000_000,
    maxThruput = 0;
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
    let end;
    let cursor,
      nProcessed = 0;
    function runCursor(cursor) {
      cursor.toArray().then((res) => {
        end = Date.now();
        nProcessed++;
        cursors.push(cursor.close());
      });
    }
    for (let i = 0, randi, randj; i < nParallel; i++) {
      randi = Math.floor(Math.random() * 100_000) + 1;
      randj = Math.floor(Math.random() * 100_000) + 100_001;
      filter._id.$in = [randi, randj];
      cursor = coll.find(filter, project);
      runCursor(cursor);
    }
    console.log();
    while (nProcessed < nParallel) {
      await sleep(200);
    }
    Promise.all(cursors);
    const elapse = end - start;
    console.log(
      `ㄴ${new Date(start).toLocaleString("co-KR")} ~ ${new Date(
        end,
      ).toLocaleString("co-KR")}`,
    );
    console.log("   ㄴelapsed       :", elapse);
    console.log("   ㄴtotal queries :", nParallel);
    thruput = Math.floor(((nParallel * 1000) / elapse) * 100) / 100;
    minThruput = Math.min(minThruput, thruput);
    maxThruput = Math.max(maxThruput, thruput);
    console.log(`   ㄴthruput       : ${thruput} ops/sec`);
    console.log("   ㄴavg time/query:", elapse / nParallel, "ms");
    cursor = coll.find(filter, project);
    explain = await cursor.explain("executionStats");
    await cursor.close();
  }
  console.log("-----");
  console.log("Max Thruput:", maxThruput);
  console.log("Min Thruput:", minThruput);

  await client.close();
}

const knownOpts = {
  warmUp: Boolean,
};
const shortHands = {
  w: ["--warmUp"],
};
await runQuery(nopt(knownOpts, shortHands));
