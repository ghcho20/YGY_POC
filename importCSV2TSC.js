import fs from "fs";
import readline from "readline";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";

function loadConfig() {
  dotenv.config();
  return [process.env.ATLAS_URI, process.env.DB, process.env.CO];
}

async function connect(uri, dbName, collName) {
  const client = new MongoClient(uri);
  await client.connect();
  const coll = client.db(dbName).collection(collName);
  return [client, coll];
}

async function importCSV() {
  const [client, coll] = await connect(...loadConfig());
  const filestream = fs.createReadStream("./poc.shop.csv");
  const rl = readline.createInterface({
    input: filestream,
    crlfDelay: Infinity,
  });

  let keys = undefined;
  let docs = undefined;
  let nLineRead = 0;
  for await (let line of rl) {
    line = line.split(",");
    if (keys === undefined) {
      keys = line;
      continue;
    }

    if (nLineRead % 500 === 0) {
      if (docs !== undefined) {
        await coll.insertMany(docs);
        process.stdout.write(">");
        if ((nLineRead / 500) % 100 === 0) {
          console.log();
        }
      }
      docs = [];
    }

    let doc = {};
    for (let i in keys) {
      doc[keys[i]] = line[i];
    }
    doc._id = new Date(doc._id);
    doc.amount = parseInt(doc.amount);
    docs.push(doc);
    nLineRead++;
  }
  console.log("\nkeys:", keys);
  await client.close();
}

await importCSV();
