import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import mgenerate from "mgeneratejs";

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

function* genDocs(nDoc) {
  let template = {
    id: { $inc: { start: 1, step: 1 } },
    amount: { $integer: { min: 100, max: 1000 } },
    funding_type: {
      $choose: { from: ["time", "day", "week"], weights: [1, 1, 1] },
    },
    funding_option1: {
      $binary: { length: { $integer: { min: 50, max: 100 } } },
    },
    funding_option2: {
      $binary: { length: { $integer: { min: 50, max: 100 } } },
    },
    funding_option3: {
      $binary: { length: { $integer: { min: 50, max: 100 } } },
    },
  };
  const NGEN = 1000;
  let option;
  for (let i = 0; i < nDoc; i += NGEN) {
    let docs = [];
    let doc;
    for (let j = 0; j < NGEN; j++) {
      doc = mgenerate(template);
      docs.push({
        _id: doc.id,
        amount: doc.amount,
        funding_type: doc.funding_type,
        funding_option1: doc.funding_option1.toString(),
        funding_option2: doc.funding_option2.toString(),
        funding_option3: doc.funding_option3.toString(),
      });
    }
    yield docs;
  }
}

async function createCollection(client, dbName, collName) {
  const db = client.db(dbName);
  await db
    .collection(collName)
    .drop()
    .catch(() => {
      console.log("Collection not found");
    });
  const collection = await db.createCollection(collName, {
    clusteredIndex: {
      key: { _id: 1 },
      unique: true,
      name: "restaurants_id",
    },
  });
  await collection.createIndex({ funding_type: 1, _id: 1 });
  return collection;
}

async function loadDocs() {
  let [uri, nDoc, _, dbName, collName] = loadConfig();

  const client = new MongoClient(uri);
  await client.connect();
  const coll = await createCollection(client, dbName, collName);

  const gendocs = genDocs(nDoc);
  for (
    let { value, done } = gendocs.next();
    !done;
    { value, done } = gendocs.next()
  ) {
    await coll.insertMany(value);
    console.log(`Inserted ${value[0]._id} ~ ${value[999]._id} documents`);
  }

  await client.close();
}

await loadDocs();
