const fs = require('fs');
const { MongoClient } = require('mongodb');
const csv = require('csv-parser');
const _ = require('lodash');
const Promise = require('bluebird');

const { memory } = require('./helpers/memory');
const { timing } = require('./helpers/timing');
const { Metrics } = require('./helpers/metrics');
const { Product } = require('./product');

const { DeleteReadableStream, DeleteWritableStream } = require('./deletion-streams');

const MONGO_URL = 'mongodb://localhost:27017/test-product-catalog';
const catalogUpdateFile = 'updated-catalog.csv';
const CHUNK_SIZE = 1000;


async function main() {
  const mongoClient = new MongoClient(MONGO_URL);
  const connection = await mongoClient.connect();
  const db = connection.db();
  await memory(
    'Update dataset',
    () => timing(
      'Update dataset',
      () => updateDataset(db)));
}

async function updateDataset(db) {
  const metrics = Metrics.zero();
  function updateMetrics(updateResult) {
    if (updateResult.modifiedCount) {
      metrics.updatedCount += updateResult.modifiedCount;
    }
    if (updateResult.upsertedCount) {
      metrics.addedCount += updateResult.upsertedCount;
    }
  }

  const dbCatalogSize = await db.collection('Products').count();
  const closestPowOf10 = 10 ** (Math.ceil(Math.log10(dbCatalogSize)));
  function logProgress(nbCsvRows) {
    const progressIndicator = Math.round(nbCsvRows * 100 / closestPowOf10);
    if (progressIndicator % 10 === 0) {
      console.debug(`[DEBUG] Processed ${nbCsvRows} rows...`);
    }
  }

  let products = [];
  let rowCount = 0;

  const processChunk = async (chunk) => {
    const bulkOps = chunk.map(product => ({
      updateOne: {
        filter: { _id: product._id },
        update: { $set: product },
        upsert: true
      }
    }));
    if (bulkOps.length > 0) {
      const updateResult = await db.collection('Products').bulkWrite(bulkOps);
      updateMetrics(updateResult);
      logProgress(rowCount);
    }
  };

  const dbIds = (await db.collection('Products').find({}, { projection: { _id: 1 } }).toArray()).map(o => o._id);

  const csvStream = fs.createReadStream(catalogUpdateFile)
    .pipe(csv())
    .on('data', async (row) => {
      const product = Product.fromCsv(Object.values(row).join(','));
      products.push(product);
      rowCount++;

      if (products.length === CHUNK_SIZE) {
        await processChunk(products);
        products = [];
      }
    })
    .on('end', async () => {
      if (products.length > 0) {
        await processChunk(products);
      }

      const productIds = products.map(p => p._id);
      const deletedProductIds = _.difference(dbIds, productIds);
      const readable = new DeleteReadableStream(deletedProductIds);

      const deleteStream = new DeleteWritableStream(db, metrics);
      deleteStream.on('finish', () => {
        logMetrics(rowCount, metrics);
      });

      readable.pipe(deleteStream);
    })
    .on('error', (error) => {
      console.error(error);
    });

  return new Promise((resolve, reject) => {
    csvStream.on('end', resolve);
    csvStream.on('error', reject);
  });
}

function logMetrics(numberOfProcessedRows, metrics) {
  console.info(`[INFO] Processed ${numberOfProcessedRows} CSV rows.`);
  console.info(`[INFO] Added ${metrics.addedCount} new products.`);
  console.info(`[INFO] Updated ${metrics.updatedCount} existing products.`);
  console.info(`[INFO] Deleted ${metrics.deletedCount} products.`);
}

if (require.main === module) {
  main()
    .then(() => {
      console.log('SUCCESS');
      process.exit(0);
    })
    .catch(err => {
      console.log('FAIL');
      console.error(err);
      process.exit(1);
    });
}
