const fs = require('fs');
const { MongoClient } = require('mongodb');
const Promise = require('bluebird');

const { memory } = require('./helpers/memory');
const { timing } = require('./helpers/timing');
const { Metrics } = require('./helpers/metrics');
const { Product } = require('./product');

const _ = require('lodash');
const { Readable } = require('stream');

const MONGO_URL = 'mongodb://localhost:27017/test-product-catalog';
const catalogUpdateFile = 'updated-catalog.csv';

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


function arrayToStream(array, chunkSize) {
  let index = 0;

  return new Readable({
    objectMode: true,
    read() {
      if (index < array.length) {
        const chunk = array.slice(index, index + chunkSize);
        index += chunkSize;
        this.push(chunk);
      } else {
        this.push(null); // Signal end of stream
      }
    }
  });
}

async function updateDataset(db) {
  const csvContent = fs.readFileSync(catalogUpdateFile, 'utf-8');
  const rowsWithHeader = csvContent.split('\n');
  const dataRows = rowsWithHeader.slice(1);// skip headers

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
    const progressIndicator = nbCsvRows * 100 / closestPowOf10;
    if (progressIndicator % 10 === 0) {
      console.debug(`[DEBUG] Processed ${nbCsvRows} rows...`);
    }
  }

  const allProducts = dataRows.filter(dataRow => dataRow).map(row => Product.fromCsv(row));
  const stream = arrayToStream(allProducts, 1000);
  let index = 0;
  for await (const products of stream) {
    await Promise.map(products, async (product) => {
      const updateResult = await db.collection('Products')
        .updateOne(
          { _id: product._id },
          { $set: product },
          { upsert: true });
      updateMetrics(updateResult);
      logProgress(++index);
    });
  }

  const dbIds = (await db.collection('Products').find({}, { _id: 1 }).toArray()).map(o => o._id);
  const productIds = allProducts.map(product => product._id);
  const deletedProductIds = _.difference(dbIds, productIds);
  for await (const productIds of arrayToStream(deletedProductIds, 1000)) {
    const deletionResult = await db.collection('Products').deleteMany({ _id: { $in: productIds } });
    metrics.deletedCount += deletionResult.deletedCount;

  }
  logMetrics(dataRows.length - 1, metrics); // dataRows.length-1 because there is a new line at the end of file.
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
