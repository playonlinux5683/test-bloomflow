const fs = require('fs');
const uuidv4 = require('uuid').v4;
const { MongoClient } = require('mongodb');
const minimist = require('minimist');
const Promise = require('bluebird');
const { timing } = require('./helpers/timing');
const { memory } = require('./helpers/memory');
const { Metrics } = require('./helpers/metrics');
const { Product } = require('./product');
const { Readable } = require('stream');

const DATABASE_NAME = 'test-product-catalog';
const MONGO_URL = `mongodb://localhost:27017/${DATABASE_NAME}`;
const catalogUpdateFile = 'updated-catalog.csv';

const { size } = minimist(process.argv.slice(2));
if (!size) {
  throw new Error("Missing 'size' parameter");
}

async function main() {
  const mongoClient = new MongoClient(MONGO_URL);
  const connection = await mongoClient.connect();
  const db = connection.db();

  // For running the script several times without manually cleaning the data
  await clearExistingData(db);

  await memory(
    'Generate dataset',
    () => timing(
      'Generate dataset',
      () => generateDataset(db, size)));
}

async function clearExistingData(db) {
  const listDatabaseResult = await db.admin().listDatabases({ nameOnly: 1 });
  if (listDatabaseResult.databases.find(d => d.name === DATABASE_NAME)) {
    await db.dropDatabase();
  }

  if (fs.existsSync(catalogUpdateFile)) {
    fs.rmSync(catalogUpdateFile);
  }
}

async function generateDataset(db, catalogSize) {
  writeCsvHeaders();

  const metrics = Metrics.zero();
  const createdAt = new Date();

  const productStream = new ProductStream(catalogSize, createdAt);
  let index = 0;
  for await (const products of productStream) {

    await db.collection('Products').insertMany(products);

    for (const product of products) {
      const updatedProduct = generateUpdate(product, index, catalogSize);
      metrics.merge(writeProductUpdateToCsv(product, updatedProduct));
      logProgress(index, catalogSize);
      index++;
    }
  }

  logMetrics(catalogSize, metrics);
}

class ProductStream extends Readable {

  constructor(catalogSize, createdAt, chunkSize = 1000) {
    super({ objectMode: true });

    this.catalogSize = catalogSize;
    this.createdAt = createdAt;
    this.chunkSize = chunkSize;
    this.index = 0;
  }

  _read() {
    if (this.index < this.catalogSize) {
      const size = Math.min(this.chunkSize, this.catalogSize - this.index);
      const chunk = Array.from({ length: size }, () => generateProduct(this.index, this.createdAt));
      this.index += size;
      this.push(chunk);
    } else {
      this.push(null); // Signal end of stream
    }
  }
}

function logProgress(numerator, denominator) {
  const progressIndicator = numerator * 100 / denominator;
  if (progressIndicator % 10 === 0) {
    console.debug(`[DEBUG] Processing ${progressIndicator}%...`);
  }
}

function writeCsvHeaders() {
  fs.appendFileSync(catalogUpdateFile, Object.keys(generateProduct(-1, null)).join(',') + '\n');
}

function generateProduct(index, createdAt) {
  return new Product(uuidv4(), `Product_${index}`, generatePrice(), createdAt, createdAt);
}

function generatePrice() {
  return Math.round(Math.random() * 1000 * 100) / 100;
}

const productEvent = {
  pDelete: 10, // probability of deleting the product
  pUpdate: 10, // probability of updating the product
  pAdd: 20, // probability of adding a new product
};

function generateUpdate(product, index, catalogSize) {
  const rand = Math.random() * 100; // float in [0; 100]
  if (rand < productEvent.pDelete) { // [0; pDelete[
    // Delete product
    return null;
  }
  if (rand < productEvent.pDelete + productEvent.pUpdate) { // [pDelete; pUpdate[
    // Update product
    return new Product(product._id, `Product_${index + catalogSize}`, generatePrice(), product.createdAt, new Date());
  }
  if (rand < productEvent.pDelete + productEvent.pUpdate + productEvent.pAdd) { // [pUpdate; pAdd[
    // Add new product
    return generateProduct(index + catalogSize, new Date());
  }

  // Unchanged product
  return product; // [pAdd; 100]
}

function writeProductUpdateToCsv(product, updatedProduct) {
  if (!updatedProduct) {
    return Metrics.deleted();
  }
  if (updatedProduct._id === product._id) {
    // Updated product or no modification => add this line
    fs.appendFileSync(catalogUpdateFile, updatedProduct.toCsv() + '\n');
    return updatedProduct.updatedAt !== updatedProduct.createdAt ? Metrics.updated() : Metrics.zero();
  }
  // keep product
  fs.appendFileSync(catalogUpdateFile, product.toCsv() + '\n');
  // add new product
  fs.appendFileSync(catalogUpdateFile, updatedProduct.toCsv() + '\n');
  return Metrics.added();
}

function logMetrics(catalogSize, metrics) {
  console.info(`[INFO] ${catalogSize} products inserted in DB.`);
  console.info(`[INFO] ${metrics.addedCount} products to be added.`);
  console.info(`[INFO] ${metrics.updatedCount} products to be updated ${(metrics.updatedCount * 100 / catalogSize).toFixed(2)}%.`);
  console.info(`[INFO] ${metrics.deletedCount} products to be deleted ${(metrics.deletedCount * 100 / catalogSize).toFixed(2)}%.`);
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
