const { Writable, Readable } = require('stream');

class DeleteReadableStream extends Readable {
  constructor(ids, options = {}) {
    super({ ...options, objectMode: true });
    this.ids = ids;
    this.index = 0;
  }

  _read() {
    if (this.index < this.ids.length) {
      this.push(this.ids.slice(this.index, this.index + CHUNK_SIZE));
      this.index += CHUNK_SIZE;
    } else {
      this.push(null);
    }
  }
}

class DeleteWritableStream extends Writable {
  constructor(db, metrics, options = {}) {
    super({ ...options, objectMode: true });
    this.db = db;
    this.metrics = metrics;
  }

  async _write(chunk, encoding, callback) {
    try {
      const deleteResult = await this.db.collection('Products').deleteMany({ _id: { $in: chunk } });
      if (deleteResult.deletedCount) {
        this.metrics.deletedCount += deleteResult.deletedCount;
      }
      callback();
    } catch (error) {
      callback(error);
    }
  }
}

module.exports = {
  DeleteReadableStream,
  DeleteWritableStream
};
