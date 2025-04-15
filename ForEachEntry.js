const fs = require('node:fs');

const { Readable, Transform } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const debug = require('debug')('packet-tools');

const { Mutex } = require('async-mutex');

const csv = require('csv');
const {
  v7: uuidv7,
} = require('uuid');

const handlebars = require('handlebars');
const ParallelStream = require('./ParallelStream');

const {
  getTempFilename, getBatchTransform, getFile, stream,
} = require('./fileTools');

class ForEachEntry {
  constructor() {
    this.timelineOutputMutex = new Mutex();
  }

  getTimelineOutputStream() {
    return this.timelineOutputMutex.runExclusive(async () => {
      if (this.outputStream) return this.outputStream;
      const timelineFile = await getTempFilename({ postfix: '.csv' });
      debug(`Writing timeline file:${timelineFile}`);
      const timelineOutputStream = new Readable({
        objectMode: true,
      });
      // eslint-disable-next-line no-underscore-dangle
      timelineOutputStream._read = () => {};

      const timelineOutputTransform = new Transform({
        objectMode: true,
        transform(obj, enc, cb) {
          this.push({
            uuid: uuidv7(),
            entry_type: obj.entry_type || 'UNKNOWN',
            person_id: obj.person_id || 0,
            reference_id: obj.reference_id || 0,
          });
          cb();
        },
      });

      const writeStream = fs.createWriteStream(timelineFile);
      const finishWritingTimelinePromise = new Promise((resolve, reject) => {
        writeStream.on('finish', () => {
          resolve();
        }).on('error', (err) => {
          reject(err);
        });
      });

      timelineOutputStream
        .pipe(timelineOutputTransform)
        .pipe(csv.stringify({ header: true }))
        .pipe(writeStream);

      this.outputStream = {
        stream: timelineOutputStream,
        promises: [finishWritingTimelinePromise],
        files: [timelineFile],
      };
      return this.outputStream;
    });
  }

  async process({
    packet,
    filename,
    transform: userTransform,
    batchSize = 500,
    bindings = {},
  }) {
    let inStream = null;

    if (filename) {
      inStream = fs.createReadStream(filename);
    } else if (packet) {
      inStream = (await stream({ packet, type: 'person' })).stream;
    }
    if (typeof userTransform !== 'function') throw new Error('transform function is required');

    const concurrency = 10;

    let timelineFiles = [];

    const transformArguments = {};
    // An array of promises that must be completed, such as writing to disk
    let bindingPromises = [];

    // new Streams may be created, and they have to be completed when the file is completed
    const newStreams = [];

    const bindingNames = Object.keys(bindings);
    // eslint-disable-next-line no-await-in-loop
    await Promise.all(bindingNames.map(async (bindingName) => {
      const binding = bindings[bindingName];
      if (!binding.path) throw new Error(`path is required for binding ${bindingName}`);
      if (binding.path === 'packet.output.timeline') {
        const { stream: streamImpl, promises, files } = await this.getTimelineOutputStream({});
        newStreams.push(streamImpl);
        transformArguments[bindingName] = streamImpl;
        bindingPromises = bindingPromises.concat(promises || []);
        timelineFiles = timelineFiles.concat(files);
      } else if (binding.type === 'packet.message') {
        transformArguments[bindingName] = await getFile({ packet, type: 'message' });
      } else if (binding.type === 'handlebars') {
        transformArguments[bindingName] = handlebars;
      } else {
        throw new Error(`Unsupported binding type for binding ${bindingName}: ${binding.type}`);
      }
    }));

    await pipeline(
      inStream,
      csv.parse({
        relax: true,
        skip_empty_lines: true,
        max_limit_on_data_read: 10000000,
        columns: true,
      }),
      getBatchTransform({ batchSize }).transform,
      new ParallelStream(
        {
          concurrency,
          async transform(batch, enc, done) {
            await userTransform({ ...transformArguments, batch });
            done();
          },
        },
      ),
    );

    newStreams.forEach((s) => s.push(null));

    return { timelineFiles };
  }
}

module.exports = ForEachEntry;
