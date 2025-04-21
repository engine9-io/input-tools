const fs = require('node:fs');

const { Writable } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { throttle } = require('throttle-debounce');
const parallelTransform = require('parallel-transform');

const debug = require('debug')('packet-tools');

const debugThrottle = throttle(1000, debug, { noLeading: false, noTrailing: false });

const { Mutex } = require('async-mutex');

const csv = require('csv');

const handlebars = require('handlebars');
const ValidatingReadable = require('./ValidatingReadable');

const {
  getTempFilename, getBatchTransform, getFile, stream,
} = require('./file/tools');

class ForEachEntry {
  constructor() {
    this.timelineOutputMutex = new Mutex();
  }

  getTimelineOutputStream() {
    return this.timelineOutputMutex.runExclusive(async () => {
      if (this.outputStream) return this.outputStream;
      const timelineFile = await getTempFilename({ postfix: '.csv' });
      debug(`Timeline output requested, writing timeline file to: ${timelineFile}`);
      const timelineOutputStream = new ValidatingReadable({
        objectMode: true,
      }, (data) => {
        if (!data) return true;
        if (typeof data !== 'object') throw new Error('Invalid timeline data push, must be an object');
        // Is this necessary?
        if (!data.person_id) throw new Error('Invalid timeline data push, must have a person_id, even if 0');
        if (!data.ts) data.ts = new Date().toISOString();
        return true;
      });
      // eslint-disable-next-line no-underscore-dangle
      timelineOutputStream._read = () => {};

      const writeStream = fs.createWriteStream(timelineFile);
      const finishWritingTimelinePromise = new Promise((resolve, reject) => {
        writeStream.on('finish', () => {
          resolve();
        }).on('error', (err) => {
          reject(err);
        });
      });

      timelineOutputStream
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
    concurrency = 10,
    bindings = {},
  }) {
    let inStream = null;

    if (filename) {
      debug(`Processing file ${filename}`);
      inStream = fs.createReadStream(filename);
    } else if (packet) {
      debug(`Processing person file from packet ${packet}`);
      inStream = (await stream({ packet, type: 'person' })).stream;
    }
    if (typeof userTransform !== 'function') throw new Error('async transform function is required');
    if (userTransform.length > 1) throw new Error('transform should be an async function that accepts one argument');

    let records = 0;
    let batches = 0;

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
      if (!binding.path) throw new Error(`Invalid binding: path is required for binding ${bindingName}`);
      if (binding.path === 'output.timeline') {
        const { stream: streamImpl, promises, files } = await this.getTimelineOutputStream({});
        newStreams.push(streamImpl);
        transformArguments[bindingName] = streamImpl;
        bindingPromises = bindingPromises.concat(promises || []);
        timelineFiles = timelineFiles.concat(files);
      } else if (binding.path === 'file') {
        transformArguments[bindingName] = await getFile(binding);
      } else if (binding.path === 'handlebars') {
        transformArguments[bindingName] = handlebars;
      } else {
        throw new Error(`Unsupported binding path for binding ${bindingName}: ${binding.path}`);
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
      parallelTransform(
        concurrency,
        (batch, cb) => {
          userTransform({ ...transformArguments, batch })
            .then((d) => cb(null, d))
            .catch(cb);
        },

      ),
      new Writable({
        objectMode: true,
        write(batch, enc, cb) {
          batches += 1;
          records += batch?.length || 0;

          debugThrottle(`Processed ${batches} batches for a total of ${records} records`);
          cb();
        },
      }),
    );
    debug('Completed all batches');

    newStreams.forEach((s) => s.push(null));

    return { timelineFiles };
  }
}

module.exports = ForEachEntry;
