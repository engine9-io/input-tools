/* eslint-disable no-await-in-loop */
const fs = require('node:fs');

const fsp = fs.promises;
const path = require('node:path');
const zlib = require('node:zlib');
const {
  Readable, Transform, PassThrough,
} = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { stringify } = require('csv');

const debug = require('debug')('FileWorker');
// const through2 = require('through2');
const csv = require('csv');
const JSON5 = require('json5');// Useful for parsing extended JSON
const languageEncoding = require('detect-file-encoding-and-language');
const S3Worker = require('./S3');
const ParquetWorker = require('./Parquet');
const { streamPacket } = require('./tools');

const { bool, getTempDir } = require('./tools');

function Worker({ accountId }) { this.accountId = accountId; }

class LineReaderTransform extends Transform {
  constructor(options = {}) {
    super({ ...options, readableObjectMode: true });
    this.buffer = '';
  }

  // eslint-disable-next-line no-underscore-dangle
  _transform(chunk, encoding, callback) {
    this.buffer += chunk.toString();
    const lines = this.buffer.split(/\r?\n/);
    this.buffer = lines.pop();
    lines.forEach((line) => this.push(line));
    callback();
  }

  // eslint-disable-next-line no-underscore-dangle
  _flush(callback) {
    if (this.buffer) {
      this.push(this.buffer);
    }
    callback();
  }
}

Worker.prototype.csvToObjectTransforms = function (options) {
  const transforms = [];
  const delimiter = options.delimiter || ',';

  const headerMapping = options.headerMapping || function (d) { return d; };
  let lastLine = null;
  let head = null;

  const skipLinesWithError = bool(options.skip_lines_with_error, false);
  const parserOptions = {
    relax: true,
    skip_empty_lines: true,
    delimiter,
    max_limit_on_data_read: 10000000,
    skip_lines_with_error: skipLinesWithError,
  };
  if (options.skip) parserOptions.from_line = options.skip;
  if (options.relax_column_count) parserOptions.relax_column_count = true;
  if (options.quote_escape) {
    parserOptions.escape = options.quote_escape;
  }

  debug('Parser options=', parserOptions);
  const parser = csv.parse(parserOptions);
  parser.on('error', (error) => {
    debug('fileToObjectStream: Error parsing csv file');
    debug(lastLine);
    throw new Error(error);
  });

  const blankAndHeaderCheck = new Transform({
    objectMode: true,
    transform(row, enc, cb) {
      // Blank rows
      if (row.length === 0) return cb();
      if (row.length === 1 && !row[0]) return cb();

      if (!head) {
        head = row.map(headerMapping);
        return cb();
      }

      const o = {};
      head.forEach((_h, i) => {
        const h = _h.trim();
        if (h) {
          o[h] = row[i];
        }
      });

      lastLine = row.join(delimiter);
      return cb(null, o);
    },
  });

  transforms.push(parser);
  transforms.push(blankAndHeaderCheck);

  return { transforms };
};

Worker.prototype.detectEncoding = async function (options) {
  if (options.encoding_override) return { encoding: options.encoding_override };
  // Limit to only the top N bytes -- for perfomance
  // Be wary, though, as gzip files may require a certain minimum number of bytes to decompress
  const bytes = 64 * 1024;
  const buff = Buffer.alloc(bytes);
  const fd = await fsp.open(options.filename);
  await fd.read(buff, 0, bytes);
  let finalBuff = buff;
  if (options.filename.slice(-3) === '.gz') {
    // This code deals with scenarios where the buffer coming in may not be exactly the gzip
    // needed chunk size.
    finalBuff = await new Promise((resolve, reject) => {
      const bufferBuilder = [];
      const decompressStream = zlib.createGunzip()
        .on('data', (chunk) => {
          bufferBuilder.push(chunk);
        }).on('close', () => {
          resolve(Buffer.concat(bufferBuilder));
        }).on('error', (err) => {
          if (err.errno !== -5) {
            // EOF: expected
            reject(err);
          }
        });
      decompressStream.write(buff);
      decompressStream.end();
    });
  }

  return languageEncoding(finalBuff);
};

Worker.prototype.detectEncoding.metadata = {
  options: {
    filename: { required: true },
  },
};

/*
Internal method to transform a file into a stream of objects.
*/
Worker.prototype.fileToObjectStream = async function (options) {
  const { filename, columns, limit: limitOption } = options;

  // handle stream item
  if (options.stream) {
    if (Array.isArray(options.stream)) {
      return { stream: Readable.from(options.stream) };
    }
    // probably already a stream
    if (typeof options.stream === 'object') return { stream: options.stream };
    throw new Error(`Invalid stream type:${typeof options.stream}`);
  }
  let limit;
  if (limitOption) limit = parseInt(limitOption, 10);
  if (!filename) throw new Error('fileToObjectStream: filename is required');
  let postfix = options.sourcePostfix || filename.toLowerCase().split('.').pop();
  if (postfix === 'zip') {
    debug('Invalid filename:', { filename });
    throw new Error('Cowardly refusing to turn a .zip file into an object stream, turn into a csv first');
  }

  const streamInfo = await this.stream({
    filename,
    columns,
    limit,
  });
  const { encoding } = streamInfo;
  let { stream } = streamInfo;
  if (!stream) throw new Error(`No stream found in fileToObjectStream from filename ${filename}`);
  if (encoding === 'object') {
    // already an object
    return { stream };
  }

  let count = 0;

  debug(`Reading file ${filename} with encoding:`, encoding);

  const head = null;
  let transforms = [];

  if (postfix === 'gz') {
    const gunzip = zlib.createGunzip();
    transforms.push(gunzip);
    gunzip.setEncoding(encoding);
    // encoding = null;// Default encoding
    postfix = filename.toLowerCase().split('.');
    postfix = postfix[postfix.length - 2];
    debug(`Using gunzip parser because postfix is .gz, encoding=${encoding}`);
  } else {
    stream.setEncoding(encoding);
  }

  if (postfix === 'csv') {
    const csvTransforms = this.csvToObjectTransforms({ ...options });
    transforms = transforms.concat(csvTransforms.transforms);
  } else if (postfix === 'txt') {
    const csvTransforms = this.csvToObjectTransforms({ ...options, delimiter: '\t' });
    transforms = transforms.concat(csvTransforms.transforms);
  } else if (postfix === 'jsonl') {
    /* Type of JSON that has the names in an array in the first record,
    and the values in JSON arrays thereafter
    */
    let headers = null;

    const lineReader = new LineReaderTransform();

    const jsonlTransform = new Transform({
      objectMode: true,
      transform(d, enc, cb) {
        if (!d) return cb();
        let obj;
        try {
          obj = JSON5.parse(d);
        } catch (e) {
          debug('Invalid line:');
          debug(d);
          throw e;
        }
        /* JSONL could potentially start with an array of names,
        in which case we need to map the subsequent values
      */
        if (headers === null) {
          if (Array.isArray(obj)) {
            headers = obj;
            return cb();
          }
          headers = false;
        }
        if (headers) {
          const mapped = {};
          headers.forEach((name, i) => { mapped[name] = obj[i]; });
          this.push(mapped);
        } else {
          this.push(obj);
        }
        return cb();
      },
    });

    transforms.push(lineReader);
    transforms.push(jsonlTransform);
  } else {
    throw new Error(`Unsupported file type: ${postfix}`);
  }
  const countAndDebug = new Transform({
    objectMode: true,
    transform(d, enc, cb) {
      if (count === 0) { debug('Sample object from file:', d); }
      count += 1;
      if ((count < 5000 && count % 1000 === 0) || (count % 50000 === 0)) {
        debug(`fileToObjectStream transformed ${count} lines`);
      }
      this.push(d);
      cb();
    },
    flush(cb) {
      // If there's no records at all, push a dummy record, and specify 0 records
      debug(`Completed reading file, records=${count}`);
      if (count === 0) {
        const o = { _is_placeholder: true };

        if (head) head.forEach((c) => { o[c] = null; });
        this.push(o);
      }
      cb();
    },
  });

  transforms.push(countAndDebug);
  transforms.forEach((t) => {
    stream = stream.pipe(t);
  });

  return { stream };
};
Worker.prototype.getFileWriterStream = async function (options = {}) {
  const accountId = options.accountId || this.accountId;
  if (!accountId) throw new Error('getFileWriterStream has no accountId');
  const targetFormat = options.targetFormat || 'csv';
  const tempDir = await getTempDir({ accountId });
  let { fileExtendedType } = options;
  if (fileExtendedType) fileExtendedType += '.';
  else fileExtendedType = '';
  // So, this could change, but it's easier to read
  // dates in a filename than UUIDs, so this is
  // a unique-ish filename generator
  const uniqueNumberedDate = `${new Date().toISOString().replace(/[^0-9]*/g, '')}.${Math.floor(Math.random() * 1000)}`;
  let filename = `${tempDir}${path.sep}${uniqueNumberedDate}.${fileExtendedType}${targetFormat}`;
  if (bool(options.gzip, false)) filename += '.gz';
  const stream = fs.createWriteStream(filename);
  debug('FileWriterStream writing to file ', filename);

  return { filename, stream };
};

Worker.prototype.getOutputStreams = async function (options) {
  const { filename, stream: fileWriterStream } = await this.getFileWriterStream(options);

  let { transform } = options;
  if (typeof options.transform === 'function') {
    if (options.transform.length === 3) {
      transform = new Transform({
        objectMode: true,
        async transform(item, encoding, cb) {
          options.transform(item, encoding, cb);
        },
      });
    } else {
      transform = new Transform({
        objectMode: true,
        async transform(item, encoding, cb) {
          cb(null, options.transform(item));
        },
      });
    }
  } else if (options.transform) {
    transform = options.transform;
  }
  const { flatten } = options;
  let flattenTransform = null;

  if (bool(flatten, false)) {
    flattenTransform = new Transform({
      objectMode: true,
      async transform(item, enc, cb) {
        // first item establishes the keys to use
        let o = {};
        Object.keys(item).forEach((k) => {
          let v = item[k];
          if (!o[k]) {
            if (typeof v === 'object') {
              while (Array.isArray(v)) [v] = v;// get first array item
              o = { ...o, ...v };
            } else {
              o[k] = v;
            }
          }
        });
        cb(null, o);
      },
    });
  }

  const stats = {
    records: 0,
  };
  let stringifier;
  if (options.targetFormat === 'jsonl') {
    stringifier = new Transform({
      objectMode: true,
      transform(d, encoding, cb) {
        cb(false, `${JSON.stringify(d)}\n`);
      },
    });
  } else {
    stringifier = stringify({ header: true });
  }
  let gzip = new PassThrough();
  if (options.gzip) {
    gzip = zlib.createGzip();
  }
  const streams = [
    transform,
    flattenTransform,
    new Transform({
      objectMode: true,
      transform(d, enc, cb) {
        stats.records += 1;
        cb(null, d);
      },
    }),
    stringifier,
    gzip,
    fileWriterStream,
  ].filter(Boolean);
  return { filename, streams, stats };
};
Worker.prototype.objectStreamToFile = async function (options) {
  const { filename, streams, stats } = await this.getOutputStreams(options);
  const { stream: inStream } = options;
  streams.unshift(inStream);
  await pipeline(
    streams,
  );
  return { filename, records: stats.records };
};

Worker.prototype.transform = async function (options) {
  const worker = this;

  const filename = worker.getFilename(options);

  debug(`Transforming ${filename}`);

  options.filename = filename;
  let { stream } = await worker.fileToObjectStream(options);
  if (typeof stream.pipe !== 'function') {
    debug(stream);
    throw new Error('No pipe in stream');
  }

  let t = options.transform;

  // No longer need this
  delete options.transform;
  if (!t) {
    t = function (d, enc, cb) {
      d.is_test_transform = true;
      cb(null, d);
    };
  }

  if (!Array.isArray(t)) t = [t];
  Object.keys(t).forEach((key) => {
    let f = t[key];
    if (typeof f === 'function') {
      f = new Transform({
        objectMode: true,
        transform: f,
      });
    }

    stream = stream.pipe(f);
  });

  const { targetFormat } = options;

  if (!targetFormat && (filename.toLowerCase().slice(-4) === '.csv' || filename.toLowerCase().slice(-7) === '.csv.gz')) {
    options.targetFormat = 'csv';
  }

  return worker.objectStreamToFile({ ...options, stream });
};

Worker.prototype.transform.metadata = {
  options: {
    sourcePostfix: { description: "Override the source postfix, if for example it's a csv" },
    encoding: { description: 'Manual override of source file encoding' },
    names: { description: 'Target field names (e.g. my_new_field,x,y,z)' },
    values: { description: "Comma delimited source field name, or Handlebars [[ ]] merge fields (e.g. 'my_field,x,y,z', '[[field1]]-[[field2]]', etc)" },
    targetFilename: { description: 'Custom name of the output file (default auto-generated)' },
    targetFormat: { description: 'Output format -- csv supported, or none for txt (default)' },
    targetRowDelimiter: { description: 'Row delimiter (default \n)' },
    targetFieldDelimiter: { description: 'Field delimiter (default \t or ,)' },
  },
};
Worker.prototype.testTransform = async function (options) {
  return this.transform({
    ...options,
    transform(d, enc, cb) { d.transform_time = new Date(); cb(null, d); },
  });
};
Worker.prototype.testTransform.metadata = {
  options: {
    filename: true,
  },
};

/* Get a stream from an actual stream, or an array, or a file*/
Worker.prototype.stream = async function (
  options,
) {
  const {
    stream: inputStream, packet, type, columns, limit,
    filename: filenameOpt,
  } = options;
  let filename = filenameOpt;

  if (inputStream) {
    if (Array.isArray(inputStream)) {
      return { stream: Readable.from(inputStream) };
    }
    // probably already a stream
    if (typeof inputStream === 'object') return { stream: inputStream, encoding: 'object' };
    throw new Error(`Invalid stream type:${typeof inputStream}`);
  } else if (filename) {
    if (filename.startsWith('engine9-accounts/')) {
      filename = `${process.env.ENGINE9_ACCOUNT_DIR}/${filename.slice('engine9-accounts/'.length)}`;
      debug(`Prepending file with ${process.env.ENGINE9_ACCOUNT_DIR}, filename=${filename}`);
    } else {
      debug(`Not prepending filename:${filename}`);
    }
    let encoding; let stream;
    if (filename.slice(-8) === '.parquet') {
      const pq = new ParquetWorker(this);
      stream = (await pq.stream({ filename, columns, limit })).stream;
      encoding = 'object';
    } else if (filename.indexOf('s3://') === 0) {
      const s3Worker = new S3Worker(this);
      stream = (await s3Worker.stream({ filename, columns, limit })).stream;
      encoding = 'UTF-8';
    } else {
      // Check if the file exists, and fast fail if not
      // Otherwise the stream hangs out as a handle
      try {
        await fsp.stat(filename);
      } catch (e) {
        debug(`Error reading file ${filename}, current directory: ${process.cwd()},__dirname:${__dirname}`);
        throw e;
      }
      stream = fs.createReadStream(filename);
      encoding = (await this.detectEncoding({ filename })).encoding;
    }
    return { stream, encoding };
  } else if (packet) {
    let { stream: packetStream } = await streamPacket({ packet, type, limit });
    const { transforms } = this.csvToObjectTransforms({});
    transforms.forEach((t) => {
      packetStream = packetStream.pipe(t);
    });
    return { stream: packetStream };
  } else {
    throw new Error('stream must be passed a stream, filename, or packet');
  }
};

Worker.prototype.sample = async function (opts) {
  opts.limit = opts.limit || 10;
  const { stream } = await this.fileToObjectStream(opts);
  return stream.toArray();
};
Worker.prototype.sample.metadata = {
  options: {
    filename: {},

  },
};

Worker.prototype.write = async function (opts) {
  const { filename, content } = opts;
  if (filename.indexOf('s3://') === 0) {
    const s3Worker = new S3Worker(this);
    const parts = filename.split('/');
    const directory = parts.slice(0, -1).join('/');
    const file = parts.slice(-1)[0];
    // debug(JSON.stringify({ parts, directory, file }));
    await s3Worker.write({
      directory,
      file,
      content,
    });
  } else {
    await fsp.writeFile(filename, content);
  }
  return { success: true, filename };
};
Worker.prototype.write.metadata = {
  options: {
    filename: { description: 'Location to write content to, can be local or s3://' },
    content: {},
  },
};

async function streamToString(stream) {
  // lets have a ReadableStream as a stream variable
  const chunks = [];

  // eslint-disable-next-line no-restricted-syntax
  for await (const chunk of stream) {
    chunks.push(Buffer.from(chunk));
  }

  return Buffer.concat(chunks).toString('utf-8');
}
/*
Retrieves and parsed
*/
Worker.prototype.json = async function (opts) {
  const { stream } = await this.stream(opts);
  const str = await streamToString(stream);
  try {
    return JSON5.parse(str);
  } catch (e) {
    debug(e);
    throw new Error(`Unparseable JSON received: ${opts.filename || '(native stream)'}`);
  }
};
Worker.prototype.json.metadata = {
  options: {
    filename: { description: 'Get a javascript object from a file' },
  },
};

Worker.prototype.list = async function ({ directory }) {
  if (!directory) throw new Error('directory is required');
  if (directory.indexOf('s3://') === 0) {
    const s3Worker = new S3Worker(this);
    return s3Worker.list({ directory });
  }
  const a = await fsp.readdir(directory, { withFileTypes: true });
  return a.map((f) => ({
    name: f.name,
    type: f.isDirectory() ? 'directory' : 'file',
  }));
};
Worker.prototype.list.metadata = {
  options: {
    directory: { required: true },
  },
};

Worker.prototype.listAll = async function ({ directory }) {
  if (!directory) throw new Error('directory is required');
  if (directory.indexOf('s3://') === 0) {
    const s3Worker = new S3Worker(this);
    return s3Worker.listAll({ directory });
  }
  const a = await fsp.readdir(directory, { recursive: true });

  return a.map((f) => `${directory}/${f}`);
};
Worker.prototype.listAll.metadata = {
  options: {
    directory: { required: true },
  },
};

Worker.prototype.empty = async function ({ directory }) {
  if (!directory) throw new Error('directory is required');
  if (directory.indexOf('s3://') === 0) {
    // currently not emptying S3 this way -- dangerous
    throw new Error('Cannot empty an s3:// directory');
  }
  const removed = [];
  // eslint-disable-next-line no-restricted-syntax
  for (const file of await fsp.readdir(directory)) {
    removed.push(file);
    await fsp.unlink(path.join(directory, file));
  }
  return { directory, removed };
};
Worker.prototype.empty.metadata = {
  options: {
    directory: { required: true },
  },
};

Worker.prototype.move = async function ({ filename, target }) {
  if (!target) throw new Error('directory is required');
  if (target.indexOf('s3://') === 0) {
    const s3Worker = new S3Worker(this);
    const parts = target.split('/');
    return s3Worker.put({ filename, directory: parts.slice(0, -1).join('/'), file: parts.slice(-1)[0] });
  }
  await fsp.mkdir(path.dirname(target), { recursive: true });
  await fsp.rename(filename, target);
  return { filename: target };
};
Worker.prototype.move.metadata = {
  options: {
    filename: {},
    target: {},
  },
};

Worker.prototype.stat = async function ({ filename }) {
  if (!filename) throw new Error('filename is required');
  if (filename.indexOf('s3://') === 0) {
    const s3Worker = new S3Worker(this);
    return s3Worker.stat({ filename });
  }
  const {
    ctime,
    birthtime,
    size,
  } = await fsp.stat(filename);
  const modifiedAt = new Date(ctime);
  let createdAt = birthtime;
  if (createdAt === 0 || !createdAt) createdAt = ctime;
  createdAt = new Date(createdAt);
  return {
    createdAt,
    modifiedAt,
    size,
  };
};
Worker.prototype.stat.metadata = {
  options: {
    filename: {},
  },
};

Worker.prototype.head = async function (options) {
  const { stream } = await this.fileToObjectStream(options);
  const chunks = [];

  const limit = options.limit || 3;
  let counter = 0;
  // eslint-disable-next-line no-restricted-syntax
  for await (const chunk of stream) {
    chunks.push(chunk);
    counter += 1;
    if (counter >= limit) break;
  }

  return chunks;
};

Worker.prototype.head.metadata = {
  options: {
    filename: { required: true },
  },
};

Worker.prototype.count = async function (options) {
  const { stream } = await this.fileToObjectStream(options);
  const sample = [];

  const limit = options.limit || 5;
  let records = 0;
  // eslint-disable-next-line no-restricted-syntax
  for await (const chunk of stream) {
    records += 1;
    if (records < limit) {
      sample.push(chunk);
    }
  }

  return { sample, records };
};

Worker.prototype.count.metadata = {
  options: {
    filename: { required: true },
  },
};

module.exports = Worker;
