const fs = require('node:fs');

const path = require('node:path');
const os = require('node:os');
const stream = require('node:stream');

const { stringify } = require('csv');
const debug = require('debug')('packet-tools');
const progress = require('debug')('info:packet-tools');

const unzipper = require('unzipper');
const { v4: uuidv4, v5: uuidv5, v7: uuidv7 } = require('uuid');
const archiver = require('archiver');
const handlebars = require('handlebars');
const { mkdirp } = require('mkdirp');
const etl = require('etl');
const JSON5 = require('json5');
const {
  S3Client,
  HeadObjectCommand,
  GetObjectCommand,
} = require('@aws-sdk/client-s3');

const { TIMELINE_ENTRY_TYPES } = require('./timelineTypes');

function getStringArray(s, nonZeroLength) {
  let a = s || [];
  if (typeof a === 'number') a = String(a);
  if (typeof a === 'string') a = [a];

  if (typeof s === 'string') a = s.split(',');
  a = a.map((x) => x.toString().trim()).filter(Boolean);
  if (nonZeroLength && a.length === 0) a = [0];
  return a;
}

async function getTempDir({ accountId = 'engine9' }) {
  const dir = [os.tmpdir(), accountId, new Date().toISOString().substring(0, 10)].join(path.sep);
  try {
    await mkdirp(dir);
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }
  return dir;
}
/*
 Get a new, timestamp based filename, creating any necessary directories
 options:
 prefix/postfix of file
 source:source file, used to generate friendly name
*/
async function getTempFilename(options) {
  let dir = await getTempDir(options);

  const target = options.targetFilename;
  if (target) {
    if (target.indexOf('/') === 0 || target.indexOf('\\') === 0) {
      // assume a full directory path has been specified
      return target;
    }

    // make a distinct directory, so we don't overwrite the file
    dir = `${dir}/${new Date().toISOString().slice(0, -6).replace(/[^0-9]/g, '_')}`;

    const newDir = await mkdirp(dir);

    return `${newDir}/${target}`;
  }
  let { prefix } = options;
  let { postfix } = options;
  const targetFormat = options.target_format || options.targetFormat;
  if (!postfix && targetFormat === 'csv') postfix = '.csv';
  if (options.source) {
    postfix = `_${options.source.split('/').pop()}`;
    postfix = postfix.replace(/['"\\]/g, '').replace(/[^a-zA-Z0-9_.-]/g, '_');
  }

  if (prefix) prefix += '_';

  const p = `${dir}/${prefix || ''}${uuidv7()}${postfix || '.txt'}`;
  return p;
}

async function list(_path) {
  const directory = await unzipper.Open.file(_path);

  return new Promise((resolve, reject) => {
    directory.files[0]
      .stream()
      .pipe(fs.createWriteStream('firstFile'))
      .on('error', reject)
      .on('finish', resolve);
  });
}

async function extract(_path, _file) {
  const directory = await unzipper.Open(_path);
  // return directory.files.map((f) => f.path);
  const file = directory.files.find((d) => d.path === _file);
  const tempFilename = await getTempFilename({ source: _file });
  return new Promise((resolve, reject) => {
    file
      .stream()
      .pipe(fs.createWriteStream(tempFilename))
      .on('error', reject)
      .on('finish', resolve);
  });
}

function appendFiles(existingFiles, _newFiles, options) {
  const newFiles = getStringArray(_newFiles);
  if (newFiles.length === 0) return;
  let { type, dateCreated } = options || {};
  if (!type) type = 'unknown';
  if (!dateCreated)dateCreated = new Date().toISOString();
  let arr = newFiles;
  if (!Array.isArray(newFiles)) arr = [arr];

  arr.forEach((p) => {
    const item = {
      type,
      originalFilename: '',
      isNew: true,
      dateCreated,
    };

    if (typeof p === 'string') {
      item.originalFilename = path.resolve(process.cwd(), p);
    } else {
      item.originalFilename = path.resolve(process.cwd(), item.originalFilename);
    }

    const file = item.originalFilename.split(path.sep).pop();
    item.path = `${type}/${file}`;
    const existingFile = existingFiles.find((f) => f.path === item.path);
    if (existingFile) throw new Error('Error adding files, duplicate path found for path:', +item.path);
    existingFiles.push(item);
  });
}

async function create(options) {
  const {
    accountId = 'engine9',
    pluginId = '',
    target = '', // target filename, creates one if not specified
    messageFiles = [], // file with contents of message, used for delivery
    personFiles = [], // files with data on people
    timelineFiles = [], // activity entry
    statisticsFiles = [], // files with aggregate statistics
  } = options;
  if (options.peopleFiles) throw new Error('Unknown option: peopleFiles, did you mean personFiles?');

  const files = [];
  const dateCreated = new Date().toISOString();
  appendFiles(files, messageFiles, { type: 'message', dateCreated });
  appendFiles(files, personFiles, { type: 'person', dateCreated });
  appendFiles(files, timelineFiles, { type: 'timeline', dateCreated });
  appendFiles(files, statisticsFiles, { type: 'statistics', dateCreated });

  const zipFilename = target || await getTempFilename({ postfix: '.packet.zip' });

  const manifest = {
    accountId,
    source: {
      pluginId,
    },
    dateCreated,
    files,
  };

  // create a file to stream archive data to.
  const output = fs.createWriteStream(zipFilename);
  const archive = archiver('zip', {
    zlib: { level: 9 }, // Sets the compression level.
  });
  return new Promise((resolve, reject) => {
    debug(`Setting up write stream to ${zipFilename}`);
    // listen for all archive data to be written
    // 'close' event is fired only when a file descriptor is involved
    output.on('close', () => {
      debug('archiver has been finalized and the output file descriptor has closed, calling success');
      debug(zipFilename);
      return resolve({
        filename: zipFilename,
        bytes: archive.pointer(),
      });
    });

    // This event is fired when the data source is drained no matter what was the data source.
    // It is not part of this library but rather from the NodeJS Stream API.
    // @see: https://nodejs.org/api/stream.html#stream_event_end
    output.on('end', () => {
      // debug('end event -- Data has been drained');
    });

    // warnings could be file not founds, etc, but we error even on those
    archive.on('warning', (err) => {
      reject(err);
    });

    // good practice to catch this error explicitly
    archive.on('error', (err) => {
      reject(err);
    });

    archive.pipe(output);

    files.forEach(({ path: name, originalFilename }) => archive.file(originalFilename, { name }));
    files.forEach((f) => {
      delete f.originalFilename;
      delete f.isNew;
    });

    archive.append(Buffer.from(JSON.stringify(manifest, null, 4), 'utf8'), { name: 'manifest.json' });
    archive.finalize();
  });
}

async function getPacketDirectory({ packet }) {
  if (packet.indexOf('s3://') === 0) {
    const parts = packet.split('/');
    const Bucket = parts[2];
    const Key = parts.slice(3).join('/');
    const s3Client = new S3Client({});

    debug('Getting ', { Bucket, Key });

    //    const directory = await unzipper.Open.s3(s3Client, { Bucket, Key });
    let size = null;
    const directory = await unzipper.Open.custom({
      async size() {
        const info = await s3Client.send(
          new HeadObjectCommand({
            Bucket,
            Key,
          }),
        );
        size = info.ContentLength;
        progress(`Retrieving file of size ${size / (1024 * 1024)} MB`);
        return info.ContentLength;
      },

      stream(offset, length) {
        const ptStream = new stream.PassThrough();
        s3Client.send(
          new GetObjectCommand({
            Bucket,
            Key,
            Range: `bytes=${offset}-${length ?? ''}`,
          }),
        )
          .then((response) => {
            response.Body.pipe(ptStream);
          })
          .catch((error) => {
            ptStream.emit('error', error);
          });

        return ptStream;
      },
    });

    return directory;
  }
  const directory = await unzipper.Open.file(packet);
  return directory;
}

async function getManifest({ packet }) {
  if (!packet) throw new Error('no packet option specififed');
  const directory = await getPacketDirectory({ packet });
  const file = directory.files.find((d) => d.path === 'manifest.json');
  const content = await file.buffer();
  const manifest = JSON.parse(content.toString());
  return manifest;
}

async function getFile({ packet, type }) {
  if (!packet) throw new Error('no packet option specififed');
  const manifest = await getManifest({ packet });
  const files = manifest.files?.filter((d) => d.type === type);
  if (!files?.length) throw new Error(`No files of type ${type} found in packet`);
  if (files?.length > 1) throw new Error(`Multiple files of type ${type} found in packet`);
  const filePath = files[0].path;
  const directory = await getPacketDirectory({ packet });
  const handle = directory.files.find((d) => d.path === filePath);

  const content = await handle.buffer().toString();
  if (filePath.slice(-5) === '.json' || filePath.slice(-6) === '.json5') {
    try {
      return JSON5.parse(content);
    } catch (e) {
      debug(`Erroring parsing json content from ${path}`, content);
      throw e;
    }
  }
  return content;
}

async function getStream({ packet, type }) {
  if (!packet) throw new Error('no packet option specififed');
  const manifest = await getManifest({ packet });
  const files = manifest.files?.filter((d) => d.type === type);
  if (!files?.length) throw new Error(`No files of type ${type} found in packet`);
  if (files?.length > 1) throw new Error(`Multiple files of type ${type} found in packet`);
  const filePath = files[0].path;
  const directory = await getPacketDirectory({ packet });
  const handle = directory.files.find((d) => d.path === filePath);
  return { stream: handle.stream(), path: filePath };
}

async function downloadFile({ packet, type = 'person' }) {
  const { stream: fileStream, path: filePath } = await getStream({ packet, type });
  const filename = await getTempFilename({ targetFilename: filePath.split('/').pop() });

  return new Promise((resolve, reject) => {
    fileStream.pipe(fs.createWriteStream(filename))
      .on('error', reject)
      .on('finish', () => {
        resolve({ filename });
      });
  });
}

async function getTimelineOutputStream() {
  const timelineFile = await getTempFilename({ postfix: '.csv' });
  debug(`Writing timeline file:${timelineFile}`);
  const timelineOutputStream = new stream.Readable({
    objectMode: true,
  });
  // eslint-disable-next-line no-underscore-dangle
  timelineOutputStream._read = () => {};

  const timelineOutputTransform = new stream.Transform({
    objectMode: true,
    transform(obj, enc, cb) {
      debug(`Pushing person_id ${obj.person_id}`, enc, cb);
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
    .pipe(stringify({ header: true }))
    .pipe(writeStream);

  return {
    stream: timelineOutputStream,
    promises: [finishWritingTimelinePromise],
    files: [timelineFile],
  };
}

async function forEachPersonImpl({
  packet,
  transform,
  batchSize = 500,
  bindings = {},
  start = 0, // which record to start with, defaults to 0
  end, // record to end with, non-inclusive
}) {
  const manifest = await getManifest({ packet });
  const personFile = (manifest.files || []).find((p) => p.type === 'person');
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
    if (!binding.type) throw new Error(`type is required for binding ${bindingName}`);
    if (binding.type === 'packet.output.timeline') {
      const { stream: streamImpl, promises, files } = await getTimelineOutputStream({});
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
  let recordCounter = 0;

  return new Promise((resolve, reject) => {
    fs.createReadStream(path.resolve(process.cwd(), packet))
      .pipe(unzipper.Parse())

      // we should not return null here, as it will cancel the pipe,
      // so we disable the consistent-return rule
      // eslint-disable-next-line consistent-return
      .pipe(etl.map(async (entry) => {
        if (entry.path === personFile.path) {
          return entry
            .pipe(etl.csv())
            // eslint-disable-next-line array-callback-return
            .pipe(etl.map(function (item) {
              if (recordCounter < start) return;
              if (end && recordCounter >= end) return;
              recordCounter += 1;
              this.push(item);
            }))
            .pipe(etl.collect(batchSize))
            .pipe(etl.map(async function (batch) {
              const out = await transform({ batch, handlebars, ...transformArguments });
              this.push(out);
            }))
            .promise()
          // .then(Promise.all(bindingPromises));
            .then(() => {}, reject);
        }
        entry.autodrain();
        // don't return null, as it will cancel the pipe
      }))
      .promise()
      .then(() => {
        // close new streams
        newStreams.forEach((s) => s.push(null));

        resolve({ timelineFiles });
      }, reject);
  });
}

async function forEachPerson({
  packet,
  transform,
  batchSize = 500,
  bindings = {},
}) {
  if (!packet) throw new Error('no packet specified');
  if (typeof transform !== 'function') throw new Error('transform function is required');
  const manifest = await getManifest({ packet });
  const personFile = (manifest.files || []).find((p) => p.type === 'person');
  if (!personFile) {
    return { no_data: true, no_person_file: true };
  }
  const totalPersonRecords = 1000000;
  const maxRecordsPerProcess = 1000000;
  const parallelItems = [];
  for (let start = 0; start < totalPersonRecords; start += maxRecordsPerProcess) {
    let end = start + maxRecordsPerProcess;
    if (end > totalPersonRecords) end = totalPersonRecords;
    parallelItems.push(forEachPersonImpl({
      packet, transform, batchSize, bindings, start, end,
    }));
  }

  const results = await Promise.all(parallelItems);
  return results;
}

function intToByteArray(_v) {
  // we want to represent the input as a 8-bytes array
  const byteArray = [0, 0, 0, 0, 0, 0, 0, 0];
  let v = _v;
  for (let index = 0; index < byteArray.length; index += 1) {
    // eslint-disable-next-line no-bitwise
    const byte = v & 0xff;
    byteArray[index] = byte;
    v = (v - byte) / 256;
  }

  return byteArray;
}

function getPluginUUID(uniqueNamespaceLikeDomainName, valueWithinNamespace) {
  return uuidv5(valueWithinNamespace, uniqueNamespaceLikeDomainName);
}

function getInputUUID(pluginId, remoteInputId) {
  if (!pluginId) throw new Error('getInputUUID: Cowardly rejecting a blank plugin_id');
  if (!remoteInputId) throw new Error('getInputUUID: Cowardly rejecting a blank remote_input_id, set a default');
  // Random custom namespace for inputs -- not secure, just a namespace:
  // 3d0e5d99-6ba9-4fab-9bb2-c32304d3df8e
  return uuidv5(`${pluginId}:${remoteInputId}`, '3d0e5d99-6ba9-4fab-9bb2-c32304d3df8e');
}

function getUUIDv7(date, inputUuid) { /* optional date and input UUID */
  const uuid = inputUuid || uuidv7();
  const bytes = Buffer.from(uuid.replace(/-/g, ''), 'hex');
  if (date !== undefined) {
    const d = new Date(date);
    // isNaN behaves differently than Number.isNaN -- we're actually going for the
    // attempted conversion here
    // eslint-disable-next-line no-restricted-globals
    if (isNaN(d)) throw new Error(`getUUIDv7 got an invalid date:${date || '<blank>'}`);
    const dateBytes = intToByteArray(d.getTime()).reverse();
    dateBytes.slice(2, 8).forEach((b, i) => { bytes[i] = b; });
  }
  return uuidv4({ random: bytes });
}
/* Returns a date from a given uuid (assumed to be a v7, otherwise the results are ... weird */
function getUUIDTimestamp(uuid) {
  const ts = parseInt((`${uuid}`).replace(/-/g, '').slice(0, 12), 16);
  return new Date(ts);
}
const uuidRegex = /^[0-9,a-f]{8}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{4}-[0-9,a-f]{12}$/;

module.exports = {
  list,
  extract,
  create,
  forEachPerson,
  getManifest,
  getFile,
  getStream,
  downloadFile,
  getTempFilename,
  getTimelineOutputStream,
  getPacketDirectory,
  getPluginUUID,
  getInputUUID,
  getUUIDv7,
  getUUIDTimestamp,
  uuidRegex,
  TIMELINE_ENTRY_TYPES,
};
