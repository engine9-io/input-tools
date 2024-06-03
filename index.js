const fs = require('node:fs');

const path = require('node:path');
const os = require('node:os');
const stream = require('node:stream');

const { finished } = stream.promises;
const { stringify } = require('csv');
const debug = require('debug')('packet-tools');

const unzipper = require('unzipper');
const { uuidv7 } = require('uuidv7');
const archiver = require('archiver');
const handlebars = require('handlebars');
const { mkdirp } = require('mkdirp');
const etl = require('etl');
const JSON5 = require('json5');

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

  const rand = `_${Math.floor(Math.random() * 10000)}`;
  const p = `${dir}/${prefix || ''}${new Date().toISOString().slice(0, -1).replace(/[^0-9]/g, '_')}${rand}${postfix || '.txt'}`;
  return p;
}

async function list(_path) {
  const directory = await unzipper.Open.file(_path);

  return new Promise((resolve, reject) => {
    directory.files[0]
      .stream()
      .pipe(fs.createWriteStream('firstFile'))
      .on('error', (e) => {
        reject(e);
      })
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
async function getManifest({ packet }) {
  if (!packet) throw new Error('no packet option specififed');
  let manifest = {};
  return new Promise((resolve, reject) => {
    fs.createReadStream(path.resolve(process.cwd(), packet))
      .pipe(unzipper.Parse())
      .pipe(etl.map(async (entry) => {
        if (entry.path === 'manifest.json') {
          const content = await entry.buffer();
          manifest = JSON.parse(content);
        } else {
          entry.autodrain();
        }
      }))
      .promise()
      .then(() => resolve(manifest), reject);
  });
}
async function getMessage({ packet }) {
  if (!packet) throw new Error('no packet option specififed');
  const manifest = await getManifest({ packet });
  const messageFiles = manifest.files?.filter((d) => d.type === 'message');
  if (!messageFiles?.length) throw new Error('No message files found in packet');
  if (messageFiles?.length > 1) throw new Error('Multiple message files found in packet');
  const messagePath = messageFiles[0].path;
  let message = {};
  return new Promise((resolve, reject) => {
    fs.createReadStream(path.resolve(process.cwd(), packet))
      .pipe(unzipper.Parse())
      .pipe(etl.map(async (entry) => {
        if (entry.path === messagePath) {
          const content = await entry.buffer();
          message = JSON5.parse(content);
        } else {
          entry.autodrain();
        }
      }))
      .promise()
      .then(() => resolve(message), reject);
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

  const transformArguments = {};
  // An array of promises that must be completed, such as writing to disk
  const bindingPromises = [];

  const bindingNames = Object.keys(bindings);
  // eslint-disable-next-line no-await-in-loop
  await Promise.all(bindingNames.map(async (bindingName) => {
    const binding = bindings[bindingName];
    if (!binding.type) throw new Error(`type is required for binding ${bindingName}`);
    if (binding.type === 'packet-output-timeline') {
      const timelineFile = await getTempFilename({ postfix: '.csv' });
      manifest.newTimelineFile = timelineFile;
      console.log(`Writing timeline file:${timelineFile}`);
      const timelineOutputStream = new stream.Readable({
        objectMode: true,
      });
      // eslint-disable-next-line no-underscore-dangle
      timelineOutputStream._read = () => {};

      transformArguments[bindingName] = timelineOutputStream;
      const timelinePromise = finished(
        timelineOutputStream
          .pipe(new stream.Transform({
            objectMode: true,
            transform(obj) {
              this.push({
                uuid: uuidv7(),
                entry_type: obj.entry_type || 'UNKNOWN',
                person_id: obj.person_id || 0,
                reference_id: obj.reference_id || 0,
              });
            },
          }))
          .pipe(stringify({
            header: true,
          }))
          .pipe(fs.createWriteStream(timelineFile)),
      );

      bindingPromises.push(timelinePromise);
    } else if (binding.type === 'packet-message') {
      transformArguments[bindingName] = getMessage({ packet });
    } else if (binding.type === 'handlebars') {
      transformArguments[bindingName] = handlebars;
    } else {
      throw new Error(`Unsupported binding type for binding ${bindingName}: ${binding.type}`);
    }
  }));

  return new Promise((resolve, reject) => {
    fs.createReadStream(path.resolve(process.cwd(), packet))
      .pipe(unzipper.Parse())

      // we should not return null here, as it will cancel the pipe,
      // so we disable the consistent return
      // eslint-disable-next-line consistent-return
      .pipe(etl.map(async (entry) => {
        if (entry.path === personFile.path) {
          return entry
            .pipe(etl.csv())
            // collect batchSize records at a time for bulk-insert
            .pipe(etl.collect(batchSize))
            // map `date` into a javascript date and set unique _id
            .pipe(etl.map(async function (batch) {
              const out = await transform({ batch, handlebars, ...transformArguments });
              this.push(out);
            }))
            .promise()
            .then(Promise.all(bindingPromises))
            .then(() => {}, reject);
        }
        entry.autodrain();
      }))
      .promise()
      .then(() => resolve(manifest), reject);
  });
}

module.exports = {
  list,
  extract,
  create,
  forEachPerson,
  getManifest,
  getMessage,
};
