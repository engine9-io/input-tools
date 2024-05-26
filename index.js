const fs = require('node:fs');
const path = require('node:path');
const debug = require('debug')('packet-tools');
const os = require('node:os');
const unzipper = require('unzipper');
const archiver = require('archiver');
const { mkdirp } = require('mkdirp');
const etl = require('etl');

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
      item.originalFilename = path.resolve(__dirname, p);
    } else {
      item.originalFilename = path.resolve(__dirname, item.originalFilename);
    }
    const file = item.originalFilename.split(path.sep).pop();
    const fileParts = file.split('.');
    if (fileParts[fileParts.length - 2] === type) {
      item.path = file;
    } else {
      fileParts.splice(-1, 0, type);
      item.path = fileParts.join('.');
    }
    const existingFile = existingFiles.find((f) => f.path === item.path);
    if (existingFile) throw new Error('Error adding files, duplicate path found for path:', +item.path);
    existingFiles.push(item);
  });
}

async function create(options) {
  const {
    accountId = 'engine9',
    pluginId = '',
    messageFiles = [], // file with contents of message, used for delivery
    personFiles = [], // files with data on people
    timelineFiles = [], // activity entry
    statisticsFiles = [], // files with aggregate statistics
  } = options;

  const files = [];
  const dateCreated = new Date().toISOString();
  appendFiles(files, messageFiles, { type: 'message', dateCreated });
  appendFiles(files, personFiles, { type: 'person', dateCreated });
  appendFiles(files, timelineFiles, { type: 'timeline', dateCreated });
  appendFiles(files, statisticsFiles, { type: 'statistics', dateCreated });

  const zipFilename = await getTempFilename({ postfix: '.packet.zip' });
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
    // listen for all archive data to be written
    // 'close' event is fired only when a file descriptor is involved
    output.on('close', () => {
      debug('archiver has been finalized and the output file descriptor has closed, calling success');
      debug(zipFilename);
      return resolve(null, {
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
  let manifest = {};
  return new Promise((resolve, reject) => {
    fs.createReadStream(path.resolve(__dirname, packet))
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
/*
fs.createReadStream('scores.csv')
  // parse the csv file
  .pipe(etl.csv())
  // map `date` into a javascript date and set unique _id
  .pipe(etl.map(d => {
    d._id = d.person_id;
    d.date = new Date(d.date);
    return d;
  }))
  // collect 1000 records at a time for bulk-insert
  .pipe(etl.collect(1000))
  // upsert records to elastic with max 10 concurrent server requests
  .pipe(etl.elastic.index(esClient,'scores','records',{concurrency:10}))
  // Switch from stream to promise chain and report done or error
  .promise()
  .then( () => console.log('done'), e => console.log('error',e));
*/

async function forEachPerson({
  packet,
  transform,
  // batchSize = 500,
  // bindings = {},
}) {
  if (!packet) throw new Error('no packet specified');
  if (typeof transform !== 'function') throw new Error('transform function is required');
  /*

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
  */
}

module.exports = {
  list,
  extract,
  create,
  forEachPerson,
  getManifest,
};
