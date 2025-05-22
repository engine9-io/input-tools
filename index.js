const fs = require('node:fs');

const path = require('node:path');

const debug = require('debug')('@engine9-io/input-tools');

const unzipper = require('unzipper');
const {
  v4: uuidv4, v5: uuidv5, v7: uuidv7, validate: uuidIsValid,
} = require('uuid');
const archiver = require('archiver');
const FileUtilities = require('./file/FileUtilities');

const {
  bool,
  getManifest,
  getFile,
  downloadFile,
  getTempFilename,
  streamPacket,
  getPacketFiles,
  getBatchTransform,
  getDebatchTransform,
} = require('./file/tools');

const ForEachEntry = require('./ForEachEntry');

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

/*
  When comparing two objects, some may come from a file (thus strings), and some from
  a database or elsewhere (not strings), so for deduping make sure to make them all strings
*/
function makeStrings(o) {
  return Object.entries(o).reduce((a, [k, v]) => {
    a[k] = (typeof v === 'object') ? JSON.stringify(v) : String(v);
    return a;
  }, {});
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
  // Random custom namespace for plugins -- not secure, just a namespace:
  return uuidv5(`${uniqueNamespaceLikeDomainName}::${valueWithinNamespace}`, 'f9e1024d-21ac-473c-bac6-64796dd771dd');
}

function getInputUUID(a, b) {
  let pluginId = a;
  let remoteInputId = b;
  if (typeof a === 'object') {
    pluginId = a.pluginId;
    remoteInputId = a.remoteInputId;
  }

  if (!pluginId) throw new Error('getInputUUID: Cowardly rejecting a blank plugin_id');
  if (!uuidIsValid(pluginId)) throw new Error(`Invalid pluginId:${pluginId}, should be a UUID`);
  const rid = (remoteInputId || '').trim();
  if (!rid) throw new Error('getInputUUID: Cowardly rejecting a blank remote_input_id, set a default');
  // Random custom namespace for inputs -- not secure, just a namespace:
  // 3d0e5d99-6ba9-4fab-9bb2-c32304d3df8e
  return uuidv5(`${pluginId}:${rid}`, '3d0e5d99-6ba9-4fab-9bb2-c32304d3df8e');
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

const requiredTimelineEntryFields = ['ts', 'entry_type_id', 'input_id', 'person_id'];

function getTimelineEntryUUID(inputObject, { defaults = {} } = {}) {
  const o = { ...defaults, ...inputObject };
  /*
      Outside systems CAN specify a unique UUID as remote_entry_uuid,
      which will be used for updates, etc.
      If not, it will be generated using whatever info we have
    */
  if (o.remote_entry_uuid) {
    if (!uuidIsValid(o.remote_entry_uuid)) throw new Error('Invalid remote_entry_uuid, it must be a UUID');
    return o.remote_entry_uuid;
  }
  /*
        Outside systems CAN specify a unique remote_entry_id
        If not, it will be generated using whatever info we have
      */

  if (o.remote_entry_id) {
    // get a temp ID
    if (!o.input_id) throw new Error('Error generating timeline entry uuid -- remote_entry_id specified, but no input_id');
    const uuid = uuidv5(o.remote_entry_id, o.input_id);
    // Change out the ts to match the v7 sorting.
    // But because outside specified remote_entry_uuid
    // may not match this standard, uuid sorting isn't guaranteed
    return getUUIDv7(o.ts, uuid);
  }

  const missing = requiredTimelineEntryFields
    .filter((d) => o[d] === undefined);// 0 could be an entry type value

  if (missing.length > 0) throw new Error(`Missing required fields to append an entry_id:${missing.join(',')}`);
  const ts = new Date(o.ts);
  // isNaN behaves differently than Number.isNaN -- we're actually going for the
  // attempted conversion here
  // eslint-disable-next-line no-restricted-globals
  if (isNaN(ts)) throw new Error(`getTimelineEntryUUID got an invalid date:${o.ts || '<blank>'}`);
  const idString = `${ts.toISOString()}-${o.person_id}-${o.entry_type_id}-${o.source_code_id || 0}`;
  // get a temp ID
  const uuid = uuidv5(idString, o.input_id);
  // Change out the ts to match the v7 sorting.
  // But because outside specified remote_entry_uuid
  // may not match this standard, uuid sorting isn't guaranteed
  return getUUIDv7(ts, uuid);
}
function getEntryTypeId(o, { defaults = {} } = {}) {
  let id = o.entry_type_id || defaults.entry_type_id;
  if (id) return id;
  const etype = o.entry_type || defaults.entry_type;
  if (!etype) {
    throw new Error('No entry_type, nor entry_type_id specified, specify a defaultEntryType');
  }
  id = TIMELINE_ENTRY_TYPES[etype];
  if (id === undefined) throw new Error(`Invalid entry_type: ${etype}`);
  return id;
}

module.exports = {
  bool,
  create,
  list,
  extract,
  streamPacket,
  getBatchTransform,
  getDebatchTransform,
  getManifest,
  getFile,
  downloadFile,
  getStringArray,
  getTempFilename,
  getTimelineEntryUUID,
  getPacketFiles,
  getPluginUUID,
  getInputUUID,
  getUUIDv7,
  getUUIDTimestamp,
  uuidIsValid,
  uuidv4,
  uuidv5,
  uuidv7,
  makeStrings,
  ForEachEntry,
  FileUtilities,
  TIMELINE_ENTRY_TYPES,
  getEntryTypeId,
};
