const fs = require('node:fs');

const path = require('node:path');
const dayjs = require('dayjs');

const debug = require('debug')('@engine9/input-tools');

const unzipper = require('unzipper');
const { v4: uuidv4, v5: uuidv5, v7: uuidv7, validate: uuidIsValid } = require('uuid');
const archiver = require('archiver');
const handlebars = require('handlebars');

const FileUtilities = require('./file/FileUtilities');

const {
  appendPostfix,
  bool,
  getManifest,
  getFile,
  downloadFile,
  getTempFilename,
  getTempDir,
  isValidDate,
  relativeDate,
  streamPacket,
  getPacketFiles,
  getBatchTransform,
  getDebatchTransform,
  getStringArray,
  makeStrings,
  writeTempFile
} = require('./file/tools');

const ForEachEntry = require('./ForEachEntry');

const { TIMELINE_ENTRY_TYPES } = require('./timelineTypes');

function getFormattedDate(dateObject, format = 'MMM DD,YYYY') {
  let d = dateObject;
  if (d === 'now') d = new Date();
  if (d) return dayjs(d).format(format);
  return '';
}

handlebars.registerHelper('date', (d, f) => {
  let format;
  if (typeof f === 'string') format = f;
  return getFormattedDate(d, format);
});
handlebars.registerHelper('json', (d) => JSON.stringify(d));

handlebars.registerHelper('uuid', () => uuidv7());

handlebars.registerHelper('percent', (a, b) => `${((100 * a) / b).toFixed(2)}%`);

handlebars.registerHelper('or', (a, b, c) => a || b || c);

async function list(_path) {
  const directory = await unzipper.Open.file(_path);

  return new Promise((resolve, reject) => {
    directory.files[0].stream().pipe(fs.createWriteStream('firstFile')).on('error', reject).on('finish', resolve);
  });
}

async function extract(_path, _file) {
  const directory = await unzipper.Open(_path);
  // return directory.files.map((f) => f.path);
  const file = directory.files.find((d) => d.path === _file);
  const tempFilename = await getTempFilename({ source: _file });
  return new Promise((resolve, reject) => {
    file.stream().pipe(fs.createWriteStream(tempFilename)).on('error', reject).on('finish', resolve);
  });
}

function appendFiles(existingFiles, _newFiles, options) {
  const newFiles = getStringArray(_newFiles);
  if (newFiles.length === 0) return;
  let { type, dateCreated } = options || {};
  if (!type) type = 'unknown';
  if (!dateCreated) dateCreated = new Date().toISOString();
  let arr = newFiles;
  if (!Array.isArray(newFiles)) arr = [arr];

  arr.forEach((p) => {
    const item = {
      type,
      originalFilename: '',
      isNew: true,
      dateCreated
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
    statisticsFiles = [] // files with aggregate statistics
  } = options;
  if (options.peopleFiles) throw new Error('Unknown option: peopleFiles, did you mean personFiles?');

  const files = [];
  const dateCreated = new Date().toISOString();
  appendFiles(files, messageFiles, { type: 'message', dateCreated });
  appendFiles(files, personFiles, { type: 'person', dateCreated });
  appendFiles(files, timelineFiles, { type: 'timeline', dateCreated });
  appendFiles(files, statisticsFiles, { type: 'statistics', dateCreated });

  const zipFilename = target || (await getTempFilename({ postfix: '.packet.zip' }));

  const manifest = {
    accountId,
    source: {
      pluginId
    },
    dateCreated,
    files
  };

  // create a file to stream archive data to.
  const output = fs.createWriteStream(zipFilename);
  const archive = archiver('zip', {
    zlib: { level: 9 } // Sets the compression level.
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
        bytes: archive.pointer()
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

const timestampMatch = /^\d{13}$/;
function dateFromString(s) {
  if (typeof s === 'number') return new Date(s);
  if (typeof s === 'string') {
    if (s.match(timestampMatch)) return new Date(parseInt(s));
  }
  return new Date(s);
}

function getUUIDv7(date, inputUuid) {
  /* optional date and input UUID */
  const uuid = inputUuid || uuidv7();
  const bytes = Buffer.from(uuid.replace(/-/g, ''), 'hex');
  if (date !== undefined) {
    const d = dateFromString(date);
    // isNaN behaves differently than Number.isNaN -- we're actually going for the
    // attempted conversion here

    if (isNaN(d)) throw new Error(`getUUIDv7 got an invalid date:${date || '<blank>'}`);
    const dateBytes = intToByteArray(d.getTime()).reverse();
    dateBytes.slice(2, 8).forEach((b, i) => {
      bytes[i] = b;
    });
  }
  return uuidv4({ random: bytes });
}
/* Returns a date from a given uuid (assumed to be a v7, otherwise the results are ... weird */
function getUUIDTimestamp(uuid) {
  const ts = parseInt(`${uuid}`.replace(/-/g, '').slice(0, 12), 16);
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
    if (!o.input_id)
      throw new Error('Error generating timeline entry uuid -- remote_entry_id specified, but no input_id');
    const uuid = uuidv5(o.remote_entry_id, o.input_id);
    // Change out the ts to match the v7 sorting.
    // But because outside specified remote_entry_uuid
    // may not match this standard, uuid sorting isn't guaranteed
    return getUUIDv7(o.ts, uuid);
  }

  const missing = requiredTimelineEntryFields.filter((d) => o[d] === undefined); // 0 could be an entry type value

  if (missing.length > 0) throw new Error(`Missing required fields to append an entry_id:${missing.join(',')}`);
  const ts = new Date(o.ts);
  // isNaN behaves differently than Number.isNaN -- we're actually going for the
  // attempted conversion here

  if (isNaN(ts)) throw new Error(`getTimelineEntryUUID got an invalid date:${o.ts || '<blank>'}`);
  const idString = `${ts.toISOString()}-${o.person_id}-${o.entry_type_id}-${o.source_code_id || 0}`;

  if (!uuidIsValid(o.input_id)) {
    throw new Error(`Invalid input_id:'${o.input_id}', type ${typeof o.input_id} -- should be a uuid`);
  }
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
function getEntryType(o, defaults = {}) {
  let etype = o.entry_type || defaults.entry_type;
  if (etype) return etype;

  const id = o.entry_type_id || defaults.entry_type_id;

  etype = TIMELINE_ENTRY_TYPES[id];
  if (etype === undefined) throw new Error(`Invalid entry_type: ${etype}`);
  return etype;
}

module.exports = {
  appendPostfix,
  bool,
  create,
  list,
  downloadFile,
  extract,
  ForEachEntry,
  FileUtilities,
  getBatchTransform,
  getDebatchTransform,
  getEntryType,
  getEntryTypeId,
  getFile,
  getManifest,
  getStringArray,
  getTempDir,
  getTempFilename,
  getTimelineEntryUUID,
  getPacketFiles,
  getPluginUUID,
  getInputUUID,
  getUUIDv7,
  getUUIDTimestamp,
  handlebars,
  isValidDate,
  makeStrings,
  relativeDate,
  streamPacket,
  TIMELINE_ENTRY_TYPES,
  writeTempFile,
  uuidIsValid,
  uuidv4,
  uuidv5,
  uuidv7
};
