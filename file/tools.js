const fs = require('node:fs');

const fsp = fs.promises;
const path = require('node:path');
const debug = require('debug')('@engine9-io/input-tools');
const os = require('node:os');
const { mkdirp } = require('mkdirp');
const { Transform } = require('node:stream');

const JSON5 = require('json5');
const { PassThrough } = require('node:stream');
const progress = require('debug')('info:@engine9-io/input-tools');
const unzipper = require('unzipper');

const {
  v7: uuidv7,
} = require('uuid');

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
  const { targetFormat } = options;
  if (!postfix && targetFormat === 'csv') postfix = '.csv';
  if (options.source) {
    postfix = `_${options.source.split('/').pop()}`;
    postfix = postfix.replace(/['"\\]/g, '').replace(/[^a-zA-Z0-9_.-]/g, '_');
  }

  if (prefix) prefix += '_';

  const p = `${dir}/${prefix || ''}${uuidv7()}${postfix || '.txt'}`;
  return p;
}

const {
  S3Client,
  HeadObjectCommand,
  GetObjectCommand,
} = require('@aws-sdk/client-s3');

async function getPacketFiles({ packet }) {
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
        const ptStream = new PassThrough();
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
  const { files } = await getPacketFiles({ packet });
  const file = files.find((d) => d.path === 'manifest.json');
  const content = await file.buffer();
  const manifest = JSON.parse(content.toString());
  return manifest;
}

function getBatchTransform({ batchSize = 100 }) {
  return {
    transform: new Transform({
      objectMode: true,
      transform(chunk, encoding, cb) {
        this.buffer = (this.buffer || []).concat(chunk);
        if (this.buffer.length >= batchSize) {
          this.push(this.buffer);
          this.buffer = [];
        }
        cb();
      },
      flush(cb) {
        if (this.buffer?.length > 0) this.push(this.buffer);
        cb();
      },
    }),
  };
}
function getDebatchTransform() {
  return {
    transform: new Transform({
      objectMode: true,
      transform(chunk, encoding, cb) {
        chunk.forEach((c) => this.push(c));
        cb();
      },
    }),
  };
}

async function getFile({ filename, packet, type }) {
  if (!packet && !filename) throw new Error('no packet option specififed');
  let content = null;
  let filePath = null;
  if (packet) {
    const manifest = await getManifest({ packet });
    const manifestFiles = manifest.files?.filter((d) => d.type === type);
    if (!manifestFiles?.length) throw new Error(`No files of type ${type} found in packet`);
    if (manifestFiles?.length > 1) throw new Error(`Multiple files of type ${type} found in packet`);
    filePath = manifestFiles[0].path;
    const { files } = await getPacketFiles({ packet });
    const handle = files.find((d) => d.path === filePath);
    const buffer = await handle.buffer();
    content = await buffer.toString();
  } else {
    content = await fsp.readFile(filename);
    filePath = filename.split('/').pop();
  }
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

async function streamPacket({ packet, type }) {
  if (!packet) throw new Error('no packet option specififed');
  const manifest = await getManifest({ packet });
  const manifestFiles = manifest.files?.filter((d) => d.type === type);
  if (!manifestFiles?.length) throw new Error(`No files of type ${type} found in packet`);
  if (manifestFiles?.length > 1) throw new Error(`Multiple files of type ${type} found in packet`);
  const filePath = manifestFiles[0].path;
  const { files } = await getPacketFiles({ packet });
  const handle = files.find((d) => d.path === filePath);
  return { stream: handle.stream(), path: filePath };
}

async function downloadFile({ packet, type = 'person' }) {
  const { stream: fileStream, path: filePath } = await streamPacket({ packet, type });
  const filename = await getTempFilename({ targetFilename: filePath.split('/').pop() });

  return new Promise((resolve, reject) => {
    fileStream.pipe(fs.createWriteStream(filename))
      .on('error', reject)
      .on('finish', () => {
        resolve({ filename });
      });
  });
}

function bool(x, _defaultVal) {
  const defaultVal = (_defaultVal === undefined) ? false : _defaultVal;
  if (x === undefined || x === null || x === '') return defaultVal;
  if (typeof x !== 'string') return !!x;
  if (x === '1') return true; // 0 will return false, but '1' is true
  const y = x.toLowerCase();
  return !!(y.indexOf('y') + 1) || !!(y.indexOf('t') + 1);
}

module.exports = {
  bool,
  getTempFilename,
  downloadFile,
  getBatchTransform,
  getDebatchTransform,
  getFile,
  getManifest,
  getPacketFiles,
  getTempDir,
  streamPacket,
};
