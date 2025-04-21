const debug = require('debug')('S3Worker');
const fs = require('node:fs');
// eslint-disable-next-line import/no-unresolved
const { mimeType: mime } = require('mime-type/with-db');
const {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
  GetObjectAttributesCommand, PutObjectCommand,
  ListObjectsV2Command,
} = require('@aws-sdk/client-s3');
const { getTempFilename } = require('./tools');

function Worker() {}

function getParts(filename) {
  if (!filename || filename.indexOf('s3://') !== 0) throw new Error(`Invalid filename for s3:${filename}`);
  const parts = filename.split('/');
  const Bucket = parts[2];
  const Key = parts.slice(3).join('/');
  return { Bucket, Key };
}
Worker.prototype.getClient = function () {
  if (!this.client) this.client = new S3Client({});
  return this.client;
};

Worker.prototype.getMetadata = async function ({ filename }) {
  const s3Client = this.getClient();
  const { Bucket, Key } = getParts(filename);

  const resp = await s3Client.send(new GetObjectAttributesCommand({
    Bucket,
    Key,
    ObjectAttributes: ['ETag', 'Checksum', 'ObjectParts', 'StorageClass', 'ObjectSize'],
  }));

  return resp;
};
Worker.prototype.getMetadata.metadata = {
  options: {
    filename: {},
  },
};

Worker.prototype.stream = async function ({ filename }) {
  const s3Client = new S3Client({});
  const { Bucket, Key } = getParts(filename);
  const command = new GetObjectCommand({ Bucket, Key });
  try {
    debug(`Streaming file ${Key}`);
    const response = await s3Client.send(command);
    return { stream: response.Body };
  } catch (e) {
    debug(`Could not stream filename:${filename}`);
    throw e;
  }
};
Worker.prototype.stream.metadata = {
  options: {
    filename: {},
  },
};

Worker.prototype.download = async function ({ filename }) {
  const file = filename.split('/').pop();
  const localPath = await getTempFilename({ targetFilename: file });
  const s3Client = new S3Client({});
  const { Bucket, Key } = getParts(filename);
  const command = new GetObjectCommand({ Bucket, Key });
  debug(`Downloading ${file} to ${localPath}`);
  const response = await s3Client.send(command);
  const fileStream = fs.createWriteStream(localPath);

  response.Body.pipe(fileStream);

  return new Promise((resolve, reject) => {
    fileStream.on('finish', async () => {
      const { size } = await fs.promises.stat(localPath);
      resolve({ size, filename: localPath });
    });
    fileStream.on('error', reject);
  });
};
Worker.prototype.download.metadata = {
  options: {
    filename: {},
  },
};

Worker.prototype.put = async function (options) {
  const { filename, directory } = options;
  if (!filename) throw new Error('Local filename required');
  if (directory?.indexOf('s3://') !== 0) throw new Error(`directory path must start with s3://, is ${directory}`);

  const file = options.file || filename.split('/').pop();
  const parts = directory.split('/');
  const Bucket = parts[2];
  const Key = parts.slice(3).filter(Boolean).concat(file).join('/');
  const Body = fs.createReadStream(filename);

  const ContentType = mime.lookup(file);

  debug(`Putting ${filename} to ${JSON.stringify({ Bucket, Key, ContentType })}}`);
  const s3Client = new S3Client({});

  const command = new PutObjectCommand({
    Bucket, Key, Body, ContentType,
  });

  return s3Client.send(command);
};
Worker.prototype.put.metadata = {
  options: {
    filename: {},
    directory: { description: 'Directory to put file, e.g. s3://foo-bar/dir/xyz' },
    file: { description: 'Name of file, defaults to the filename' },
  },
};

Worker.prototype.write = async function (options) {
  const { directory, file, content } = options;

  if (!directory?.indexOf('s3://') === 0) throw new Error('directory must start with s3://');
  const parts = directory.split('/');

  const Bucket = parts[2];
  const Key = parts.slice(3).filter(Boolean).concat(file).join('/');
  const Body = content;

  debug(`Writing content of length ${content.length} to ${JSON.stringify({ Bucket, Key })}}`);
  const s3Client = new S3Client({});
  const ContentType = mime.lookup(file);

  const command = new PutObjectCommand({
    Bucket, Key, Body, ContentType,
  });

  return s3Client.send(command);
};
Worker.prototype.write.metadata = {
  options: {
    directory: { description: 'Directory to put file, e.g. s3://foo-bar/dir/xyz' },
    file: { description: 'Name of file, defaults to the filename' },
    content: { description: 'Contents of file' },
  },
};

Worker.prototype.list = async function ({ directory }) {
  if (!directory) throw new Error('directory is required');
  let dir = directory;
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key: Prefix } = getParts(dir);
  const s3Client = new S3Client({});
  const command = new ListObjectsV2Command({
    Bucket,
    Prefix: `${Prefix}/`,
    Delimiter: '/',
  });

  const { Contents: files, CommonPrefixes } = await s3Client.send(command);
  // debug('Prefixes:', { CommonPrefixes });
  const output = [].concat((CommonPrefixes || []).map((f) => ({
    name: f.Prefix.slice(Prefix.length + 1, -1),
    type: 'directory',
  })))
    .concat((files || []).map(({ Key }) => ({
      name: Key.slice(Prefix.length + 1),
      type: 'file',
    })));

  return output;
};
Worker.prototype.list.metadata = {
  options: {
    directory: { required: true },
  },
};
/* List everything with the prefix */
Worker.prototype.listAll = async function ({ directory }) {
  if (!directory) throw new Error('directory is required');
  let dir = directory;
  while (dir.slice(-1) === '/') dir = dir.slice(0, -1);
  const { Bucket, Key: Prefix } = getParts(dir);
  const s3Client = new S3Client({});
  const files = [];
  let ContinuationToken = null;
  do {
    const command = new ListObjectsV2Command({
      Bucket,
      Prefix: `${Prefix}/`,
      ContinuationToken,
      // Delimiter: '/',
    });
    debug(`Sending command with ContinuationToken ${ContinuationToken}`);
    // eslint-disable-next-line no-await-in-loop
    const result = await s3Client.send(command);
    const newFiles = (result.Contents?.map((d) => `s3://${Bucket}/${d.Key}`) || []);
    debug(`Retrieved ${newFiles.length} new files, total ${files.length},sample ${newFiles.slice(0, 3).join(',')}`);
    files.push(...newFiles);
    ContinuationToken = result.NextContinuationToken;
  } while (ContinuationToken);
  return files;
};
Worker.prototype.listAll.metadata = {
  options: {
    directory: { required: true },
  },
};

Worker.prototype.stat = async function ({ filename }) {
  if (!filename) throw new Error('filename is required');

  const s3Client = new S3Client({});
  const { Bucket, Key } = getParts(filename);
  const command = new HeadObjectCommand({ Bucket, Key });
  const response = await s3Client.send(command);

  const {
    // "AcceptRanges": "bytes",
    ContentLength, // : "3191",
    ContentType, // : "image/jpeg",
    // ETag": "\"6805f2cfc46c0f04559748bb039d69ae\"",
    LastModified, // : "2016-12-15T01:19:41.000Z",
    // Metadata": {},
    // VersionId": "null"

  } = response;
  const modifiedAt = new Date(LastModified);
  const createdAt = modifiedAt;// Same for S3
  const size = parseInt(ContentLength, 10);

  return {
    createdAt,
    modifiedAt,
    contentType: ContentType,
    size,
  };
};
Worker.prototype.stat.metadata = {
  options: {
    filename: {},
  },
};

module.exports = Worker;
