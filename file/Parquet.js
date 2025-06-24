const parquet = require('@dsnp/parquetjs');

const { Readable } = require('node:stream');
const debug = require('debug')('ParquetWorker');
const { S3Client } = require('@aws-sdk/client-s3');
const FileWorker = require('./FileUtilities');

function Worker() {}

async function getReader(options) {
  const { filename } = options;
  if (!filename) throw new Error('filename is required');
  if (filename.indexOf('s3://') === 0) {
    const client = new S3Client({});
    const parts = filename.split('/');

    return parquet.ParquetReader.openS3(client, {
      Bucket: parts[2],
      Key: parts.slice(3).join('/'),
    });
  }
  return parquet.ParquetReader.openFile(filename);
}

Worker.prototype.meta = async function (options) {
  const reader = await getReader(options);
  return {
    records: String(reader.metadata?.num_rows),
  };
  // getMetadata();
};
Worker.prototype.meta.metadata = {
  options: {
    path: {},
  },
};
Worker.prototype.schema = async function (options) {
  const reader = await getReader(options);
  return reader.getSchema();
};
Worker.prototype.schema.metadata = {
  options: {
    path: {},
  },
};

function cleanColumnName(name) {
  return name.toLowerCase().replace(/[^a-z0-9_]/g, '_');
}

Worker.prototype.stream = async function (options) {
  const stream = new Readable({ objectMode: true });

  const reader = await getReader(options);
  let columns;
  if (options.columns) {
    const { fieldList } = await this.schema(options);
    columns = [];
    let requestedColumns = options.columns;
    if (typeof options.columns === 'string') requestedColumns = options.columns.split(',').map((d) => d.trim());
    else requestedColumns = options.columns.map((d) => (d.name ? d.name.trim() : d.trim()));
    requestedColumns.forEach((c) => {
      const matchingCols = fieldList.filter((f) => (
        f.name === c || cleanColumnName(f.name) === cleanColumnName(c)
      )).map((f) => f.name);
      columns = columns.concat(matchingCols);
    });
  }
  let limit = 0;
  if (parseInt(options.limit, 10) === options.limit) limit = parseInt(options.limit, 10);
  // create a new cursor
  debug(`Reading parquet file ${options.filename} with columns ${columns?.join(',')} and limit ${limit}`);
  const cursor = reader.getCursor(columns);

  // read all records from the file and print them
  let record = null;
  let counter = 0;

  const start = new Date().getTime();
  do {
    // eslint-disable-next-line no-await-in-loop
    record = await cursor.next();
    counter += 1;
    if (limit && counter > limit) {
      debug(`Reached limit of ${limit}, stopping`);
      break;
    }
    if (counter % 5000 === 0) {
      const end = new Date().getTime();
      debug(`Read ${counter} ${(counter * 1000) / (end - start)}/sec `);
    }
    stream.push(record);
  } while (record);
  stream.push(null);
  await reader.close();

  return { stream };
};

Worker.prototype.stream.metadata = {
  options: {
    path: {},
  },
};

Worker.prototype.toFile = async function (options) {
  const { stream } = await this.stream(options);
  const fworker = new FileWorker(this);
  return fworker.objectStreamToFile({ ...options, stream });
};
Worker.prototype.toFile.metadata = {
  options: {
    path: {},
  },
};

Worker.prototype.stats = async function (options) {
  const reader = await getReader(options);
  const schema = reader.getSchema();
  const fileMetadata = reader.getFileMetaData();
  const rowGroups = reader.getRowGroups();

  // const reader = await parquet.ParquetReader.openS3(client, getParams(options));
  // return reader.getSchema();
  return {
    schema,
    fileMetadata,
    rowGroups,
  };
};
Worker.prototype.stats.metadata = {
  options: {
    path: {},
  },
};

module.exports = Worker;
