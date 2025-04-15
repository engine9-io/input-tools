/* eslint-disable no-console */
const {
  setTimeout,
} = require('node:timers/promises');

const { describe } = require('node:test');
// const assert = require('node:assert');
const { Readable } = require('node:stream');
const { createWriteStream } = require('node:fs');
const { pipeline } = require('node:stream/promises');
const ParallelStream = require('../ParallelStream');
const { getTempFilename } = require('../index');

describe('Should process items in parallel:', async () => {
  const outputFile = await getTempFilename({});
  const writeStream = createWriteStream(outputFile);

  const CONCURRENCY = 500;
  await pipeline(
    Readable.from(
      [...Array(1000)].map((v, i) => ({ i })),
    ),

    new ParallelStream(
      CONCURRENCY,
      async (obj, enc, push, done) => {
        let res;

        try {
          await setTimeout(Math.random() * 1000);
          if (Math.random() > 0.7) throw new Error('Random error');

          res = `${obj.id} is complete\n`;
        } catch (err) {
          await setTimeout(Math.random() * 2000);// longer timeouts for errors
          res = `${obj.id} is error, ${err.name}\n`;
        }

        done(null, obj.id); // _onComplete actually

        return res;
      },
    ),
    writeStream,
  );

  console.log('Wrote responses to ', outputFile);
});
