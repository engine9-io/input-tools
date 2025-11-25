const { describe, it } = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('test/forEach');

const { ForEachEntry } = require('../../index');

describe('Test Person File For Each', async () => {
  it('forEachPerson Should loop through 1000 sample people', async () => {
    let counter = 0;
    const forEach = new ForEachEntry();
    const result = await forEach.process({
      packet: 'test/sample/1000_message.packet.zip',
      batchSize: 50,
      bindings: {
        timelineOutputFileStream: {
          path: 'output.timeline',
          options: {
            entry_type: 'ENTRY_OPTION'
          }
        },
        sampleOutputFileStream: {
          path: 'output.stream'
        }
      },
      async transform(props) {
        const { batch, timelineOutputFileStream, sampleOutputFileStream } = props;

        batch.forEach((p) => {
          if (Math.random() > 0.9) {
            sampleOutputFileStream.push({
              // for testing we don't need real person_ids
              person_id: p.person_id || Math.floor(Math.random() * 1000000),
              email: p.email,
              entry_type: 'SAMPLE_OUTPUT'
            });
          }
          timelineOutputFileStream.push({
            // for testing we don't need real person_ids
            person_id: p.person_id || Math.floor(Math.random() * 1000000),
            email: p.email,
            entry_type: 'EMAIL_DELIVERED'
          });
        });

        batch.forEach(() => {
          counter += 1;
        });
      }
    });
    assert(result.outputFiles?.timelineOutputFileStream?.[0]?.records);
    assert(result.outputFiles?.sampleOutputFileStream?.[0]?.records);
    debug(result);
    assert.equal(counter, 1000, `Expected to loop through 1000 people, actual:${counter}`);
  });
  debug('Completed tests');
});
