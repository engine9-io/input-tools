const {
  describe, it,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('test/forEach');

const { ForEachEntry } = require('../../index');

describe('Test Person Packet For Each', async () => {
  it('forEachPerson Should loop through 1000 sample people', async () => {
    let counter = 0;
    const forEach = new ForEachEntry();
    await forEach.process(
      {
        packet: 'test/sample/1000_message.packet.zip',
        batchSize: 50,
        bindings: {
          timelineOutputStream: {
            path: 'output.timeline',
            options: {
              entry_type: 'SAMPLE',
            },
          },
        },
        async transform(props) {
          const {
            batch,
            timelineOutputStream,
          } = props;
          if (timelineOutputStream) {
            batch.forEach((p) => {
              timelineOutputStream.push(
                {
                  // for testing we don't need real person_ids
                  person_id: p.person_id || Math.floor(Math.random() * 1000000),
                  email: p.email,
                  entry_type: 'EMAIL_DELIVERED',
                },
              );
            });
          } else {
            throw new Error(`output.timeline did not put a timelineOutputStream into the bindings:${Object.keys(props)}`);
          }
          batch.forEach(() => { counter += 1; });
        },
      },
    );
    assert.equal(counter, 1000, `Expected to loop through 1000 people, actual:${counter}`);
  });
  debug('Completed tests');
});
