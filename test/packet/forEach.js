const {
  describe, it,
} = require('node:test');
const assert = require('node:assert');

const { forEachPerson } = require('../../index');

describe('Test Person Packet For Each', async () => {
  it('forEachPerson Should loop through 1000 sample people', async () => {
    let counter = 0;
    await forEachPerson(
      {
        packet: 'test/sample/1000_message.packet.zip',
        batchSize: 50,
        bindings: {
          timelineOutputStream: { type: 'packet.output.timeline' },
        },
        async transform(props) {
          const {
            batch,
            timelineOutputStream,
          } = props;
          if (!timelineOutputStream) {
            throw new Error(`packet.output.timeline did not put a timelineOutputStream into the bindings:${Object.keys(props)}`);
          }
          batch.forEach(() => { counter += 1; });
          batch.forEach((p) => {
            timelineOutputStream.push(
              {
                person_id: p.person_id,
                email: p.email,
                entry_type_label: 'EMAIL_DELIVERED',
              },
            );
          });
        },
      },
    );
    assert.equal(counter, 1000, `Expected to loop through 1000 people, actual:${counter}`);
  });
});
