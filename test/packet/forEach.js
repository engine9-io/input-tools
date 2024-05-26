const {
  describe, it,
} = require('node:test');
const assert = require('node:assert');

const { forEachPerson } = require('../../index');

describe('Test Person Packet Creator', async () => {
  it('should loop through 1000 sample people', async () => {
    let counter = 0;
    await forEachPerson(
      {
        packet: 'test/sample/1000_message.packet.zip',
        batchSize: 50,
        bindings: {
          // timelineOutput: { type: 'timeline', table: 'person_email', columns: ['person_id'] },
        },
        async transform({
          batch,
          // message,

        }) {
          batch.forEach(() => { counter += 1; });
          return {
            timelineEntries: batch.map((p) => ({
              person_id: p.person_id,
              email: p.email,
              entry_type_label: 'EMAIL_DELIVERED',
            })),
          };
        },
      },
    );
    assert.equal(counter, 1000, `Expected to loop through 1000 people, actual:${counter}`);
  });
});
