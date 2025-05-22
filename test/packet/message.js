const {
  describe, it,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('message');

const { forEachPerson } = require('../../index');

describe('Test Person Packet Message For Each', async () => {
  it('message: forEachPerson should loop through 1000 sample people', async () => {
    const messageContent = [];
    let counter = 0;
    const results = await forEachPerson(
      {
        packet: 'test/sample/1000_message.packet.zip',
        batchSize: 50,
        bindings: {
          timelineOutputStream: { type: 'output.timeline' },
          message: { type: 'packet.message' },
          handlebars: { type: 'handlebars' },
        },
        async transform({
          batch,
          message,
          handlebars,
          timelineOutputStream,
        }) {
          const template = handlebars.compile(message.content.text);
          batch.forEach((person) => {
            messageContent.push(template(person));
          });
          batch.forEach(() => { counter += 1; });
          batch.forEach((p) => {
            timelineOutputStream.push(
              {
                person_id: p.person_id,
                email: p.email,
                entry_type: 'EMAIL_DELIVERED',
              },
            );
          });
        },
      },
    );
    debug(results);
    assert.equal(counter, 1000, `Expected to loop through 1000 people, actual:${counter}`);
  });
});
