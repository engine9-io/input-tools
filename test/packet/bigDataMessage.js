const {
  describe, it,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('test:big-data');
const { setTimeout } = require('node:timers/promises');

const { ForEachEntry } = require('../../index');

describe('big-data message: forEachPerson', async () => {
  it('message: forEachPerson should loop through 1000000 sample people', async () => {
    const messageContent = [];
    let counter = 0;
    const forEach = new ForEachEntry();

    await forEach.transform(
      {
        packet: '../1000000_person_message.packet.zip',
        batchSize: 100000,
        bindings: {
          timelineOutputStream: { type: 'packet.output.timeline' },
          message: { type: 'packet.message' },
          handlebars: { type: 'handlebars' },
        },
        async transform({
          batch,
          message,
          handlebars,
          timelineOutputStream,
        }) {
          debug(`Processing batch of ${batch.length}`);
          if (!message?.content?.text) throw new Error(`Sample message has no content.text:${JSON.stringify(message)}`);
          const template = handlebars.compile(message.content.text);
          batch.forEach((person) => {
            messageContent.push(template(person));
          });
          batch.forEach((p) => {
            const o = {
              person_id: p.person_id,
              email: p.email,
              entry_type_label: 'EMAIL_DELIVERED',
            };
            counter += 1;
            if (counter % 10000 === 0) debug(`Processed ${counter} items, last person_id=${p.person_id}`, o);
            timelineOutputStream.push(o);
          });
          debug(`Processed batch of size ${batch.length}`);
          await setTimeout(Math.random() * 100);
        },
      },
    );

    assert.equal(counter, 1000000, `Expected to loop through 1000000 people, actual:${counter}`);
  });
});
