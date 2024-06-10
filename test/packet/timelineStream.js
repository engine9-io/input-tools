const {
  describe, it,
} = require('node:test');
const fs = require('node:fs');
const assert = require('node:assert');

const { getTimelineOutputStream } = require('../../index');

describe('TimelineOutputStream', async () => {
  it('timeline: It should save items to a csv file', async () => {
    const {
      stream: timelineStream, promises,
      files,
    } = await getTimelineOutputStream({});
    timelineStream.push({ foo: 'bar' });
    // finish the input stream
    timelineStream.push(null);
    await promises[0];
    const content = fs.readFileSync(files[0]).toString().split('\n').map((d) => d.trim())
      .filter(Boolean);

    const s = 'uuid,entry_type,person_id,reference_id';
    assert.equal(content[0].slice(0, s.length), s, "Beginning of first line doesn't match expected timeline csv header");
    assert.equal(content.length, 2, `There are ${content.length}, not 2 lines in the CSV file`);
  });
});
