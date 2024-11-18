const {
  it,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('message');

const { getUUIDv7, getUUIDTimestamp } = require('../index');

it('Should correctly calculate UUIDS given various inputs', async () => {
  const now = new Date();

  const uuid = getUUIDv7();
  const ts = getUUIDTimestamp(uuid);
  debug(uuid, ts, now);
  const diff = now.getTime() - ts.getTime();
  assert(diff < 1000 && diff >= 0, 'More than a second between newly created');

  const uuid2 = getUUIDv7(now);
  const ts2 = getUUIDTimestamp(uuid2);
  const diff2 = now.getTime() - ts2.getTime();
  assert(diff2 === 0, 'Timestamps should match');

  const uuid3 = getUUIDv7(now);
  assert(uuid2 !== uuid3, 'UUIDs should be unique match');
});
