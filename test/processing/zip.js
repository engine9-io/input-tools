const { describe, it } = require('node:test');
const assert = require('node:assert');
const debug = require('debug');

const { create, getManifest } = require('../../index');

describe('Test Person Packet Creator', async () => {
  const pfile = './test/sample/message/5_fake_people.csv';
  it(`should create a zip file from directory ${process.cwd()} with path ${pfile}`, async () => {
    const out = await create({
      personFiles: [pfile],
      messageFiles: 'test/sample/message/message.json5',
    });
    debug('Successfully created:', out);
    return out;
  });
  it('should retrieve a manifest', async () => {
    const manifest = await getManifest({ packet: './test/sample/5_message.packet.zip' });
    assert.equal(typeof manifest, 'object', 'Manifest is not an object');
    assert.equal(manifest.accountId, 'engine9', 'Manifest does not have an accountId=engine9');
  });
});
