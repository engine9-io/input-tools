const { describe, it } = require('node:test');
const assert = require('node:assert');

const { create, getManifest } = require('../../index');

describe('Test Person Packet Creator', async () => {
  it('should create a zip file', async () => {
    await create({
      personFiles: ['test/sample/message/1000_fake_people.csv'],
      messageFiles: 'test/sample/message/message.json5',
    });
  });
  it('should retrieve a manifest', async () => {
    const manifest = await getManifest({ packet: 'test/sample/message.packet.zip' });
    // console.log('Manifest=', typeof manifest, manifest);
    assert.equal(typeof manifest, 'object', 'Manifest is not an object');
    assert.equal(manifest.accountId, 'engine9', 'Manifest does not have an accountId=engine9');
  });
});
