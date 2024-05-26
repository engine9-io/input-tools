const {
  describe, it,
} = require('node:test');
const { create } = require('../../index');

describe('Test Person Packet Creator', async () => {
  it('should create a zip file', async () => {
    await create({
      personFiles: ['test/sample/message/1000_fake_people.csv'],
      messageFiles: 'test/sample/message/message.json5',
    });
  });
});
