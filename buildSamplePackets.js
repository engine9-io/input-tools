import InputTools from './index.js';
(async () => {
  await InputTools.create({
    target: './test/sample/5_message.packet.zip',
    personFiles: ['./test/sample/message/5_fake_people.csv'],
    messageFiles: ['./test/sample/message/message.json5']
  });
  await InputTools.create({
    target: './test/sample/1000_message.packet.zip',
    personFiles: ['./test/sample/message/1000_fake_people.csv'],
    messageFiles: ['./test/sample/message/message.json5']
  });
})();
