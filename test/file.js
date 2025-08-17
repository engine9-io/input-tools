const {
  it,
} = require('node:test');
const assert = require('node:assert');
const debug = require('debug')('files');

const { FileUtilities } = require('../index');

it('Should list a directory', async () => {
  const futil=new FileUtilities({accountId:'test'});
  let files=await futil.list({directory:'.'});
  assert(files.length,"Should have some files");
  debug(files);
  let startTest=await futil.list({directory:'.',start:'2040-01-01'});
  assert(startTest.length===0,"Should NOT have any files before future start date");
  let endTest=await futil.list({directory:'.',end:'1900-01-01'});
  assert(endTest.length===0,"Should NOT have any files before past end date");
});
