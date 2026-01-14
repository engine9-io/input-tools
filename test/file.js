import nodetest from 'node:test';
import assert from 'node:assert';
import * as debug$0 from 'debug';
import { FileUtilities } from '../index.js';
const { it } = nodetest;
const debug = debug$0('files');
it('Should list a directory', async () => {
  const futil = new FileUtilities({ accountId: 'test' });
  let files = await futil.list({ directory: '.' });
  assert(files.length, 'Should have some files');
  debug(files);
  let startTest = await futil.list({ directory: '.', start: '2040-01-01' });
  assert(startTest.length === 0, 'Should NOT have any files before future start date');
  let endTest = await futil.list({ directory: '.', end: '1900-01-01' });
  assert(endTest.length === 0, 'Should NOT have any files before past end date');
});
it('Should be able to analyze CSV files with and without header lines', async () => {
  const futil = new FileUtilities({ accountId: 'test' });
  const f1 = await futil.columns({ filename: __dirname + '/sample/fileWithHead.csv' });
  assert.equal(f1.likelyHeaderLines, 1, 'Number of header lines should be 1');
  const f2 = await futil.columns({ filename: __dirname + '/sample/fileWithoutHead.csv' });
  assert.equal(f2.likelyHeaderLines, 0, 'Number of header lines should be 1');
});
