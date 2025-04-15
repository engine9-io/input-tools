// With a strong assist from Danny Dai at:
// https://medium.com/@dannz_51927/node-js-streams-ordered-parallel-execution-10ce6baa995e

/* eslint-disable no-underscore-dangle */
const { Transform } = require('node:stream');
const debug = require('debug')('ParallelStream');

class ParallelStream extends Transform {
  constructor({ concurrency, transform: userTransform, opts }) {
    super({ objectMode: true, highWaterMark: concurrency + 1, ...opts });
    this.userTransform = userTransform;
    this.running = 0;
    this.concurrency = 0;
    this.terminateCb = null;
    this.promiseQueue = [];
    // debug(this.writableHighWaterMark, this.readableHighWaterMark);
  }

  _transform(chunk, enc, done) {
    this.running += 1;
    this.concurrency += 1;

    // allow for push in the transform
    this.push.bind(this.userTransform);
    // task userTransform function pushed to the queue
    this.promiseQueue.push(
      this.userTransform(
        chunk,
        enc,
        this._onComplete.bind(this),
      ),
    );
    if (this.concurrency < this.writableHighWaterMark - 1) {
      done();
    } else {
      // for every N tasks, this block gets called to execute promise.all
      // result order garanteed by promise.all
      // debug('tasks reached ', this.running);
      // debug('running these tasks....');
      this.concurrency = 0;
      const tempTaskQueue = [...this.promiseQueue];
      this.promiseQueue = [];
      Promise.all(tempTaskQueue).then((values) => {
        values.forEach((value) => {
          // this.push(value);
        });

        done();
      });
    }
  }

  _flush(done) {
    if (this.concurrency !== 0) {
      Promise.all(this.promiseQueue).then((values) => {
        values.forEach((value) => {
          debug('_flush push value', value);
          this.push(value);
        });

        // debug('in _flush, tasks done, pushes finished');

        // this call fired later than its assignment due to aync block,
        // so this.terminateCb isnt null
        if (typeof this.terminateCb === 'function') {
          this.terminateCb();
        }
      });
    }

    // debug('in _flush', Date.now());
    if (this.running > 0) {
      // if any tasks left, run this
      this.terminateCb = done;
    } else {
      done();
    }
  }

  _destroy(error, callback) {
    if (error) debug(error);

    // debug('in _destroy', Date.now());
    super._destroy(error, callback);
  }

  _onComplete(err) {
    this.running -= 1;

    // debug('in on_complete', chunk.toString(), Date.now());
    if (err) {
      return this.emit('error', err);
    }
    if (this.running === 0) {
      // debug('running task = 0');
    }
    return null;
  }
}

module.exports = ParallelStream;
