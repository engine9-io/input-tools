const { Readable } = require('node:stream');

/*
  A readable that will check data prior to it going into the stream
*/
class ValidatingReadable extends Readable {
  constructor(options, validator) {
    super(options);
    this.validator = validator || (() => true);
  }

  // _read() {super._read(size)}

  push(chunk) {
    try {
      this.validator(chunk);
      super.push(chunk);
    } catch (e) {
      this.emit('error', e);
    }
  }
}

module.exports = ValidatingReadable;
