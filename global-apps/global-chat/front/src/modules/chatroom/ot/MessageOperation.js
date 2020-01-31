import * as types from '../MESSAGE_TYPES';

class MessageOperation {
  constructor(timestamp, authorPublicKey, content, invert) {
    this._timestamp = timestamp;
    this._authorPublicKey = authorPublicKey;
    this._content = content;
    this._invert = invert;
  }

  static EMPTY = new MessageOperation(0, null, null, false);

  static createFromJson(json) {
    return new MessageOperation(
      json.timestamp,
      json.author,
      json.content,
      json.invert
    );
  }

  apply(state) {
    const key = JSON.stringify({
      timestamp: this._timestamp,
      authorPublicKey: this._authorPublicKey,
      content: this._content,
      type: types.MESSAGE_REGULAR
    });

    if (this._invert) {
      state.messages.delete(key);
    } else {
      state.messages.add(key);
    }

    return state;
  }

  isEmpty() {
    return this._content === null;
  }

  invert() {
    return new MessageOperation(this._timestamp, this._authorPublicKey, this._content, !this._invert);
  }

  isEqual(messageOperation) {
    return (
      this._timestamp === messageOperation._timestamp
      && this._authorPublicKey === messageOperation._authorPublicKey
      && this._content === messageOperation._content
      && this._invert === messageOperation._invert
    );
  }

  toJSON() {
    return {
      type: types.MESSAGE,
      value: {
        timestamp: this._timestamp,
        author: this._authorPublicKey,
        content: this._content,
        invert: this._invert
      }
    };
  }
}

export default MessageOperation;
