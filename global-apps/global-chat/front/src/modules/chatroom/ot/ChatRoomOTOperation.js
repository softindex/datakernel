class ChatRoomOTOperation {
  constructor(timestamp, authorPublicKey, content, removed) {
    this._timestamp = timestamp;
    this._authorPublicKey = authorPublicKey;
    this._content = content;
    this._removed = removed;
  }

  static EMPTY = new ChatRoomOTOperation(0, null, null, false);

  static createFromJson({value}) {
    return new ChatRoomOTOperation(
      value.timestamp,
      value.author,
      value.content,
      value.invert
    );
  }

  apply(state) {
    const key = JSON.stringify({
      timestamp: this._timestamp,
      authorPublicKey: this._authorPublicKey,
      content: this._content
    });

    if (this._removed) {
      state.delete(key);
    } else {
      state.add(key);
    }

    return state;
  }

  isEmpty() {
    return this._content === null;
  }

  invert() {
    return new ChatRoomOTOperation(this._timestamp, this._authorPublicKey, this._content, !this._removed);
  }

  isEqual(chatOTOperation) {
    return (
      this._timestamp === chatOTOperation._timestamp
      && this._authorPublicKey === chatOTOperation._authorPublicKey
      && this._content === chatOTOperation._content
      && this._removed === chatOTOperation._removed
    );
  }

  toJSON() {
    return {
      type: 'Message',
      value: {
        timestamp: this._timestamp,
        author: this._authorPublicKey,
        content: this._content,
        invert: this._removed
      }
    };
  }
}

export default ChatRoomOTOperation;
