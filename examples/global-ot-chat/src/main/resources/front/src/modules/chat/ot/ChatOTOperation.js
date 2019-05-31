class ChatOTOperation {
  constructor(timestamp, author, content, isDeleted) {
    this.timestamp = timestamp;
    this.author = author;
    this.content = content;
    this.isDeleted = isDeleted;
  }

  static EMPTY = new ChatOTOperation(0, '', '', false);

  apply(state) {
    const key = JSON.stringify({
      timestamp: this.timestamp,
      author: this.author,
      content: this.content,
      isDeleted: this.isDeleted
    });

    if (this.isDeleted) {
      state.delete(key);
    } else {
      state.add(key);
    }

    return state;
  }

  isEmpty() {
    return !this.author || !this.content;
  }

  invert() {
    return new ChatOTOperation(this.timestamp, this.author, this.content, !this.isDeleted);
  }

  isEqual(chatOTOperation) {
    return (
      chatOTOperation.timestamp === this.timestamp
      && chatOTOperation.author === this.author
      && chatOTOperation.content === this.content
      && chatOTOperation.isDeleted === this.isDeleted
    );
  }
}

export default ChatOTOperation;
