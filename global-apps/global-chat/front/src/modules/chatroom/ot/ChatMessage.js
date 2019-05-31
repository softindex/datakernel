class ChatMessage {
  constructor(timestamp, author, content) {
    this.timestamp = timestamp;
    this.author = author;
    this.content = content;
  }

  isEmpty() {
    return !this.author || !this.content;
  }

  isEqual(chatMessage) {
    return (
      chatMessage.timestamp === this.timestamp &&
      chatMessage.author === this.author &&
      chatMessage.content === this.content
    );
  }

}

export default ChatMessage;
