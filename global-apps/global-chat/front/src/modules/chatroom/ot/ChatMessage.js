class ChatMessage {
  constructor(timestamp, authorPublicKey, content) {
    this.timestamp = timestamp;
    this.author = authorPublicKey;
    this.content = content;
  }

  isEmpty() {
    return !this.author && !this.content;
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
