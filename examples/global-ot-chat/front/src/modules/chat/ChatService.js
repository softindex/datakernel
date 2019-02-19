import Service from '../../common/Service';

function wait(time) {
  return new Promise(resolve => {
    setTimeout(resolve, time);
  });
}

class ChatService extends Service {
  constructor() {
    super({
      messages: []
    });
    this._maxMessageId = 0;
  }

  async sendMessage(author, text) {
    const messageId = ++this._maxMessageId;

    this.setState({
      messages: [...this.state.messages, {
        id: messageId,
        author,
        text,
        time: new Date(),
        loaded: false
      }]
    });

    await wait(1000);

    this.setState({
      messages: this.state.messages.map(message => {
        if (message.id === messageId) {
          return {
            ...message,
            loaded: true
          };
        }

        return message;
      })
    });
  }
}

export default ChatService;
