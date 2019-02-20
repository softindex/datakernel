import Service from '../../common/Service';
import ChatOTOperation from './ot/ChatOTOperation';

class ChatService extends Service {
  constructor(chatOTStateManager) {
    super({
      messages: [],
      ready: false
    });
    this._chatOTStateManager = chatOTStateManager;
  }

  async init() {
    await this._chatOTStateManager.checkout();
    this.setState({
      messages: this._getMessagesFromStateManager(),
      ready: true
    });
  }

  async sendMessage(author, content) {
    const timestamp = Date.now();
    const operation = new ChatOTOperation(timestamp, author, content, false);
    this._chatOTStateManager.add([operation]);

    this.setState({
      messages: [...this.state.messages, {
        author,
        content,
        timestamp
      }]
    });

    await this._chatOTStateManager.sync();

    this.setState({
      messages: this._getMessagesFromStateManager()
    });
  }

  _getMessagesFromStateManager() {
    const otState = this._chatOTStateManager.getState();
    const orderedMessages = [...otState]
      .map(JSON.parse)
      .sort((left, right) => left.timestamp - right.timestamp);
    return orderedMessages;
  }
}

export default ChatService;
