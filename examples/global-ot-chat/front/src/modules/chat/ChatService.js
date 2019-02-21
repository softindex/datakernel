import Service from '../../common/Service';
import ChatOTOperation from './ot/ChatOTOperation';

const FETCH_TIMEOUT = 500;

class ChatService extends Service {
  constructor(chatOTStateManager) {
    super({
      messages: [],
      ready: false
    });
    this._chatOTStateManager = chatOTStateManager;
    this._interval = null;
  }

  async init() {
    await this._chatOTStateManager.checkout();
    this.setState({
      messages: this._getMessagesFromStateManager(),
      ready: true
    });

    let fetching = false;
    this._interval = setInterval(async () => {
      if (fetching) {
        return;
      }

      fetching = true;
      try {
        await this.fetch();
      } finally {
        fetching = false;
      }
    }, FETCH_TIMEOUT);
  }

  stop() {
    clearInterval(this._interval);
  }

  async sendMessage(author, content) {
    const timestamp = Date.now();
    const operation = new ChatOTOperation(timestamp, author, content, false);
    this._chatOTStateManager.add([operation]);

    this.setState({
      messages: [...this.state.messages, {
        author,
        content,
        timestamp,
        loaded: false
      }]
    });

    await this.fetch();
  }

  async fetch() {
    await this._chatOTStateManager.sync();

    this.setState({
      messages: this._getMessagesFromStateManager()
    });
  }

  _getMessagesFromStateManager() {
    const otState = this._chatOTStateManager.getState();
    return [...otState]
      .map(key => ({
        ...JSON.parse(key),
        loaded: true
      }))
      .sort((left, right) => left.timestamp - right.timestamp);
  }
}

export default ChatService;
