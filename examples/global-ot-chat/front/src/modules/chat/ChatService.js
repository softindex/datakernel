import Service from '../../common/Service';
import ChatOTOperation from './ot/ChatOTOperation';

const RETRY_CHECKOUT_TIMEOUT = 1000;
const FETCH_TIMEOUT = 500;

class ChatService extends Service {
  constructor(chatOTStateManager, graphModel) {
    super({
      messages: [],
      ready: false,
      commitsGraph: null
    });
    this._chatOTStateManager = chatOTStateManager;
    this._interval = null;
    this._reconnectTimeout = null;
    this._graphModel = graphModel;
  }

  async init() {
    // Get initial state
    try {
      await this._chatOTStateManager.checkout();
    } catch (err) {
      console.error(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

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
    clearTimeout(this._reconnectTimeout);
  }

  async sendMessage(author, content) {
    const timestamp = Date.now();
    const operation = new ChatOTOperation(timestamp, author, content, false);
    this._chatOTStateManager.add([operation]);

    this.setState({
      messages: [...this.state.messages, {
        author,
        content,
        loaded: false,
        timestamp
      }]
    });

    await this.fetch();
  }

  async fetch() {
    await this._chatOTStateManager.sync();

    this.setState({
      messages: this._getMessagesFromStateManager()
    });

    // Update graph
    const revision = this._chatOTStateManager.getRevision();
    const commitsGraph = await this._graphModel.getGraph(this._chatOTStateManager.getRevision());
    if (this._chatOTStateManager.getRevision() === revision) {
      this.setState({commitsGraph});
    }
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

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_CHECKOUT_TIMEOUT);
    });
  }
}

export default ChatService;
