import Service from '../../common/Service';
import ChatOTOperation from './ot/ChatOTOperation';

const RETRY_CHECKOUT_TIMEOUT = 1000;

class ChatService extends Service {
  constructor(chatOTStateManager, graphModel) {
    super({
      messages: [],
      ready: false,
      commitsGraph: null
    });
    this._chatOTStateManager = chatOTStateManager;
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

    this._chatOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._chatOTStateManager.removeChangeListener(this._onStateChange);
  }

  async sendMessage(author, content) {
    const timestamp = Date.now();
    const operation = new ChatOTOperation(timestamp, author, content, false);
    this._chatOTStateManager.add([operation]);

    await this._sync();
  }

  _onStateChange = async () => {
    this.setState({
      messages: this._getMessagesFromStateManager()
    });

    const revision = this._chatOTStateManager.getRevision();
    const commitsGraph = await this._graphModel.getGraph(revision);
    if (revision === this._chatOTStateManager.getRevision()) {
      this.setState({
        commitsGraph
      });
    }
  };

  _getMessagesFromStateManager() {
    const otState = this._chatOTStateManager.getState();
    return [...otState]
      .map(key => JSON.parse(key))
      .sort((left, right) => left.timestamp - right.timestamp);
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_CHECKOUT_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._chatOTStateManager.sync();
    } catch (err) {
      console.error(err);
      await this._sync();
    }
  }
}

export default ChatService;
