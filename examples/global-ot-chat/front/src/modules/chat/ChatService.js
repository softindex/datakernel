import Service from '../../common/Service';
import ChatOTOperation from './ot/ChatOTOperation';

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
    this._graphModel = graphModel;
  }

  async init() {
    await this._chatOTStateManager.checkout();
    const revision = this._chatOTStateManager.getRevision();
    const commitsGraph = await this._graphModel.getGraph(revision);
    this.setState({
      messages: this._getMessagesFromStateManager(),
      ready: true,
      commitsGraph
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
        loaded: false,
        timestamp
      }]
    });

    await this.fetch();
  }

  async fetch() {
    const revision = this._chatOTStateManager.getRevision();
    await this._chatOTStateManager.sync();
    const commitsGraph = await this._graphModel.getGraph(this._chatOTStateManager.getRevision());
    if (this._chatOTStateManager.getRevision() === revision) return;

    this.setState({
      messages: this._getMessagesFromStateManager(),
      commitsGraph
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
