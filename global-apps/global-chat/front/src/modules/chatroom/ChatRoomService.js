import Service from '../../common/Service';
import ChatRoomOTOperation from './ot/ChatRoomOTOperation';
import ChatMessage from "./ot/ChatMessage";

const RETRY_CHECKOUT_TIMEOUT = 1000;

class ChatRoomService extends Service {
  constructor(chatOTStateManager) {
    super({
      messages: [],
      ready: false,
    });
    this._chatOTStateManager = chatOTStateManager;
    this._reconnectTimeout = null;
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

    this._onStateChange();

    this._chatOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._chatOTStateManager.removeChangeListener(this._onStateChange);
  }

  async sendMessage(author, content) {
    const timestamp = Date.now();
    const message = new ChatMessage(timestamp, author, content);
    const operation = new ChatRoomOTOperation(message, false);
    this._chatOTStateManager.add([operation]);

    await this._sync();
  }

  _onStateChange = () => {
    this.setState({
      messages: this._getMessagesFromStateManager(),
      ready: true
    });
  };

  _getMessagesFromStateManager() {
    const otState = this._chatOTStateManager.getState();
    return [...otState]
      .map(key => JSON.parse(key))
      .map(op => op.message)
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

export default ChatRoomService;
