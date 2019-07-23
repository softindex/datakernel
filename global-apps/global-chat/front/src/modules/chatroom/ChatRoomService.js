import Service from '../../common/Service';
import ChatRoomOTOperation from './ot/ChatRoomOTOperation';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import chatRoomSerializer from "./ot/serializer";
import chatRoomOTSystem from "./ot/ChatRoomOTSystem";

const RETRY_CHECKOUT_TIMEOUT = 1000;

class ChatRoomService extends Service {
  constructor(chatOTStateManager, publicKey) {
    super({
      messages: [],
      ready: false,
    });
    this._chatOTStateManager = chatOTStateManager;
    this._reconnectTimeout = null;
    this._publicKey = publicKey;
  }

  static createFrom(roomId, publicKey) {
    const chatRoomOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/room/' + roomId,
      serializer: chatRoomSerializer
    });
    const chatRoomStateManager = new OTStateManager(() => new Set(), chatRoomOTNode, chatRoomOTSystem);
    return new ChatRoomService(chatRoomStateManager, publicKey);
  }

  async init() {
    try {
      await this._chatOTStateManager.checkout();
    } catch (err) {
      console.log(err);
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

  async sendMessage(content) {
    const timestamp = Date.now();
    const operation = new ChatRoomOTOperation(timestamp, this._publicKey, content, false);
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
      console.log(err);
      await this._sync();
    }
  }
}

export default ChatRoomService;
