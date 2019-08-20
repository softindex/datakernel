import {Service} from 'global-apps-common';
import ChatRoomOTOperation from './ot/ChatRoomOTOperation';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import chatRoomSerializer from "./ot/serializer";
import chatRoomOTSystem from "./ot/ChatRoomOTSystem";
import {ROOT_COMMIT_ID} from "global-apps-common";

const RETRY_TIMEOUT = 1000;

class ChatRoomService extends Service {
  constructor(chatOTStateManager, publicKey, setNotNew) {
    super({
      messages: [],
      chatReady: false,
    });
    this._chatOTStateManager = chatOTStateManager;
    this._reconnectTimeout = null;
    this._resyncTimeout = null;
    this._publicKey = publicKey;
    this._setNotNew = setNotNew;
  }

  static createFrom(roomId, publicKey, setNotNew) {
    const chatRoomOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/room/' + roomId,
      serializer: chatRoomSerializer
    });
    const chatRoomStateManager = new OTStateManager(() => new Set(), chatRoomOTNode, chatRoomOTSystem);
    return new ChatRoomService(chatRoomStateManager, publicKey, setNotNew);
  }

  async init() {
    try {
      if (this._setNotNew) {
        this._chatOTStateManager.checkoutRoot(ROOT_COMMIT_ID);
      } else {
        await this._chatOTStateManager.checkout();
      }
    } catch (err) {
      console.log(err);

      const delay = this._retryDelay();
      this._reconnectTimeout = delay.timeoutId;
      await delay.promise;

      await this.init();
      return;
    }

    this._onStateChange();

    this._chatOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    clearTimeout(this._resyncTimeout);
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
      chatReady: true
    });
  };

  _getMessagesFromStateManager() {
    const otState = this._chatOTStateManager.getState();
    return [...otState]
      .map(key => JSON.parse(key))
      .sort((left, right) => left.timestamp - right.timestamp);
  }

  _retryDelay() {
    let timeoutId;
    const promise = new Promise(resolve => {
      timeoutId = setTimeout(resolve, RETRY_TIMEOUT);
    });
    return {timeoutId, promise};
  }

  async _sync() {
    try {
      await this._chatOTStateManager.sync();
      if (this._setNotNew && this._chatOTStateManager.getRevision() !== ROOT_COMMIT_ID) {
        this._setNotNew();
        this._setNotNew = null;
      }
    } catch (err) {
      console.log(err);

      const delay = this._retryDelay();
      this._resyncTimeout = delay.timeoutId;
      await delay.promise;

      await this._sync();
    }
  }
}

export default ChatRoomService;
