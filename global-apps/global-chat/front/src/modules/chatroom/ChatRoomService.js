import {Service, delay} from 'global-apps-common';
import ChatRoomOTOperation from './ot/ChatRoomOTOperation';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import chatRoomSerializer from "./ot/serializer";
import chatRoomOTSystem from "./ot/ChatRoomOTSystem";
import {RETRY_TIMEOUT} from '../../common/utils'

class ChatRoomService extends Service {
  constructor(chatOTStateManager, publicKey) {
    super({
      messages: [],
      chatReady: false
    });
    this._chatOTStateManager = chatOTStateManager;
    this._reconnectDelay = null;
    this._resyncDelay = null;
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

      this._reconnectDelay = delay(RETRY_TIMEOUT);
      try {
        await this._reconnectDelay.promise;
      } catch (err) {
        return;
      }

      await this.init();
      return;
    }

    this._onStateChange();

    this._chatOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
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

  async _sync() {
    try {
      await this._chatOTStateManager.sync();
    } catch (err) {
      console.log(err);

      this._resyncDelay = delay(RETRY_TIMEOUT);
      try {
        await this._resyncDelay.promise;
      } catch (err) {
        return;
      }

      await this._sync();
    }
  }
}

export default ChatRoomService;
