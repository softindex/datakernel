import {Service, delay, retry, RejectionError} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import chatRoomSerializer from "./ot/serializer";
import chatRoomOTSystem from "./ot/ChatRoomOTSystem";
import {RETRY_TIMEOUT} from '../../common/utils';
import MessageOperation from './ot/MessageOperation';
import CallOperation from './ot/CallOperation';
import HandleCallOperation from './ot/HandleCallOperation';
import DropCallOperation from './ot/DropCallOperation';
import initOTState from './ot/initOTState';
import CallsValidationService from '../calls/CallsValidationService';

class ChatRoomService extends Service {
  constructor(chatOTStateManager, publicKey, callsService, callsValidationService) {
    super({
      messages: [],
      call: {
        callerInfo: {
          publicKey: null,
          peerId: null
        },
        timestamp: null,
        handled: new Map()
      },
      chatReady: false,
      isHostValid: false
    });
    this._chatOTStateManager = chatOTStateManager;
    this._publicKey = publicKey;
    this._callsService = callsService;
    this._callsValidationService = callsValidationService;
    this._checkoutRetry = null;
    this._resyncDelay = null;
    this._joinPromise = null;
  }

  static createFrom(roomId, publicKey, callsService) {
    const chatRoomOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/room/' + roomId,
      serializer: chatRoomSerializer
    });
    const chatRoomStateManager = new OTStateManager(initOTState, chatRoomOTNode, chatRoomOTSystem);
    const callsValidationService = new CallsValidationService(callsService);

    return new ChatRoomService(chatRoomStateManager, publicKey, callsService, callsValidationService);
  }

  async init() {
    this._checkoutRetry = retry(async () => await this._chatOTStateManager.checkout(), RETRY_TIMEOUT);

    try {
      await this._checkoutRetry;
    } catch (err) {
      if (!(err instanceof RejectionError)) {
        console.error(err);
      }

      return;
    }

    this._onStateChange();
    this._chatOTStateManager.addChangeListener(this._onStateChange);
    this._callsValidationService.addChangeListener(this._onHostValidChange);
  }

  stop() {
    if (this._checkoutRetry) {
      this._checkoutRetry.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }

    this._callsValidationService.stop();
    this._chatOTStateManager.removeChangeListener(this._onStateChange);
    this._callsValidationService.removeChangeListener(this._onHostValidChange);
  }

  async sendMessage(content) {
    const timestamp = Date.now();
    const operation = new MessageOperation(timestamp, this._publicKey, content, false);
    await this._addOperation(operation);
  }

  async startCall() {
    await this._callsService.hostCall();
    this._callsService.addListener('finish', this._onCallFinish);
    this.setState({
      isHostValid: true
    });
    const operation = CallOperation.create({
      pubKey: this._publicKey,
      peerId: this._callsService.state.peerId,
      timestamp: Date.now()
    });
    await this._addOperation(operation);
  }

  async acceptCall() {
    this._callsService.addListener('finish', this._onCallFinish);
    this._callsService.addListener('disconnect', this._onCallDisconnect);
    this._callsValidationService.stop();
    this._joinPromise = this._callsService.joinCall(this.state.call.callerInfo);

    try {
      await this._joinPromise;
    } catch (error) {
      if (!(error instanceof RejectionError)) {
        throw error;
      }
    }

    this._joinPromise = null;

    if (this.state.call.callerInfo.publicKey) {
      const operation = HandleCallOperation.accept(this._publicKey, null);
      await this._addOperation(operation);
    }
  }

  async declineCall() {
    const operation = HandleCallOperation.reject(this._publicKey, null);
    await this._addOperation(operation);
  }

  finishCall() {
    this._callsService.finishCall();
  }

  async _addOperation(operation) {
    this._chatOTStateManager.add([operation]);
    await this._sync();
  }

  _onStateChange = () => {
    const otState = this._chatOTStateManager.getState();
    const oldPeerId = this.state.call.callerInfo.peerId;
    const newPeerId = otState.call.callerInfo.peerId;
    const callFinished = oldPeerId && newPeerId === null;
    const inThisCall = Boolean(newPeerId) && newPeerId === this._callsService.state.hostPeerId;

    this.setState({
      messages: [...otState.messages]
        .map(key => JSON.parse(key))
        .sort((left, right) => left.timestamp - right.timestamp),
      chatReady: true,
      call: {...otState.call},
      isHostValid: this.state.isHostValid || inThisCall
    });

    if (newPeerId !== oldPeerId) {
      this._callsValidationService.stop();
    }

    if (
      ![null, oldPeerId, this._callsService.state.peerId].includes(newPeerId)
      && newPeerId !== this._callsService.state.hostPeerId
    ) {
      this._callsValidationService.start(otState.call.callerInfo);

      if (oldPeerId === this._callsService.state.hostPeerId) {
        this._callsService.finishCall();
      }
    }

    if (callFinished && this._joinPromise) {
      this._joinPromise.cancel();
      this._joinPromise = null;
    }
  };

  async _sync() {
    try {
      await this._chatOTStateManager.sync();
    } catch (err) {
      console.log(err);

      this._resyncDelay = delay(RETRY_TIMEOUT);
      try {
        await this._resyncDelay;
      } catch (err) {
        return;
      }

      await this._sync();
    }
  }

  _onCallFinish = async hostPeerId => {
    if (hostPeerId !== this.state.call.callerInfo.peerId) {
      this.setState({
        isHostValid: false
      });
      return;
    }

    let operation;

    if (hostPeerId === this._callsService.state.peerId) {
      operation = DropCallOperation.create(
        this._publicKey,
        this.state.call.callerInfo.peerId,
        this.state.call.timestamp,
        this.state.call.handled,
        Date.now()
      );
    } else {
      operation = HandleCallOperation.reject(this._publicKey, this.state.call.handled.get(this._publicKey) || null);
      this._callsValidationService.start(this.state.call.callerInfo);
    }

    this._callsService.removeListener('finish', this._onCallFinish);
    this._callsService.removeListener('disconnect', this._onCallDisconnect);
    await this._addOperation(operation);
  };

  _onCallDisconnect = hostPeerId => {
    if (hostPeerId === this.state.call.callerInfo.peerId) {
      this.setState({
        isHostValid: false
      });

      this._callsService.removeListener('finish', this._onCallFinish);
      this._callsService.removeListener('disconnect', this._onCallDisconnect);
      this._callsValidationService.start(this.state.call.callerInfo);
    }
  };

  _onHostValidChange = isHostValid => {
    this.setState({
      isHostValid
    });
  }
}

export default ChatRoomService;
