import {isEqual} from 'lodash';
import * as types from '../MESSAGE_TYPES';

class CallOperation {
  constructor(prevCaller, nextCaller) {
    this._prevCaller = prevCaller;
    this._nextCaller = nextCaller;
  }

  static create(nextCaller) {
    return new CallOperation(null, nextCaller);
  }

  static createFromJson(json) {
    return new CallOperation(json.prev, json.next);
  }

  apply(state) {
    if (this._prevCaller === null) {
      if (state.call.callerInfo.publicKey) {
        const finishMessage = JSON.stringify({
          timestamp: state.call.timestamp,
          authorPublicKey: state.call.callerInfo.publicKey,
          authorPeerId: state.call.callerInfo.peerId,
          type: types.MESSAGE_DROP
        });
        state.messages.add(finishMessage);
      }

      const startMessage = JSON.stringify({
        timestamp: this._nextCaller.timestamp,
        authorPublicKey: this._nextCaller.pubKey,
        authorPeerId: this._nextCaller.peerId,
        type: types.MESSAGE_CALL
      });
      state.messages.add(startMessage);
      state.call.handled = new Map();
    }

    if (this._nextCaller === null) {
      const message = JSON.stringify({
        timestamp: this._prevCaller.timestamp,
        authorPublicKey: this._prevCaller.pubKey,
        authorPeerId: this._prevCaller.peerId,
        type: types.MESSAGE_CALL
      });
      state.messages.delete(message);
    }

    state.call.callerInfo = {
      publicKey: this._nextCaller ? this._nextCaller.pubKey : null,
      peerId: this._nextCaller ? this._nextCaller.peerId : null
    };
    state.call.timestamp = this._nextCaller ? this._nextCaller.timestamp : null;

    return state;
  }

  isEmpty() {
    return isEqual(this._prevCaller, this._nextCaller);
  }

  isEqual(callOperation) {
    return isEqual(callOperation._nextCaller, this._nextCaller) && isEqual(callOperation._prevCaller, this._prevCaller);
  }

  invert() {
    return new CallOperation(this._nextCaller, this._prevCaller);
  }

  getPrev() {
    return this._prevCaller;
  }

  getNext() {
    return this._nextCaller;
  }

  toJSON() {
    return {
      type: types.CALL,
      value: {
        prev: this._prevCaller,
        next: this._nextCaller
      }
    };
  }
}

export default CallOperation;
