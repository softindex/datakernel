import {isEqual} from 'lodash';
import * as types from '../types';

class DropCallOperation {
  constructor(pubKey, peerId, timestamp, handled, dropTime, invert) {
    this._pubKey = pubKey;
    this._peerId = peerId;
    this._timestamp = timestamp;
    this._handled = handled;
    this._dropTime = dropTime;
    this._invert = invert;
  }

  static EMPTY = new DropCallOperation(null, null,null, new Map(), -1, false);

  static create(pubKey, peerId, timestamp, handled, dropTime) {
    return new DropCallOperation(pubKey, peerId, timestamp, handled, dropTime, false);
  }

  static createFromJson(json) {
    return new DropCallOperation(
      json.callInfo.pubKey,
      json.callInfo.peerId,
      json.callInfo.timestamp,
      new Map(json.handled),
      json.dropTimestamp,
      json.invert
    );
  }

  apply(state) {
    const message = JSON.stringify({
      timestamp: this._dropTime,
      authorPublicKey: this._pubKey,
      type: types.MESSAGE_DROP
    });

    if (this._invert) {
      state.call.callerInfo = {
        publicKey: this._pubKey,
        peerId: this._peerId
      };
      state.call.timestamp = this._timestamp;
      state.call.handled = this._handled;
      state.messages.delete(message);
    } else {
      state.call.callerInfo = {
        publicKey: null,
        peerId: null
      };
      state.call.timestamp = null;
      state.call.handled = new Map();
      state.messages.add(message);
    }

    return state;
  }

  isEmpty() {
    return this._dropTime === -1;
  }

  invert() {
    return new DropCallOperation(
      this._pubKey,
      this._peerId,
      this._timestamp,
      this._handled,
      this._dropTime,
      !this._invert
    );
  }

  getPubKey() {
    return this._pubKey;
  }

  getPeerId() {
    return this._peerId;
  }

  getTimestamp() {
    return this._timestamp;
  }

  getHandled() {
    return this._handled;
  }

  getDropTime() {
    return this._dropTime;
  }

  isInvert() {
    return this._invert;
  }

  isInversionFor(dropCallOperation) {
    return (
      this._pubKey === dropCallOperation._pubKey
      && this._peerId === dropCallOperation._peerId
      && this._timestamp === dropCallOperation._timestamp
      && isEqual(this._handled, dropCallOperation._handled)
      && this._dropTime === dropCallOperation._dropTime
      && this._invert !== dropCallOperation._invert
    );
  }

  isEqual(dropCallOperation) {
    return (
      this._pubKey === dropCallOperation._pubKey
      && this._peerId === dropCallOperation._pubKey
      && this._timestamp === dropCallOperation._timestamp
      && isEqual(this._handled, dropCallOperation._handled)
      && this._dropTime === dropCallOperation._dropTime
      && this._invert === dropCallOperation._invert
    );
  }

  toJSON() {
    return {
      type: types.DROP,
      value: {
        callInfo: {
          pubKey: this._pubKey,
          peerId: this._peerId,
          timestamp: this._timestamp,
        },
        handled: [...this._handled.entries()],
        dropTimestamp: this._dropTime,
        invert: this._invert
      }
    }
  }
}

export default DropCallOperation;
