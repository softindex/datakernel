import * as types from '../MESSAGE_TYPES';

class HandleCallOperation {
  constructor(mapOperation) {
    this._mapOperation = mapOperation;
  }

  static accept(publicKey, prev) {
    const mapOperation = new Map([[publicKey, {prev, next: true}]]);
    return new HandleCallOperation(mapOperation);
  }

  static reject(publicKey, prev) {
    const mapOperation = new Map([[publicKey, {prev, next: false}]]);
    return new HandleCallOperation(mapOperation);
  }

  static createFromJson(json) {
    return new HandleCallOperation(new Map(json));
  }

  apply(state) {
    for (const [publicKey, status] of this._mapOperation.entries()) {
      if (status.next === null) {
        state.call.handled.delete(publicKey);
      } else {
        state.call.handled.set(publicKey, status.next);
      }
    }

    return state;
  }

  isEmpty() {
    return [...this._mapOperation.values()].every(status => status.prev === null && status.next === null);
  }

  invert() {
    const mapOperation = new Map();

    for (const [publicKey, status] of this._mapOperation.entries()) {
      mapOperation.set(publicKey, {
        prev: status.next,
        next: status.prev
      });
    }

    return new HandleCallOperation(mapOperation);
  }

  isEqual(handleCallOperation) {
    if (this._mapOperation.size !== handleCallOperation._mapOperation.size) {
      return false;
    }

    for (const [publicKey, status] of this._mapOperation.entries()) {
      if (!handleCallOperation._mapOperation.has(publicKey)) {
        return false;
      }

      const operation = handleCallOperation.get(publicKey);

      if (operation.status.prev !== status.prev || operation.status.next !== status.next) {
        return false;
      }
    }

    return true;
  }

  getMapOperation() {
    return this._mapOperation;
  }

  toJSON() {
    return {
      type: types.HANDLE,
      value: [...this._mapOperation.entries()]
    };
  }
}

export default HandleCallOperation;
