import Service from '../../common/Service';
import {ClientOTNode, OTStateManager} from "ot-core";
import serializer from "../ot/serializer";
import mapOTSystem from "../ot/mapOTSystem";
import MapOTOperation from "../ot/MapOTOperation";

const RETRY_TIMEOUT = 1000;

class ListService extends Service {
  constructor(listOTStateManager) {
    super({
      items: {},
      ready: false
    });

    this._listOTStateManager = listOTStateManager;
    this._reconnectTimeout = null;
    this._resyncTimeout = null;
  }

  static create() {
    const listOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/list/',
      serializer: serializer
    });
    const listOTStateManager = new OTStateManager(() => new Map(), listOTNode, mapOTSystem);
    return new ListService(listOTStateManager);
  }

  async init() {
    try {
      await this._listOTStateManager.checkout();
    } catch (err) {
      console.error(err);

      const delay = this._retryDelay();
      this._reconnectTimeout = delay.timeoutId;
      await delay.promise;

      await this.init();
      return;
    }

    this._onStateChange();

    this._listOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    clearTimeout(this._resyncTimeout);
    this._listOTStateManager.removeChangeListener(this._onStateChange);
  }

  createItem(name) {
    if (!name) {
      this._sendOperation(name, false);
    }
  };

  deleteItem(name) {
    return this._sendOperation(name, null);
  };

  renameItem(name, newName) {
    if (name === newName) {
      return;
    }

    const value = this.state.items[name];
    const operation = new MapOTOperation({
      [name]: {
        prev: value,
        next: null
      },
      [newName]: {
        prev: null,
        next: value
      }
    });
    this._listOTStateManager.add([operation]);
    this._sync();
  }

  toggleItemStatus(name) {
    return this._sendOperation(name, !this.state.items[name]);
  };

  toggleAllItemsStatus(name, nextValue) {
    return this._sendAllOperation(name, nextValue);
  };

  _sendOperation(name, nextValue) {
    const operation = new MapOTOperation({
      [name]: {
        prev: name in this.state.items ? this.state.items[name] : null,
        next: nextValue
      }
    });
    this._listOTStateManager.add([operation]);
    this._sync();
  };

  _sendAllOperation(names, nextValue) {
    let operations = [];
    names.forEach(name => {
      const operation = new MapOTOperation({
        [name]: {
          prev: name in this.state.items ? this.state.items[name] : null,
          next: nextValue
        }
      });
      operations.push(operation);
    });
    operations.forEach(operation => {
      this._listOTStateManager.add([operation]);
    });
    this._sync();
  };

  _onStateChange = () => {
    this.setState({
      items: this._listOTStateManager.getState(),
      ready: true
    });
  };

  _retryDelay() {
    let timeoutId;
    const promise = new Promise(resolve => {
      timeoutId = setTimeout(resolve, RETRY_TIMEOUT);
    });
    return {timeoutId, promise};
  }

  _itemExists = name => typeof this.state.items[name] === 'boolean';

  async _sync() {
    try {
      await this._listOTStateManager.sync();
    } catch (err) {
      console.error(err);

      const delay = this._retryDelay();
      this._resyncTimeout = delay.timeoutId;
      await delay.promise;

      await this._sync();
    }
  }
}

export default ListService;
