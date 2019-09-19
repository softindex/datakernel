import {Service, delay} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core";
import serializer from "./ot/serializer";
import mapOTSystem from "./ot/mapOTSystem";
import MapOTOperation from "./ot/MapOTOperation";

const RETRY_TIMEOUT = 1000;

class ListService extends Service {
  constructor(listOTStateManager) {
    super({
      items: new Map(),
      ready: false
    });

    this._listOTStateManager = listOTStateManager;
    this._reconnectDelay = null;
    this._resyncDelay = null;
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

    this._listOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
    this._listOTStateManager.removeChangeListener(this._onStateChange);
  }

  async createItem(todoName) {
    const newName = Date.now() + Math.random().toFixed(10).substr(2) + todoName;
    if (todoName !== ' ' && todoName) {
      return this._sendOperationOnCreate(newName);
    }
  };

  async deleteItem(itemId) {
    return this._sendOperation(itemId,  null);
  };

  async renameItem(itemId, newName) {
    const name = this.state.items.get(itemId).name;
    if (name === newName) {
      return;
    }

    const value = this.state.items.get(itemId).isDone;
    const operation = new MapOTOperation({
      [itemId + name]: {
        prev: value,
        next: null
      },
      [itemId + newName]: {
        prev: null,
        next: value
      }
    });
    this._listOTStateManager.add([operation]);
    await this._sync();
  }

  async toggleItemStatus(itemId) {
    return this._sendOperation(itemId, !this.state.items.get(itemId).isDone);
  };

  async toggleAllItemsStatus() {
    return this._sendAllOperation();
  };

  async _sendOperation(itemId, nextValue) {
    const operation = new MapOTOperation({
      [itemId + this.state.items.get(itemId).name]: {
        prev: this.state.items.has(itemId) ? this.state.items.get(itemId).isDone : null,
        next: nextValue
      }
    });
    this._listOTStateManager.add([operation]);
    await this._sync();
  };

  async _sendOperationOnCreate(newName) {
    const operation = new MapOTOperation({
      [newName]: {
        prev: null,
        next: false
      }
    });
    this._listOTStateManager.add([operation]);
    await this._sync();
  };

  async _sendAllOperation() {
    let operations = [];
    let counterDone = 0;
    [...this.state.items].map(([, {isDone}]) => {
      if (isDone) {
        counterDone++;
      }
    });

    [...this.state.items].map(([itemId, {name}]) => {
      const operation = new MapOTOperation({
        [itemId + name]: {
          prev: this.state.items.has(itemId) ? this.state.items.get(itemId).isDone : null,
          next: this.state.items.size !== counterDone
        }
      });
      operations.push(operation);
    });

    for (let i = 0; i < operations.length; i++) {
      this._listOTStateManager.add([operations[i]]);
    }
    await this._sync();
  };

  _onStateChange = () => {
    const items = Object.entries(this._listOTStateManager.getState())
      .map(([name, isDone]) => (
        [name.slice(0, 23), {
          name: name.slice(23),
          isDone: isDone
        }]
      ));
    this.setState({
      items: new Map(items),
      ready: true
    });
  };

  async _sync() {
    try {
      await this._listOTStateManager.sync();
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

export default ListService;
