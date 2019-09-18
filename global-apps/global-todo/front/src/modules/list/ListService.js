import {Service, delay} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core";
import serializer from "./ot/serializer";
import mapOTSystem from "./ot/mapOTSystem";
import MapOTOperation from "./ot/MapOTOperation";

const RETRY_TIMEOUT = 1000;

class ListService extends Service {
  constructor(listOTStateManager) {
    super({
      items: {},
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
    const name = Date.now() + ';' + todoName;
    if (todoName !== ' ' && todoName) {
      return this._sendOperation(name, false);
    }
  };

  async deleteItem(name) {
    return this._sendOperation(name, null);
  };

  async renameItem(name, newName) {
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
    await this._sync();
  }

  async toggleItemStatus(name) {
    return this._sendOperation(name, !this.state.items[name]);
  };

  async toggleAllItemsStatus() {
    return this._sendAllOperation();
  };

  async _sendOperation(name, nextValue) {
    const operation = new MapOTOperation({
      [name]: {
        prev: name in this.state.items ? this.state.items[name] : null,
        next: nextValue
      }
    });
    this._listOTStateManager.add([operation]);
    await this._sync();
  };

  async _sendAllOperation() {
    let operations = [];
    let counterDone = 0;
    Object.entries(this.state.items).map(([, isDone]) => {
     if (isDone) {
       counterDone++;
     }
    });

    Object.entries(this.state.items).map(([name,]) => {
      const operation = new MapOTOperation({
        [name]: {
          prev: name in this.state.items ? this.state.items[name] : null,
          next: !(Object.keys(this.state.items).length === counterDone)
        }
      });
      operations.push(operation);
    });

    for (let i=0; i<operations.length; i++) {
      this._listOTStateManager.add([operations[i]]);
    }
    await this._sync();
  };

  _onStateChange = () => {
    this.setState({
      items: this._listOTStateManager.getState(),
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
