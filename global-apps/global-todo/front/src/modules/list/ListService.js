import Service from '../../common/Service';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import serializer from "../ot/serializer";
import createMapOTSystem from "../ot/MapOTSystem";
import {ROOT_COMMIT_ID} from "../../common/utils";
import MapOTOperation from "../ot/MapOTOperation";

const RETRY_TIMEOUT = 1000;

class ListService extends Service {
  constructor(listOTStateManager, isNew) {
    super({
      items: {},
      ready: false
    });

    this._listOTStateManager = listOTStateManager;
    this._reconnectTimeout = null;
    this._resyncTimeout = null;
    this._isNew = isNew;
  }

  static createFrom(listId, isNew) {
    const listOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/list/' + listId,
      serializer: serializer
    });
    const otSystem = createMapOTSystem((first, second) => (first === second) ? 0 : (first ? 1 : -1));
    const listOTStateManager = new OTStateManager(() => new Map(), listOTNode, otSystem);
    return new ListService(listOTStateManager, isNew);
  }

  async init() {
    // Get initial state
    try {
      if (this._isNew) {
        this._listOTStateManager.checkoutRoot(ROOT_COMMIT_ID);
      } else {
        await this._listOTStateManager.checkout();
      }
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
    if (this._itemExists(name)) {
      // TODO maybe add deduplication or prohibit identical item names by showing error to user
      console.error('Item with this name already exists', name);
      return;
    }
    this._sendOperation(name, false);
  };

  deleteItem(name) {
    if (this._itemExists(name)) {
      return this._sendOperation(name, null);
    }
    console.error('No such item', name);
  };

  renameItem(name, newName) {
    if (name === newName) return;

    if (!this._itemExists(name)) {
      console.error('No such item', name);
      return;
    }
    if (this._itemExists(newName)) {
      // TODO maybe add deduplication or prohibit identical item names by showing error to user
      console.error('Item with this name already exists', name);
      return;
    }
    let value = this.state.items[name];
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

  changeItemState(name) {
    if (!this._itemExists(name)){
      console.error('No such item', name);
      return;
    }
    return this._sendOperation(name, !this.state.items[name]);
  };

  _sendOperation(name, next) {
    const operation = new MapOTOperation({
      [name]: {
        prev: this.state.items[name],
        next: next
      }
    });
    this._listOTStateManager.add([operation]);
    this._sync();
  };

  _onStateChange = () => {
    let state = this._listOTStateManager.getState();
    this.setState({
      items: state,
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
      if (this._isNew && this._listOTStateManager.getRevision() !== ROOT_COMMIT_ID) {
        this._isNew = false;
      }
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
