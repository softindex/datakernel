import Service from '../../common/Service';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import createMapOTSystem from "../ot/MapOTSystem";
import MapOTOperation from "../ot/MapOTOperation";
import {randomString, wait} from '../../common/utils';
import serializer from "../ot/serializer";

const RETRY_TIMEOUT = 1000;

class ListsService extends Service {
  constructor(listsOTStateManager) {
    super({
      lists: {},
      ready: false,
      newLists: new Set()
    });
    this._listsOTStateManager = listsOTStateManager;
    this._reconnectTimeout = null;
  }

  static create() {
    const listsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/index',
      serializer: serializer
    });
    const otSystem = createMapOTSystem((left, right) => left.compareLocale(right));
    const listsOTStateManager = new OTStateManager(() => new Map(), listsOTNode, otSystem);
    return new ListsService(listsOTStateManager);
  }

  async init() {
    try {
      await this._listsOTStateManager.checkout();
    } catch (err) {
      console.error(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

    this._onStateChange();
    this._listsOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._listsOTStateManager.removeChangeListener(this._onStateChange);
  }

  createList(name) {
    const id = randomString(32);
    this._sendOperation(id, name);
    this.setState({
      ...this.state,
      newLists: new Set([...this.state.newLists, id])
    });
    return id;
  };

  renameList(id, newName) {
    const oldName = this.state.lists[id];
    if (oldName && oldName !== newName) {
      this._sendOperation(id, newName);
    }
  };

  deleteList(id) {
    if (this.state.lists[id]) {
      this._sendOperation(id, null)
    }
  };

  _sendOperation(id, next) {
    const listsOperation = new MapOTOperation({
      [id]: {
        prev: this.state.lists[id],
        next: next
      }
    });
    this._listsOTStateManager.add([listsOperation]);
    this._sync();
  };

  _onStateChange = () => {
    this.setState({
      lists: this._getLists(),
      ready: true
    });
  };

  _getLists() {
    return this._listsOTStateManager.getState();
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._listsOTStateManager.sync();
    } catch (err) {
      console.log(err);
      await wait(RETRY_TIMEOUT);
      await this._sync();
    }
  }
}

export default ListsService;

