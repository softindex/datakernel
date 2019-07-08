import Store from '../../common/Store';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import serializer from "../ot/serializer";
import createMapOTSystem from "../ot/MapOTSystem";
import MapOTOperation from "../ot/MapOTOperation";

const RETRY_TIMEOUT = 1000;
const METADATA_COMPARATOR = (first, second) => {
  let result = first.title.localeCompare(second.title);
  if (result !== 0) return result;
  result = first.description.localeCompare(second.description);
  if (result !== 0) return result;
  return first.extension.localeCompare(second.extension);
};


class IndexService extends Store {
  constructor(videosOTStateManager) {
    super({
      videos: {},
      ready: false
    });

    this._videosOTStateManager = videosOTStateManager;
    this._reconnectTimeout = null;
    this._resyncTimeout = null;
  }

  static create() {
    const videosOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/metadata/',
      serializer: serializer
    });
    const otSystem = createMapOTSystem(METADATA_COMPARATOR);
    const videosOTStateManager = new OTStateManager(() => new Map(), videosOTNode, otSystem);
    return new IndexService(videosOTStateManager);
  }

  async init() {
    try {
      await this._videosOTStateManager.checkout();
    } catch (err) {
      console.error(err);

      const delay = this._retryDelay();
      this._reconnectTimeout = delay.timeoutId;
      await delay.promise;

      await this.init();
      return;
    }

    this._onStateChange();

    this._videosOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    clearTimeout(this._resyncTimeout);
    this._videosOTStateManager.removeChangeListener(this._onStateChange);
  }

  add(id, title, description, extension) {
    const operation = new MapOTOperation({
      [id]: {
        prev: null,
        next: {title, description, extension}
      }
    });
    this._videosOTStateManager.add([operation]);
    this._sync();
  };

  _onStateChange = () => {
    let state = this._videosOTStateManager.getState();
    this.setStore({
      videos: state,
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

  async _sync() {
    try {
      await this._videosOTStateManager.sync();
    } catch (err) {
      console.error(err);

      const delay = this._retryDelay();
      this._resyncTimeout = delay.timeoutId;
      await delay.promise;

      await this._sync();
    }
  }
}

export default IndexService;
