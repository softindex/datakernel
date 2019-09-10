import {Service} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import {ROOT_COMMIT_ID} from "../../common/utils";
import InsertOperation from "./ot/InsertOperation";
import DeleteOperation from "./ot/DeleteOperation";
import serializer from "./ot/serializer";
import editorOTSystem from "./ot/editorOTSystem";

const RETRY_TIMEOUT = 1000;

class DocumentService extends Service {
  constructor(documentOTStateManager, isNew) {
    super({
      content: '',
      ready: false,
    });
    this._documentOTStateManager = documentOTStateManager;
    this._reconnectTimeout = null;
    this._resyncTimeout = null;
    this._isNew = isNew;
  }

  static createFrom(documentId, setNotNew) {
    const documentOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/document/' + documentId,
      serializer
    });
    const documentStateManager = new OTStateManager(() => '', documentOTNode, editorOTSystem);
    return new DocumentService(documentStateManager, setNotNew);
  }

  async init() {
    // Get initial state
    try {
      if (this._isNew) {
        this._documentOTStateManager.checkoutRoot(ROOT_COMMIT_ID);
      } else {
        await this._documentOTStateManager.checkout();
      }
    } catch (err) {
      console.log(err);

      const delay = this._retryDelay();
      this._reconnectTimeout = delay.timeoutId;
      await delay.promise;

      await this.init();
      return;
    }

    this._onStateChange();

    this._documentOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    clearTimeout(this._resyncTimeout);
    this._documentOTStateManager.removeChangeListener(this._onStateChange);
  }

  insert(position, content) {
    this._applyOperations([new InsertOperation(position, content)]);
  }

  delete(position, content) {
    this._applyOperations([new DeleteOperation(position, content)]);
  }

  replace(position, oldContent, newContent) {
    this._applyOperations([
      new DeleteOperation(position, oldContent),
      new InsertOperation(position, newContent)
    ]);
  }

  _onStateChange = () => {
    const state = this._documentOTStateManager.getState();
    this.setState({
      content: state,
      ready: true
    });
  };

  _applyOperations(operations) {
    this._documentOTStateManager.add(operations);
    this._sync();
  }

  _retryDelay() {
    let timeoutId;
    const promise = new Promise(resolve => {
      timeoutId = setTimeout(resolve, RETRY_TIMEOUT);
    });
    return {timeoutId, promise};
  }

  async _sync() {
    try {
      await this._documentOTStateManager.sync();
      if (this._isNew && this._documentOTStateManager.getRevision() !== ROOT_COMMIT_ID) {
        this._isNew = null;
      }
    } catch (err) {
      console.log(err);

      const delay = this._retryDelay();
      this._resyncTimeout = delay.timeoutId;
      await delay.promise;

      await this._sync();
    }
  }
}

export default DocumentService;
