import {ClientOTNode, OTStateManager} from "ot-core/lib";
import {ROOT_COMMIT_ID, Service, delay} from "global-apps-common";
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
    this._reconnectDelay = null;
    this._resyncDelay = null;
    this._isNew = isNew;
  }

  static createFrom(documentId, isNew) {
    const documentOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/document/' + documentId,
      serializer
    });
    const documentStateManager = new OTStateManager(() => '', documentOTNode, editorOTSystem);
    return new DocumentService(documentStateManager, isNew);
  }

  async init() {
    try {
      if (this._isNew) {
        this._documentOTStateManager.checkoutRoot(ROOT_COMMIT_ID);
      } else {
        await this._documentOTStateManager.checkout();
      }
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

    this._documentOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
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

  async _sync() {
    try {
      await this._documentOTStateManager.sync();
      if (this._isNew && this._documentOTStateManager.getRevision() !== ROOT_COMMIT_ID) {
        this._isNew = null;
      }
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

export default DocumentService;
