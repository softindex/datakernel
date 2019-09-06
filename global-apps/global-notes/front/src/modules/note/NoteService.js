import {ClientOTNode, OTStateManager} from 'ot-core/lib';
import {Service, ROOT_COMMIT_ID} from 'global-apps-common';
import DeleteOperation from './ot/operations/DeleteOperation';
import InsertOperation from './ot/operations/InsertOperation';
import serializer from '../note/ot/serializer';
import noteOTSystem from './ot/NoteOTSystem';

const RETRY_TIMEOUT = 1000;

class NoteService extends Service {
  constructor(noteOTStateManager, isNew) {
    super({
      content: '',
      ready: false
    });

    this._noteOTStateManager = noteOTStateManager;
    this._reconnectTimeout = null;
    this._resyncTimeout = null;
    this._isNew = isNew;
  }

  static create(noteId, isNew) {
    const noteOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/note/' + noteId,
      serializer: serializer
    });
    const noteOTStateManager = new OTStateManager(() => '', noteOTNode, noteOTSystem);

    return new NoteService(noteOTStateManager, isNew);
  }

  async init() {
    try {
      if (this._isNew) {
        this._noteOTStateManager.checkoutRoot(ROOT_COMMIT_ID);
      } else {
        await this._noteOTStateManager.checkout();
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
    this._noteOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    clearTimeout(this._resyncTimeout);
    this._noteOTStateManager.removeChangeListener(this._onStateChange);
  }

  insert(position, content) {
    this._applyOperations([
      new InsertOperation(position, content)
    ]);
  }

  delete(position, content) {
    this._applyOperations([
      new DeleteOperation(position, content)
    ]);
  }

  replace(position, oldContent, newContent) {
    this._applyOperations([
      new DeleteOperation(position, oldContent),
      new InsertOperation(position, newContent)
    ]);
  }

  _onStateChange = () => {
    const state = this._noteOTStateManager.getState();
    this.setState({
      content: state,
      ready: true
    });
  };

  _applyOperations(operations) {
    this._noteOTStateManager.add(operations);
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
      await this._noteOTStateManager.sync();

      if (this._isNew && this._noteOTStateManager.getRevision() !== ROOT_COMMIT_ID) {
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

export default NoteService;
