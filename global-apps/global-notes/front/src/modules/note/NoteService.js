import {ClientOTNode, OTStateManager} from 'ot-core/lib';
import {Service, delay} from 'global-apps-common';
import DeleteOperation from './ot/operations/DeleteOperation';
import InsertOperation from './ot/operations/InsertOperation';
import serializer from '../note/ot/serializer';
import noteOTSystem from './ot/NoteOTSystem';

const RETRY_TIMEOUT = 1000;

class NoteService extends Service {
  constructor(noteOTStateManager) {
    super({
      content: '',
      ready: false
    });

    this._noteOTStateManager = noteOTStateManager;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static create(noteId) {
    const noteOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/note/' + noteId,
      serializer: serializer
    });
    const noteOTStateManager = new OTStateManager(() => '', noteOTNode, noteOTSystem);

    return new NoteService(noteOTStateManager);
  }

  async init() {
    try {
      await this._noteOTStateManager.checkout();
    } catch (err) {
      console.error(err);
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
    this._noteOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
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

  async _sync() {
    try {
      await this._noteOTStateManager.sync();
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

export default NoteService;
