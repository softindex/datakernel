import {Service, randomString, delay} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import notesOTSystem from "./ot/NotesOTSystem";
import NotesOTOperation from "./ot/NotesOTOperation";
import serializer from "../notes/ot/serializer";

const RETRY_TIMEOUT = 1000;

class NotesService extends Service {
  constructor(notesOTStateManager) {
    super({
      notes: {},
      ready: false
    });
    this._notesOTStateManager = notesOTStateManager;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static create() {
    const notesOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/notes',
      serializer: serializer
    });
    const notesOTStateManager = new OTStateManager(() => new Map(), notesOTNode, notesOTSystem);

    return new NotesService(notesOTStateManager);
  }

  async init() {
    try {
      await this._notesOTStateManager.checkout();
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
    this._notesOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
    this._notesOTStateManager.removeChangeListener(this._onStateChange);
  }

  async createNote(name) {
    const id = randomString(32);
    await this._sendOperation(id, name);

    return id;
  };

  async renameNote(id, newName) {
    if (this.state.notes[id]) {
      await this._sendOperation(id, newName);
    }
  };

  async deleteNote(id) {
    if (this.state.notes[id]) {
      await this._sendOperation(id, null)
    }
  };

  async _sendOperation(id, next) {
    const notesOperation = new NotesOTOperation({
      [id]: {
        prev: this.state.notes[id],
        next: next
      }
    });
    this._notesOTStateManager.add([notesOperation]);
    await this._sync();
  };

  _onStateChange = () => {
    this.setState({
      notes: this._getNotes(),
      ready: true
    });
  };

  _getNotes() {
    return this._notesOTStateManager.getState();
  }

  async _sync() {
    try {
      await this._notesOTStateManager.sync();
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

export default NotesService;

