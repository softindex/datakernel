import {Service, randomString, wait} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import notesOTSystem from "./ot/NotesOTSystem";
import NotesOTOperation from "./ot/NotesOTOperation";
import serializer from "../notes/ot/serializer";

const RETRY_TIMEOUT = 1000;

class NotesService extends Service {
  constructor(notesOTStateManager) {
    super({
      notes: {},
      ready: false,
      newNotes: new Set()
    });
    this._notesOTStateManager = notesOTStateManager;
    this._reconnectTimeout = null;
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
      console.error(err);
      await this._reconnectDelay();
      await this.init();

      return;
    }

    this._onStateChange();
    this._notesOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._notesOTStateManager.removeChangeListener(this._onStateChange);
  }

  async createNote(name) {
    const id = randomString(32);
    this._sendOperation(id, name);
    this.setState({
      newNotes: new Set([...this.state.newNotes, id])
    });

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

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._notesOTStateManager.sync();
    } catch (err) {
      console.log(err);
      await wait(RETRY_TIMEOUT);
      await this._sync();
    }
  }
}

export default NotesService;

