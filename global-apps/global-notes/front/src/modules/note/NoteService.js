import Service from '../../common/Service';
import DeleteOperation from './ot/operations/DeleteOperation';
import InsertOperation from './ot/operations/InsertOperation';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import serializer from "../note/ot/serializer";
import noteOTSystem from "./ot/NoteOTSystem";

const RETRY_CHECKOUT_TIMEOUT = 1000;

class NoteService extends Service {
  constructor(noteOTStateManager) {
    super({
      content: '',
      ready: false
    });

    this._noteOTStateManager = noteOTStateManager;
    this._reconnectTimeout = null;
  }

  static from(noteId){
    const noteOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/note/' + noteId,
      serializer: serializer
    });
    const noteOTStateManager = new OTStateManager(() => '', noteOTNode, noteOTSystem);
    return new NoteService(noteOTStateManager);
  }

  async init() {
    // Get initial state
    try {
      await this._noteOTStateManager.checkout();
    } catch (err) {
      console.error(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

    this._onStateChange();

    this._noteOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
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
    let state = this._noteOTStateManager.getState();
    this.setState({
      content: state,
      ready: true
    });
  };

  _applyOperations(operations) {
    this._noteOTStateManager.add(operations);
    this._sync();
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_CHECKOUT_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._noteOTStateManager.sync();
    } catch (err) {
      console.error(err);
      await this._sync();
    }
  }
}

export default NoteService;
