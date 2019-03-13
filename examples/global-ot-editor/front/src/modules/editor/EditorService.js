import Service from '../../common/Service';
import DeleteOperation from './ot/operations/DeleteOperation';
import InsertOperation from './ot/operations/InsertOperation';

const RETRY_CHECKOUT_TIMEOUT = 1000;

class EditorService extends Service {
  constructor(editorOTStateManager, graphModel) {
    super({
      content: '',
      ready: false
    });

    this._editorOTStateManager = editorOTStateManager;
    this._reconnectTimeout = null;
    this._graphModel = graphModel;
  }

  async init() {
    // Get initial state
    try {
      await this._editorOTStateManager.checkout();
    } catch (err) {
      console.error(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

    this._onStateChange();

    this._editorOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._editorOTStateManager.removeChangeListener(this._onStateChange);
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
    this.setState({
      content: this._editorOTStateManager.getState(),
      ready: true
    });

    const revision = this._editorOTStateManager.getRevision();
    this._graphModel.getGraph(revision).then(commitsGraph => {
      if (revision === this._editorOTStateManager.getRevision()) {
        this.setState({
          commitsGraph
        });
      }
    });
  };

  _applyOperations(operations) {
    this._editorOTStateManager.add(operations);
    this._sync();
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_CHECKOUT_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._editorOTStateManager.sync();
    } catch (err) {
      console.error(err);
      await this._sync();
    }
  }
}

export default EditorService;
