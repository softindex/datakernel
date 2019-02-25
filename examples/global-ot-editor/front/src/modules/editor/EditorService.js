import Service from '../../common/Service';
import DeleteOperation from './ot/operations/DeleteOperation';
import InsertOperation from './ot/operations/InsertOperation';

const RETRY_CHECKOUT_TIMEOUT = 500;
const SYNC_INTERVAL = 500;

class EditorService extends Service {
  constructor(editorOTStateManager, graphModel) {
    super({
      content: '',
      ready: false
    });

    this._editorOTStateManager = editorOTStateManager;
    this._reconnectTimeout = null;
    this._syncInterval = null;
    this._graphModel = graphModel;
  }

  async init() {
    // Get initial state
    try {
      await this._editorOTStateManager.checkout();
    } catch (e) {
      this._reconnectTimeout = setTimeout(() => this.init(), RETRY_CHECKOUT_TIMEOUT);
      throw e;
    }


    this.setState({
      content: this._editorOTStateManager.getState(),
      ready: true
    });

    // Synchronization
    let syncing = false;
    this._syncInterval = setInterval(async () => {
      if (syncing) {
        return;
      }

      syncing = true;

      try {
        await this._editorOTStateManager.sync();
      } finally {
        syncing = false;
      }

      const revision = this._editorOTStateManager.getRevision();
      const commitsGraph = await this._graphModel.getGraph(revision);
      if (revision === this._editorOTStateManager.getRevision()) {
        this.setState({
          commitsGraph
        });
      }
    }, SYNC_INTERVAL);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    clearInterval(this._syncInterval);
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

  async _applyOperations(operations) {
    this._editorOTStateManager.add(operations);
    this.setState({
      content: this._editorOTStateManager.getState()
    });
  }
}

export default EditorService;
