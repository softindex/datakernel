import {ClientOTNode, OTStateManager} from "ot-core/lib";
import documentsOTSystem from "./ot/DocumentsOTSystem";
import documentsSerializer from "./ot/serializer";
import CreateOrDropDocument from "./ot/CreateOrDropDocument";
import {randomString, Service} from 'global-apps-common';
import RenameDocument from "./ot/RenameDocument";
import {delay} from "global-apps-common/lib";

const RETRY_TIMEOUT = 1000;
const DOCUMENT_ID_LENGTH = 32;

class DocumentsService extends Service {
  constructor(documentsOTStateManager, contactsService, pubicKey) {
    super({
      documents: new Map(),
      documentsReady: false,
      newDocuments: new Set()
    });
    this._documentsOTStateManager = documentsOTStateManager;
    this._reconnectDelay = null;
    this._resyncDelay = null;
    this._contactsService = contactsService;
    this._myPublicKey = pubicKey;
  }

  static createFrom(contactsService, publicKey) {
    const documentsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/documents',
      serializer: documentsSerializer
    });
    const documentsOTStateManager = new OTStateManager(() => new Map(), documentsOTNode, documentsOTSystem);
    return new DocumentsService(documentsOTStateManager, contactsService, publicKey);
  }

  async init() {
    try {
      await this._documentsOTStateManager.checkout();
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

    this._documentsOTStateManager.addChangeListener(this._onStateChange);
    this._contactsService.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
    this._documentsOTStateManager.removeChangeListener(this._onStateChange);
    this._contactsService.removeChangeListener(this._onStateChange);
  }

  async createDocument(name, participants) {
    const documentId = randomString(DOCUMENT_ID_LENGTH);
    await this._createDocument(documentId, name, [...participants, this._myPublicKey]);
    return documentId;
  }

  async deleteDocument(documentId) {
    const document = this.state.documents.get(documentId);
    if (!document) {
      return;
    }
    const deleteDocumentOperation = new CreateOrDropDocument(documentId, document.name, document.participants, true);
    this._documentsOTStateManager.add([deleteDocumentOperation]);
    await this._sync();
    return documentId;
  }

  async renameDocument(documentId, newDocumentName) {
    const document = this.state.documents.get(documentId);
    if (!document) {
      return;
    }

    const renameDocumentOperation = new RenameDocument(documentId, document.name, newDocumentName, Date.now());
    this._documentsOTStateManager.add([renameDocumentOperation]);
    await this._sync();
  }

  async _createDocument(documentId, documentName, participants) {
    const addDocumentOperation = new CreateOrDropDocument(documentId, documentName, participants, false);
    this._documentsOTStateManager.add([addDocumentOperation]);
    this.setState({
      ...this.state,
      newDocuments: new Set([...this.state.newDocuments, documentId])
    });
    await this._sync();
  }

  _onStateChange = () => {
    this.setState({
      documents: this._getDocuments(),
      documentsReady: true
    });
  };

  _getDocuments() {
    const otState = [...this._documentsOTStateManager.getState()]
      .map(([documentId, document]) => {
        return {
          id: documentId,
          name: document.name,
          participants: document.participants,
          virtual: false
        }
      });

    return new Map([
      ...otState
    ].map(({id, name, participants, virtual}) => ([id, {name, participants, virtual}])));
  }

  async _sync() {
    try {
      await this._documentsOTStateManager.sync();
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

export default DocumentsService;
