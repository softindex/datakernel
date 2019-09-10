import {Service} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import documentsOTSystem from "./ot/DocumentsOTSystem";
import documentsSerializer from "./ot/serializer";
import CreateOrDropDocument from "./ot/CreateOrDropDocument";
import {randomString, wait, toEmoji} from '../../common/utils';
import RenameDocument from "./ot/RenameDocument";

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
    this._reconnectTimeout = null;
    this._contactsService = contactsService;
    this._myPublicKey = pubicKey;
    this._getDocumentName.bind(this);
  }

  static createForm(contactsService, pubKey) {
    const documentsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/documents',
      serializer: documentsSerializer
    });
    const documentsOTStateManager = new OTStateManager(() => new Map(), documentsOTNode, documentsOTSystem);
    return new DocumentsService(documentsOTStateManager, contactsService, pubKey);
  }

  async init() {
    try {
      await this._documentsOTStateManager.checkout();
    } catch (err) {
      console.log(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

    this._onStateChange();

    this._documentsOTStateManager.addChangeListener(this._onStateChange);
    this._contactsService.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._documentsOTStateManager.removeChangeListener(this._onStateChange);
    this._contactsService.removeChangeListener(this._onStateChange);
  }

  createDocument(name, participants) {
    const documentId = randomString(DOCUMENT_ID_LENGTH);
    this._createDocument(documentId, name, [...participants, this._myPublicKey]);
    return documentId;
  }

  deleteDocument(documentId) {
    const document = this.state.documents.get(documentId);
    if (!document) {
      return;
    }

    const deleteDocumentOperation = new CreateOrDropDocument(documentId, document.name, document.participants, true);
    this._documentsOTStateManager.add([deleteDocumentOperation]);
    this._sync();
  }

  renameDocument(documentId, newDocumentName) {
    const document = this.state.documents.get(documentId);
    if (!document) {
      return;
    }

    const renameDocumentOperation = new RenameDocument(documentId, document.name, newDocumentName, Date.now());
    this._documentsOTStateManager.add([renameDocumentOperation]);
    this._sync();
  }

  _createDocument(documentId, documentName, participants) {
    const addDocumentOperation = new CreateOrDropDocument(documentId, documentName, participants, false);
    this._documentsOTStateManager.add([addDocumentOperation]);
    this.setState({
      ...this.state,
      newDocuments: new Set([...this.state.newDocuments, documentId])
    });
    this._sync();
  }

  _onStateChange = () => {
    this.setState({
      documents: this._getDocuments(),
      documentsReady: true
    });
  };

  _getDocumentName(document) {
    return document.participants
      .filter(participantPublicKey => participantPublicKey !== this._myPublicKey)
      .map(participantPublicKey => {
        return this._contactsService.getContactName(participantPublicKey) || toEmoji(participantPublicKey, 3);
      })
      .join(', ');
  }

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

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._documentsOTStateManager.sync();
    } catch (err) {
      console.log(err);
      await wait(RETRY_TIMEOUT);
      await this._sync();
    }
  }
}

export default DocumentsService;
