import Service from '../../common/Service';
import ContactsOTOperation from "./ot/ContactsOTOperation";
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import contactsOTSystem from "./ot/ContactsOTSystem";
import contactsSerializer from "./ot/serializer";
import {wait} from '../../common/utils';

const RETRY_TIMEOUT = 1000;

class ContactsService extends Service {
  constructor(contactsOTStateManager) {
    super({
      contacts: new Map(),
      contactsReady: false,
    });
    this._contactsOTStateManager = contactsOTStateManager;
    this._reconnectTimeout = null;
    this.getContactName = this.getContactName.bind(this);
  }

  static create() {
    const contactsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/contacts',
      serializer: contactsSerializer
    });
    const contactsOTStateManager = new OTStateManager(() => new Map(), contactsOTNode, contactsOTSystem);
    return new ContactsService(contactsOTStateManager);
  }

  async init() {
    try {
      await this._contactsOTStateManager.checkout();
    } catch (err) {
      console.log(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

    this._onStateChange();

    this._contactsOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._contactsOTStateManager.removeChangeListener(this._onStateChange);
  }

  addContact(pubKey, name) {
    let operation = new ContactsOTOperation(pubKey, name, false);
    this._contactsOTStateManager.add([operation]);

    this._sync();
  }

  async removeContact(pubKey, name) {
    let operation = new ContactsOTOperation(pubKey, name, true);
    this._contactsOTStateManager.add([operation]);

    await this._sync();
  }

  getContactName(publicKey) {
    if (this.state.contacts.get(publicKey)){
     return  this.state.contacts.get(publicKey).name
    }
  }

  _onStateChange = () => {
    this.setState({
      contacts: this._getContactsFromStateManager(),
      contactsReady: true
    });
  };

  _getContactsFromStateManager() {
    return new Map(
      [...this._contactsOTStateManager.getState()]
        .map(([pubKey, name]) => [pubKey, {name}])
    );
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._contactsOTStateManager.sync();
    } catch (err) {
      console.error(err);
      await wait(RETRY_TIMEOUT);
      await this._sync();
    }
  }
}

export default ContactsService;
