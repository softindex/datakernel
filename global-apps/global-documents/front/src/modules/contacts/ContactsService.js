import {Service, delay, retry} from 'global-apps-common';
import ContactsOTOperation from "./ot/ContactsOTOperation";

const RETRY_TIMEOUT = 1000;

class ContactsService extends Service {
  constructor(contactsOTStateManager, publicKey) {
    super({
      contacts: new Map(),
      ready: false
    });
    this._myPublicKey = publicKey;
    this._contactsOTStateManager = contactsOTStateManager;
    this._contactsCheckoutPromise = null;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static createFrom(contactsOTStateManager, publicKey) {
    return new ContactsService(
      contactsOTStateManager,
      publicKey
    );
  }

  async init() {
    this._contactsCheckoutPromise = retry(() => this._contactsOTStateManager.checkout(), RETRY_TIMEOUT);
    try {
      await this._contactsCheckoutPromise;
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
    this._contactsOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
    this._contactsCheckoutPromise.stop();
    this._contactsOTStateManager.removeChangeListener(this._onStateChange);
  }

  async addContact(publicKey, alias = '') {
    const operation = new ContactsOTOperation(publicKey, alias, false);
    this._contactsOTStateManager.add([operation]);
    await this._sync();
  }

  async removeContact(publicKey) {
    const alias = this.state.contacts.get(publicKey).name;
    let operation = new ContactsOTOperation(publicKey, alias, true);
    this._contactsOTStateManager.add([operation]);
    await this._sync();
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
        .map(([publicKey, name]) => [publicKey, {name}])
    );
  }

  async _sync() {
    try {
      await this._contactsOTStateManager.sync();
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

export default ContactsService;