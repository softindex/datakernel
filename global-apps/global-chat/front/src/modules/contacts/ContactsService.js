import {Service, delay} from 'global-apps-common';
import ContactsOTOperation from "./ot/ContactsOTOperation";
import {retry} from 'global-apps-common';
import {createDialogRoomId, RETRY_TIMEOUT} from '../../common/utils';

class ContactsService extends Service {
  constructor(contactsOTStateManager, roomsService, publicKey) {
    super({
      contacts: new Map(),
      ready: false
    });
    this._myPublicKey = publicKey;
    this._contactsOTStateManager = contactsOTStateManager;
    this._roomsService = roomsService;
    this._contactsCheckoutPromise = null;
    this._roomsCheckoutPromise = null;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static createFrom(contactsOTStateManager, roomsService, publicKey) {
    return new ContactsService(
      contactsOTStateManager,
      roomsService,
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

    const dialogRoomId = await this._roomsService.createDialog(publicKey);
    await this._sync();

    return {
      dialogRoomId
    };
  }

  async removeContact(publicKey) {
    const alias = this.state.contacts.get(publicKey).name;
    let operation = new ContactsOTOperation(publicKey, alias, true);
    this._contactsOTStateManager.add([operation]);

    const dialogRoomId = createDialogRoomId(this._myPublicKey, publicKey);
    await this._roomsService.quitRoom(dialogRoomId);
    await this._sync();

    return {
      dialogRoomId
    };
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
