import Service from '../../common/Service';
import ContactsOTOperation from "./ot/ContactsOTOperation";
import {wait, retry, getDialogRoomId} from '../../common/utils';
import GlobalAppStoreAPI from "../../common/GlobalAppStoreAPI";

const RETRY_TIMEOUT = 1000;

class ContactsService extends Service {
  constructor(contactsOTStateManager, roomsOTStateManager, roomsService, globalAppStoreAPI, publicKey) {
    super({
      contacts: new Map(),
      users: new Map(),
      contactsReady: false,
    });
    this._myPublicKey = publicKey;
    this._contactsOTStateManager = contactsOTStateManager;
    this._roomsOTStateManager = roomsOTStateManager;
    this._roomsService = roomsService;
    this._globalAppStoreAPI = globalAppStoreAPI;
    this._contactsCheckoutPromise = null;
    this._roomsCheckoutPromise = null;
    this._appStoreNamesPromise = null;
  }

  static createFrom(contactsOTStateManager, roomsOTStateManager, roomsService, publicKey) {
    return new ContactsService(contactsOTStateManager, roomsOTStateManager, roomsService,
      GlobalAppStoreAPI.create(), publicKey);
  }

  async init() {
    this._contactsCheckoutPromise = retry(() => this._contactsOTStateManager.checkout(), RETRY_TIMEOUT);
    this._roomsCheckoutPromise = retry(() => this._roomsOTStateManager.checkout(), RETRY_TIMEOUT);

    const [, appStoreUsers]  = await Promise.all([
      this._contactsCheckoutPromise,
      this._roomsCheckoutPromise.then(() => {
        this._appStoreNamesPromise = retry(() => this._getAppStoreNames(), RETRY_TIMEOUT);
        return this._appStoreNamesPromise;
      })
    ]);

    this.setState({
      users: appStoreUsers
    });

    this._onStateChange();

    this._contactsOTStateManager.addChangeListener(this._onStateChange);
    this._roomsOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    this._contactsCheckoutPromise.stop();
    this._roomsCheckoutPromise.stop();
    this._appStoreNamesPromise.stop();
    this._contactsOTStateManager.removeChangeListener(this._onStateChange);
    this._roomsOTStateManager.removeChangeListener(this._onStateChange);
  }

  async addContact(publicKey, name) {
    let operation = new ContactsOTOperation(publicKey, name, false);
    this._contactsOTStateManager.add([operation]);

    await this._roomsService.createDialog(publicKey);
    await this._sync();
  }

  async removeContact(publicKey, name) {
    let operation = new ContactsOTOperation(publicKey, name, true);
    this._contactsOTStateManager.add([operation]);

    const roomId = getDialogRoomId([this._myPublicKey, publicKey]);
    await this._roomsService.quitRoom(roomId);
    await this._sync();
  }

  _getRooms() {
    return [...this._roomsOTStateManager.getState()]
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

  _getUserByPublicKey(publicKey) {
    return this._globalAppStoreAPI.getUserByPublicKey(publicKey);
  }

  async _getAppStoreNames() {
    let users = this.state.users;
    this._getRooms().map(([, {participants}]) => {
      participants
        .filter(publicKey => publicKey !== this._myPublicKey)
        .forEach(publicKey => {
          this._getUserByPublicKey(publicKey)
            .then(user => users.set(publicKey, user)) // TODO
            .catch(error => console.error(error))
        })
    });
    return users;
  }

  async _sync() {
    try {
      await this._contactsOTStateManager.sync();
      await this._roomsOTStateManager.sync();
    } catch (err) {
      console.error(err);
      await wait(RETRY_TIMEOUT);
      await this._sync();
    }
  }
}

export default ContactsService;
