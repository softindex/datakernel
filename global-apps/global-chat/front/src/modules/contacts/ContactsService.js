import Service from '../../common/Service';
import ContactsOTOperation from "./ot/ContactsOTOperation";
import {wait, retry, createDialogRoomId, toEmoji} from '../../common/utils';
import GlobalAppStoreAPI from "../../common/GlobalAppStoreAPI";
import ProfileService from "../profiles/ProfileService";

const RETRY_TIMEOUT = 1000;

class ContactsService extends Service {
  constructor(contactsOTStateManager, roomsOTStateManager, roomsService,
              globalAppStoreAPI, publicKey, profileServiceCreate) {
    super({
      contacts: new Map(),
      names: new Map(),
      contactsReady: false
    });
    this._myPublicKey = publicKey;
    this._contactsOTStateManager = contactsOTStateManager;
    this._roomsOTStateManager = roomsOTStateManager;
    this._roomsService = roomsService;
    this._globalAppStoreAPI = globalAppStoreAPI;
    this._profileServiceCreate = profileServiceCreate;
    this._contactsCheckoutPromise = null;
    this._roomsCheckoutPromise = null;
  }

  static createFrom(contactsOTStateManager, roomsOTStateManager, roomsService, publicKey) {
    return new ContactsService(contactsOTStateManager, roomsOTStateManager, roomsService,
      GlobalAppStoreAPI.create(process.env.REACT_APP_GLOBAL_OAUTH_LINK), publicKey,
      contactPublicKey => {return ProfileService.create(contactPublicKey)});
  }

  async init() {
    this._contactsCheckoutPromise = retry(() => this._contactsOTStateManager.checkout(), RETRY_TIMEOUT);
    this._roomsCheckoutPromise = retry(() => this._roomsOTStateManager.checkout(), RETRY_TIMEOUT);

    await Promise.all([
      this._contactsCheckoutPromise,
      this._roomsCheckoutPromise.then(() => {
      })
    ]);

    this._onStateChange();

    this._contactsOTStateManager.addChangeListener(this._onStateChange);
    this._roomsOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    this._contactsCheckoutPromise.stop();
    this._roomsCheckoutPromise.stop();
    this._contactsOTStateManager.removeChangeListener(this._onStateChange);
    this._roomsOTStateManager.removeChangeListener(this._onStateChange);
  }

  async addContact(publicKey, alias = '') {
    const operation = new ContactsOTOperation(publicKey, alias, false);
    this._contactsOTStateManager.add([operation]);

    await this._roomsService.createDialog(publicKey);
    await this._sync();
  }

  async removeContact(publicKey, alias) {
    let operation = new ContactsOTOperation(publicKey, alias, true);
    this._contactsOTStateManager.add([operation]);

    const roomId = createDialogRoomId(this._myPublicKey, publicKey);
    await this._roomsService.quitRoom(roomId);
    await this._sync();
  }

  async _getChatProfileName(publicKey) {
    const profilesService = this._profileServiceCreate(publicKey);
    return profilesService.getProfile();
  }

  _getRooms() {
    return [...this._roomsOTStateManager.getState()]
  }

  _onStateChange = () => {
    this.setState({
      contacts: this._getContactsFromStateManager(),
      contactsReady: true
    });

    let roomParticipants = new Set();
    for (const [, {participants}] of this._getRooms()) {
      participants
        .filter(publicKey => publicKey !== this._myPublicKey)
        .map(publicKey => roomParticipants.add(publicKey));
    }

    this._getNames(new Set([...roomParticipants]))
      .then(names => {
        this.setState({
          names
        });
      })
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

  async _getNames(publicKeys) {
    const names = new Map(this.state.names);

    for (const publicKey of publicKeys) {
      if (this.state.contacts.has(publicKey)) {
        if (this.state.contacts.get(publicKey).name !== '') {
          names.set(publicKey, this.state.contacts.get(publicKey).name);
          continue;
        }
      }

      const userProfile = await this._getChatProfileName(publicKey);
      if (Object.entries(userProfile).length !== 0 && userProfile.name !== '') {
        names.set(publicKey, userProfile.name);
        continue;
      }

      const user = await this._getUserByPublicKey(publicKey);
      if (user !== null) {
        const appStoreName = user.firstName !== '' && user.lastName !== '' ?
          user.firstName + ' ' + user.lastName : user.username;
        names.set(publicKey, appStoreName);
      } else {
        names.set(publicKey, toEmoji(publicKey, 3));
      }

    }

    return names;
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
