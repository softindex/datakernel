import {Service} from 'global-apps-common';
import {wait, retry, toEmoji} from 'global-apps-common';
import {GlobalAppStoreAPI} from "global-apps-common";
import ProfileService from "../profile/ProfileService";

const RETRY_TIMEOUT = 1000;

class NamesService extends Service {
  constructor(contactsOTStateManager, roomsOTStateManager, globalAppStoreAPI, publicKey, profileServiceCreate) {
    super({
      names: new Map(),
      namesReady: false
    });
    this._myPublicKey = publicKey;
    this._contactsOTStateManager = contactsOTStateManager;
    this._roomsOTStateManager = roomsOTStateManager;
    this._globalAppStoreAPI = globalAppStoreAPI;
    this._profileServiceCreate = profileServiceCreate;
    this._contactsCheckoutPromise = null;
    this._roomsCheckoutPromise = null;
  }

  static createFrom(contactsOTStateManager, roomsOTStateManager, publicKey) {
    return new NamesService(
      contactsOTStateManager,
      roomsOTStateManager,
      GlobalAppStoreAPI.create(process.env.REACT_APP_GLOBAL_OAUTH_LINK),
      publicKey,
      contactPublicKey => {
        return ProfileService.create(contactPublicKey)
      });
  }

  async init() {
    this._contactsCheckoutPromise = retry(() => this._contactsOTStateManager.checkout(), RETRY_TIMEOUT);
    this._roomsCheckoutPromise = retry(() => this._roomsOTStateManager.checkout(), RETRY_TIMEOUT);

    await Promise.all([
      this._contactsCheckoutPromise,
      this._roomsCheckoutPromise
    ]);

    this._onStateChange();

    this._contactsOTStateManager.addChangeListener(this._onStateChange);
    this._roomsOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    this._contactsCheckoutPromise.stop();
    this._contactsOTStateManager.removeChangeListener(this._onStateChange);
  }

  async _getChatProfileName(publicKey) {
    const profilesService = this._profileServiceCreate(publicKey);
    return profilesService.getProfile();
  }

  _onStateChange = () => {
    let roomParticipants = new Set();
    const contacts = this._getContactsFromStateManager();
    for (const [, {participants}] of this._getRooms()) {
      participants
        .filter(publicKey => publicKey !== this._myPublicKey)
        .map(publicKey => roomParticipants.add(publicKey));
    }

    this._getNames(new Set([...roomParticipants]), contacts)
      .then(names => {
        this.setState({
          names,
          namesReady: true
        });
      })
  };

  _getContactsFromStateManager() {
    return new Map(
      [...this._contactsOTStateManager.getState()]
        .map(([publicKey, name]) => [publicKey, {name}])
    );
  }

  _getRooms() {
    return [...this._roomsOTStateManager.getState()]
  }

  _getUserByPublicKey(publicKey) {
    return this._globalAppStoreAPI.getUserByPublicKey(publicKey);
  }

  async _getNames(publicKeys, contacts) {
    const names = new Map(this.state.names);

    for (const publicKey of publicKeys) {
      if (contacts.has(publicKey)) {
        if (contacts.get(publicKey).name !== '') {
          names.set(publicKey, contacts.get(publicKey).name);
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

export default NamesService;
