import {retry, toEmoji, delay, GlobalAppStoreAPI, Service} from 'global-apps-common';
import ProfileService from "../profile/ProfileService";

const RETRY_TIMEOUT = 1000;

class NamesService extends Service {
  constructor(contactsOTStateManager, globalAppStoreAPI, publicKey, createProfileService) {
    super({
      names: new Map(),
      namesReady: false
    });
    this._myPublicKey = publicKey;
    this._contactsOTStateManager = contactsOTStateManager;
    this._globalAppStoreAPI = globalAppStoreAPI;
    this.createProfileService = createProfileService;
    this._contactsCheckoutPromise = null;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static createFrom(contactsOTStateManager, publicKey) {
    return new NamesService(
      contactsOTStateManager,
      GlobalAppStoreAPI.create(process.env.REACT_APP_GLOBAL_OAUTH_LINK),
      publicKey,
      contactPublicKey => {
        return ProfileService.createFrom(contactPublicKey)
      });
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

  async _getChatProfile(publicKey) {
    const profilesService = this.createProfileService(publicKey);
    return profilesService.getProfile();
  }

  _onStateChange = () => {
    const contacts = this._getContactsFromStateManager();
    this._getNames(contacts)
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

  _getUserByPublicKey(publicKey) {
    return this._globalAppStoreAPI.getUserByPublicKey(publicKey);
  }

  async _getNames(contacts) {
    const names = new Map(this.state.names);
    for (const publicKey of contacts.keys()) {
      if (contacts.has(publicKey) && publicKey !== this._myPublicKey) {
        if (contacts.get(publicKey).name !== '') {
          names.set(publicKey, contacts.get(publicKey).name);
          continue;
        }
      }

      const userProfile = await this._getChatProfile(publicKey);
      if (Object.entries(userProfile).length !== 0 && userProfile.name !== '') {
        names.set(publicKey, userProfile.name);
        continue;
      }

      if (publicKey === this._myPublicKey) {
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

export default NamesService;