import {retry, toEmoji, delay, Service, GlobalAppStoreAPI} from 'global-apps-common';
import ProfileService from "../profile/ProfileService";
import {RETRY_TIMEOUT} from '../../common/utils';

class NamesService extends Service {
  constructor(contactsOTStateManager, roomsOTStateManager, globalAppStoreAPI, publicKey, createProfileService) {
    super({
      names: new Map(),
      namesReady: false
    });
    this._myPublicKey = publicKey;
    this._contactsOTStateManager = contactsOTStateManager;
    this._roomsOTStateManager = roomsOTStateManager;
    this._globalAppStoreAPI = globalAppStoreAPI;
    this.createProfileService = createProfileService;
    this._contactsCheckoutPromise = null;
    this._roomsCheckoutPromise = null;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static createFrom(contactsOTStateManager, roomsOTStateManager, publicKey) {
    return new NamesService(
      contactsOTStateManager,
      roomsOTStateManager,
      GlobalAppStoreAPI.create(process.env.REACT_APP_GLOBAL_OAUTH_LINK),
      publicKey,
      contactPublicKey => {
        return ProfileService.createFrom(contactPublicKey)
      });
  }

  async init() {
    this._contactsCheckoutPromise = retry(() => this._contactsOTStateManager.checkout(), RETRY_TIMEOUT);
    this._roomsCheckoutPromise = retry(() => this._roomsOTStateManager.checkout(), RETRY_TIMEOUT);

    try {
      await Promise.all([
        this._contactsCheckoutPromise,
        this._roomsCheckoutPromise
      ]);
    } catch (err) {
      console.log(err);

      this._reconnectDelay = delay(RETRY_TIMEOUT);
      try {
        await this._reconnectDelay;
      } catch (err) {
        return;
      }

      await this.init();
      return;
    }

    this._onStateChange();

    this._contactsOTStateManager.addChangeListener(this._onStateChange);
    this._roomsOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
    this._contactsCheckoutPromise.cancel();
    this._roomsCheckoutPromise.cancel();
    this._contactsOTStateManager.removeChangeListener(this._onStateChange);
  }

  async _getChatProfile(publicKey) {
    const profilesService = this.createProfileService(publicKey);
    return profilesService.getProfile();
  }

  _onStateChange = () => {
    let roomParticipants = new Set();
    const contacts = this._getContactsFromStateManager();
    for (const [, {participants}] of [...this._roomsOTStateManager.getState()]) {
      for (const publicKey of participants) {
        roomParticipants.add(publicKey);
      }
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

  _getUserByPublicKey(publicKey) {
    return this._globalAppStoreAPI.getUserByPublicKey(publicKey);
  }

  async _getNames(publicKeys, contacts) {
    const names = new Map(this.state.names);

    for (const publicKey of publicKeys) {
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

      try {
        const user = await this._getUserByPublicKey(publicKey);
        if (user !== null) {
          const appStoreName = user.firstName !== '' && user.lastName !== '' ?
            user.firstName + ' ' + user.lastName : user.username;
          names.set(publicKey, appStoreName);
        } else {
          names.set(publicKey, toEmoji(publicKey, 3));
        }
      } catch (err) {
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
      console.log(err);
      this._resyncDelay = delay(RETRY_TIMEOUT);
      try {
        await this._resyncDelay;
      } catch (err) {
        return;
      }
      await this._sync();
    }
  }
}

export default NamesService;
