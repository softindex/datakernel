import {Service, delay} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import profileOTSystem from "./ot/ProfileOTSystem";
import profileSerializer from "./ot/serializer";
import ProfileOTOperation from "./ot/ProfileOTOperation";
import {RETRY_TIMEOUT} from '../../common/utils';
import {GlobalAppStoreAPI} from "global-apps-common";

class MyProfileService extends Service {
  constructor(profileOTStateManager, globalAppStoreAPI, myPublicKey) {
    super({
      profile: {},
      profileReady: false
    });
    this._myPublicKey = myPublicKey;
    this._profileOTStateManager = profileOTStateManager;
    this._globalAppStoreAPI = globalAppStoreAPI;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static createFrom(publicKey) {
    const profileOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/myProfile',
      serializer: profileSerializer
    });
    const profileOTStateManager = new OTStateManager(() => ({}), profileOTNode, profileOTSystem);
    return new MyProfileService(
      profileOTStateManager,
      GlobalAppStoreAPI.create(process.env.REACT_APP_GLOBAL_OAUTH_LINK),
      publicKey
    );
  }

  async init() {
    try {
      await this._profileOTStateManager.checkout();
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
    this._profileOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    if (this._reconnectDelay) {
      this._reconnectDelay.cancel();
    }
    if (this._resyncDelay) {
      this._resyncDelay.cancel();
    }
    this._profileOTStateManager.removeChangeListener(this._onStateChange);
  }

  async setProfileField(fieldName, value) {
    const profileNameOperation = new ProfileOTOperation({
      [fieldName]: {
        prev: this.state.profile[fieldName],
        next: value
      }
    });
    this._profileOTStateManager.add([profileNameOperation]);
    await this._sync();
  };

  async _getAppStoreName() {
   const user = await this._globalAppStoreAPI.getUserByPublicKey(this._myPublicKey);
   return user !== null ? user.firstName + ' ' + user.lastName : '';
  }

  _onStateChange = () => {
    const profile = this._getProfileFields();
    (async () => {
      if (Object.keys(profile).length === 0 || profile.name === '') {
        profile.name = await this._getAppStoreName();
      }
    })()
      .catch(err => console.error(err))
      .finally(() => this.setState({profile, profileReady: true}));
  };

  _getProfileFields() {
    return this._profileOTStateManager.getState();
  }

  async _sync() {
    try {
      await this._profileOTStateManager.sync();
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

export default MyProfileService;

