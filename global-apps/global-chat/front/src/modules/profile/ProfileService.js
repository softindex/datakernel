import Service from '../../common/Service';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import profileOTSystem from "./ot/ProfileOTSystem";
import profileSerializer from "./ot/serializer";
import {wait} from '../../common/utils';

const RETRY_TIMEOUT = 1000;

class ProfileService extends Service {
  constructor(profilesOTStateManager) {
    super();
    this._profilesOTStateManager = profilesOTStateManager;
    this._reconnectTimeout = null;
  }

  static create(pubKey) {
    const profileOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/profile/' + pubKey,
      serializer: profileSerializer
    });
    const profilesOTStateManager = new OTStateManager(() => ({}), profileOTNode, profileOTSystem);
    return new ProfileService(profilesOTStateManager);
  }

  async init() {
    try {
      await this._profilesOTStateManager.checkout();
    } catch (err) {
      console.error(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

    this._onStateChange();
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
  }

  async getProfile() {
    await this._profilesOTStateManager.checkout();
    return this._profilesOTStateManager.getState();
  }

  _onStateChange = () => {
    this.setState({
      profile: this._getProfileFields(),
      profilesReady: true
    });
  };

  _getProfileFields() {
    return this._profilesOTStateManager.getState();
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._profilesOTStateManager.sync();
    } catch (err) {
      console.log(err);
      await wait(RETRY_TIMEOUT);
      await this._sync();
    }
  }
}

export default ProfileService;

