import Service from '../../common/Service';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import profilesOTSystem from "./ot/ProfilesOTSystem";
import profilesSerializer from "./ot/serializer";
import {wait} from '../../common/utils';

const RETRY_TIMEOUT = 1000;

class ProfilesService extends Service {
  constructor(profilesOTStateManager) {
    super({
      profile: {},
      profilesReady: false
    });
    this._profilesOTStateManager = profilesOTStateManager;
    this._reconnectTimeout = null;
  }

  static create(pubKey) {
    const profileOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/profile/' + pubKey,
      serializer: profilesSerializer
    });
    const profilesOTStateManager = new OTStateManager(() => ({}), profileOTNode, profilesOTSystem);
    return new ProfilesService(profilesOTStateManager);
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
    this._profilesOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._profilesOTStateManager.removeChangeListener(this._onStateChange);
  }

  getProfile() {
    return this.state.profile;
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

export default ProfilesService;

