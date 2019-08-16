import Service from '../../common/Service';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import myProfileOTSystem from "./ot/MyProfileOTSystem";
import myProfileSerializer from "./ot/serializer";
import MyProfileOTOperation from "./ot/MyProfileOTOperation";
import {wait} from '../../common/utils';

const RETRY_TIMEOUT = 1000;

class MyProfileService extends Service {
  constructor(profileOTStateManager) {
    super({
      profile: {},
      profileReady: false
    });
    this._profileOTStateManager = profileOTStateManager;
    this._reconnectTimeout = null;
  }

  static create() {
    const profileOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/myProfile',
      serializer: myProfileSerializer
    });
    const profileOTStateManager = new OTStateManager(() => ({}), profileOTNode, myProfileOTSystem);
    return new MyProfileService(profileOTStateManager);
  }

  async init() {
    try {
      await this._profileOTStateManager.checkout();
    } catch (err) {
      console.error(err);
      await this._reconnectDelay();
      await this.init();
      return;
    }

    this._onStateChange();
    this._profileOTStateManager.addChangeListener(this._onStateChange);
  }

  stop() {
    clearTimeout(this._reconnectTimeout);
    this._profileOTStateManager.removeChangeListener(this._onStateChange);
  }

  async setProfileField(fieldName, value) {
    const profileNameOperation = new MyProfileOTOperation({
      [fieldName]: {
        prev: this.state.profile[fieldName],
        next: value
      }
    });
    this._profileOTStateManager.add([profileNameOperation]);
    await this._sync();
  };

  _onStateChange = () => {
    this.setState({
      profile: this._getProfileFields(),
      profileReady: true
    });
  };

  _getProfileFields() {
    return this._profileOTStateManager.getState();
  }

  _reconnectDelay() {
    return new Promise(resolve => {
      this._reconnectTimeout = setTimeout(resolve, RETRY_TIMEOUT);
    });
  }

  async _sync() {
    try {
      await this._profileOTStateManager.sync();
    } catch (err) {
      console.log(err);
      await wait(RETRY_TIMEOUT);
      await this._sync();
    }
  }
}

export default MyProfileService;

