import {Service, delay} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import profileOTSystem from "./ot/ProfileOTSystem";
import profileSerializer from "./ot/serializer";
import ProfileOTOperation from "./ot/ProfileOTOperation";
import {RETRY_TIMEOUT} from '../../common/utils';

class MyProfileService extends Service {
  constructor(profileOTStateManager) {
    super({
      profile: {},
      profileReady: false
    });
    this._profileOTStateManager = profileOTStateManager;
    this._reconnectDelay = null;
    this._resyncDelay = null;
  }

  static create() {
    const profileOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/myProfile',
      serializer: profileSerializer
    });
    const profileOTStateManager = new OTStateManager(() => ({}), profileOTNode, profileOTSystem);
    return new MyProfileService(profileOTStateManager);
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

  _onStateChange = () => {
    this.setState({
      profile: this._getProfileFields(),
      profileReady: true
    });
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

