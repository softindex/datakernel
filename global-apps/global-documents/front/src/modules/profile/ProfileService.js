import {Service} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import profileOTSystem from "./ot/ProfileOTSystem";
import profileSerializer from "./ot/serializer";

class ProfileService extends Service {
  constructor(profilesOTStateManager) {
    super();
    this._profilesOTStateManager = profilesOTStateManager;
  }

  static createFrom(pubKey) {
    const profileOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/profile/' + pubKey,
      serializer: profileSerializer
    });
    const profilesOTStateManager = new OTStateManager(() => ({}), profileOTNode, profileOTSystem);
    return new ProfileService(profilesOTStateManager);
  }

  async getProfile() {
    await this._profilesOTStateManager.checkout();
    return this._profilesOTStateManager.getState();
  }
}

export default ProfileService;