import {Service, mapOperationSerializer} from 'global-apps-common';
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import {profileOTSystem} from "../../common/utils";

class ProfileService extends Service {
  constructor(profilesOTStateManager) {
    super();
    this._profilesOTStateManager = profilesOTStateManager;
  }

  static createFrom(pubKey) {
    const profileOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/profile/' + pubKey,
      serializer: mapOperationSerializer
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

