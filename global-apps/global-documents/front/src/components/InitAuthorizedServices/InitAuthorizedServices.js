import React, {useMemo} from 'react';
import {checkAuth, AuthContext, connectService, RegisterDependency, initService, useSnackbar} from 'global-apps-common';
import ContactsService from "../../modules/contacts/ContactsService";
import MyProfileService from "../../modules/profile/MyProfileService";
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import NamesService from "../../modules/names/NamesService";
import contactsSerializer from "../../modules/contacts/ot/serializer";
import contactsOTSystem from "../../modules/contacts/ot/ContactsOTSystem";
import {withRouter} from "react-router-dom";
import DocumentsService from "../../modules/documents/DocumentsService";

function InitAuthorizedServices({publicKey, children}) {
  const {showSnackbar} = useSnackbar();
  const {
    contactsOTStateManager,
    profileService,
    contactsService,
    namesService,
    documentsService
  } = useMemo(() => {
    const contactsOTNode = ClientOTNode.createWithJsonKey({
      url: '/ot/contacts',
      serializer: contactsSerializer
    });

    const contactsOTStateManager = new OTStateManager(() => new Map(), contactsOTNode, contactsOTSystem);
    const profileService = MyProfileService.create();
    const contactsService = ContactsService.createFrom(contactsOTStateManager, publicKey);
    const namesService = NamesService.createFrom(contactsOTStateManager, publicKey);
    const documentsService = DocumentsService.createFrom(contactsService, publicKey);

    return {
      contactsOTStateManager,
      profileService,
      contactsService,
      namesService,
      documentsService
    };
  }, [publicKey]);

  function errorHandler(err) {
    showSnackbar(err.message, 'error');
  }

  initService(contactsService, errorHandler);
  initService(documentsService, errorHandler);
  initService(profileService, errorHandler);
  initService(namesService, errorHandler);

  return (
    <RegisterDependency name="contactsOTStateManager" value={contactsOTStateManager}>
      <RegisterDependency name={MyProfileService} value={profileService}>
        <RegisterDependency name={NamesService} value={namesService}>
          <RegisterDependency name={ContactsService} value={contactsService}>
            <RegisterDependency name={DocumentsService} value={documentsService}>
              {children}
            </RegisterDependency>
          </RegisterDependency>
        </RegisterDependency>
      </RegisterDependency>
    </RegisterDependency>
  );
}

export default connectService(
  AuthContext, ({publicKey}) => ({publicKey})
)(
  checkAuth(
    withRouter(InitAuthorizedServices))
);
