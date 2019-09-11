import React, {useEffect, useMemo, useState} from 'react';
import Header from "../Header/Header"
import Document from "../Document/Document"
import SideBar from "../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import {checkAuth, AuthContext, connectService, RegisterDependency, useService, initService} from 'global-apps-common';
import {withSnackbar} from "notistack";
import EmptyDocument from "../EmptyDocument/EmptyDocument";
import ContactsService from "../../modules/contacts/ContactsService";
import DocumentsService from "../../modules/documents/DocumentsService";
import MyProfileService from "../../modules/profile/MyProfileService";
import {ClientOTNode, OTStateManager} from "ot-core/lib";
import NamesService from "../../modules/names/NamesService";
import contactsSerializer from "../../modules/contacts/ot/serializer";
import contactsOTSystem from "../../modules/contacts/ot/ContactsOTSystem";
import AddContactDialog from "../AddContactDialog/AddContactDialog";

function MainScreen({publicKey, enqueueSnackbar, match, classes, history}) {
  const [redirectPublicKey, setRedirectPublicKey] = useState(null);
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


  const {documents} = useService(documentsService);
  const {contacts} = useService(contactsService);

  function errorHandler(err) {
    enqueueSnackbar(err.message, {
      variant: 'error'
    });
  }

  initService(contactsService, errorHandler);
  initService(documentsService, errorHandler);
  initService(profileService, errorHandler);
  initService(namesService, errorHandler);

  const {documentId} = match.params;
  const redirectURI = history.location.pathname;

  useEffect(() => {
    if (redirectURI !== '/' && redirectURI.substr(1, 6) === 'invite') {
      const redirectPublicKey = redirectURI.slice(8);
      if (/^[0-9a-z:]{5,}:[0-9a-z:]{5,}$/i.test(redirectPublicKey)) {
        if (!contacts.has(redirectPublicKey)) {
          setRedirectPublicKey(redirectPublicKey);
        } else {
          setRedirectPublicKey(null);
        }
      }
    }
  }, [redirectURI, contacts]);

  return (
    <RegisterDependency name="contactsOTStateManager" value={contactsOTStateManager}>
      <RegisterDependency name={MyProfileService} value={profileService}>
        <RegisterDependency name={NamesService} value={namesService}>
          <RegisterDependency name={ContactsService} value={contactsService}>
            <RegisterDependency name={DocumentsService} value={documentsService}>
              <Header title={documents.has(documentId) ? documents.get(documentId).name : ''}/>
              <div className={classes.document}>
                <SideBar/>
                {!documentId && (
                  <EmptyDocument/>
                )}
                {documentId && (
                  <Document
                    documentId={documentId}
                    isNew={documentsService.state.newDocuments.has(documentId)}
                  />
                )}
              </div>
              {redirectPublicKey !== null && (
                <AddContactDialog
                  onClose={() => {
                    setRedirectPublicKey(null)
                  }}
                  contactPublicKey={redirectPublicKey}/>
              )}
            </RegisterDependency>
          </RegisterDependency>
        </RegisterDependency>
      </RegisterDependency>
    </RegisterDependency>
  );
}

export default connectService(
  AuthContext, ({publicKey}, accountService) => ({
    publicKey, accountService
  })
)(checkAuth(
  withSnackbar(withStyles(mainScreenStyles)(MainScreen))
  )
);
