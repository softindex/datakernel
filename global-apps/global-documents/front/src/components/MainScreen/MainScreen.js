import React from 'react';
import Header from "../Header/Header"
import SideBar from "../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import checkAuth from '../../common/checkAuth';
import connectService from "../../common/connectService";
import DocumentsContext from "../../modules/documents/DocumentsContext";
import ContactsContext from "../../modules/contacts/ContactsContext";
import {withSnackbar} from "notistack";
import StartDocument from "../EmptyDocument/EmptyDocument";
import ContactsService from "../../modules/contacts/ContactsService";
import DocumentsService from "../../modules/documents/DocumentsService";
import AccountContext from "../../modules/account/AccountContext";
import ProfileService from "../../modules/profile/ProfileService";
import ProfileContext from "../../modules/profile/ProfileContext";
import Document from "../Document/Document";
import SearchContactsService from "../../modules/searchContacts/SearchContactsService";
import SearchContactsContext from "../../modules/searchContacts/SearchContactsContext";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);
    this.contactsService = ContactsService.create();
    this.documentsService = DocumentsService.createForm(this.contactsService, props.publicKey);
    this.profileService = ProfileService.create();
    this.searchContactsService = SearchContactsService.create();
  }

  componentDidMount() {
    Promise.all([
      this.contactsService.init(),
      this.documentsService.init(),
      this.profileService.init(),
    ]).catch((err) => {
      this.props.enqueueSnackbar(err.message, {
        variant: 'error'
      });
    });
  }

  componentWillUnmount() {
    this.documentsService.stop();
    this.contactsService.stop();
    this.profileService.stop()
  }

  render() {
    const {documentId} = this.props.match.params;
    return (
      <SearchContactsContext.Provider value={this.searchContactsService}>
        <ProfileContext.Provider value={this.profileService}>
          <DocumentsContext.Provider value={this.documentsService}>
            <ContactsContext.Provider value={this.contactsService}>
              <Header documentId={documentId}/>
              <div className={this.props.classes.document}>
                <SideBar publicKey={this.props.publicKey}/>
                {!documentId && (
                  <StartDocument/>
                )}
                {documentId && (
                  <Document
                    documentId={documentId}
                    isNew={this.documentsService.state.newDocuments.has(documentId)}
                  />
                )}
              </div>
            </ContactsContext.Provider>
          </DocumentsContext.Provider>
        </ProfileContext.Provider>
      </SearchContactsContext.Provider>
    );
  }
}

export default connectService(
  AccountContext, ({publicKey}, accountService) => ({
    publicKey, accountService
  })
)(checkAuth(
  withSnackbar(withStyles(mainScreenStyles)(MainScreen))
  )
);
