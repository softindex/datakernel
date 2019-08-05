import React from 'react';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Paper from "@material-ui/core/Paper";
import Button from "@material-ui/core/Button";
import AddContactForm from "../AddContactDialog/AddContactDialog";
import DocumentForm from "../CreateDocumentDialog/CreateDocumentDialog"
import DocumentsList from "../DocumentsList/DocumentsList";
import ContactsList from "../ContactsList/ContactsList";
import connectService from "../../common/connectService";
import ContactsContext from "../../modules/contacts/ContactsContext";
import IconButton from "@material-ui/core/IconButton";
import SearchIcon from "@material-ui/icons/Search";
import InputBase from "@material-ui/core/InputBase";
import DocumentsContext from "../../modules/documents/DocumentsContext";

const DOCUMENTS_TAB = 'documents';
const CONTACTS_TAB = 'contacts';

class SideBar extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tabId: DOCUMENTS_TAB,
      showAddDialog: false,
      contactsList: props.contacts,
      documentsList: props.documents,
      beforeInitState: false
    }
  };

  onChangeTab = (event, nextTabId) => {
    this.setState({tabId: nextTabId})
  };

  onSearchChangeContacts = (event) => {
    if (event.target.value === '') {
      this.setState({
        contactsList: this.props.contacts,
        documentsList: this.props.documents
      });
    } else {
      this.setState({
        contactsList: new Map([...this.props.contacts]
          .filter(([pubKey, {name}]) => name
            .toLowerCase()
            .includes(event.target.value.toLowerCase()))),
        beforeInitState: true
      });
    }
  };

  onSearchChangeDocuments = (event) => {
    if (event.target.value === '') {
      this.setState({
        contactsList: this.props.contacts,
        documentsList: this.props.documents
      })
    } else {
      this.setState({
        documentsList: new Map([...this.props.documents]
          .filter(([documentId, {name}]) => name
            .toLowerCase()
            .includes(event.target.value.toLowerCase()))),
        beforeInitState: true
      });
    }
  };

  showAddDialog = () => {
    this.setState({
      showAddDialog: true
    });
  };

  closeAddDialog = () => {
    this.setState({
      showAddDialog: false
    });
  };

  render() {
    const {classes} = this.props;
    return (
      <div className={classes.wrapper}>
        <Paper className={classes.search}>
          <IconButton
            className={classes.iconButton}
            disabled={true}
          >
            <SearchIcon/>
          </IconButton>
          <InputBase
            className={classes.inputDiv}
            placeholder={this.state.tabId === DOCUMENTS_TAB? "Documents..." : "People, public keys..."}
            autoFocus
            value={this.state.search}
            onChange={this.onSearchChangeDocuments}
            classes={{input: classes.input}}
          />
        </Paper>
        <Paper square className={classes.paper}>
          <Tabs
            value={this.state.tabId}
            centered={true}
            indicatorColor="primary"
            textColor="primary"
            onChange={this.onChangeTab}
          >
            <Tab value={DOCUMENTS_TAB} label="Documents"/>
            <Tab value={CONTACTS_TAB} label="Contacts"/>
          </Tabs>
        </Paper>

        {this.state.tabId === DOCUMENTS_TAB && (
          <div className={classes.tabContent}>
            <DocumentForm
              open={this.state.showAddDialog}
              onClose={this.closeAddDialog}
            />
            <div className={classes.documentsList}>
              <DocumentsList
                documents={this.state.documentsList.size === 0 && !this.state.beforeInitState ?
                  this.props.documents : this.state.documentsList}
                contacts={this.props.contacts}
                documentsService={this.props.documentsService}
                ready={this.props.ready}
                addContact={this.props.addContact}
                createDialog={this.props.createDialog}
                quitDocument={this.props.deleteDocument}
                renameDocument={this.props.renameDocument}
                publicKey={this.props.publicKey}
              />
            </div>
            {/*{this.state.documentsList.size === 0 && this.state.search !== '' && (*/}
            {/*  <Typography*/}
            {/*    className={classes.secondaryText}*/}
            {/*    color="textSecondary"*/}
            {/*    variant="body1"*/}
            {/*  >*/}
            {/*    Nothing found*/}
            {/*  </Typography>*/}
            {/*)}*/}
          </div>
        )}
        {this.state.tabId === CONTACTS_TAB && (
          <div className={classes.tabContent}>
            <AddContactForm
              open={this.state.showAddDialog}
              onClose={this.closeAddDialog}
              publicKey={this.props.publicKey}
              addContact={this.props.addContact}
            />
            {/*<Button*/}
            {/*  className={classes.button}*/}
            {/*  fullWidth={true}*/}
            {/*  variant="contained"*/}
            {/*  size="medium"*/}
            {/*  color="primary"*/}
            {/*  onClick={this.showAddDialog}*/}
            {/*>*/}
            {/*  Add Contact*/}
            {/*</Button>*/}
            <div className={classes.documentsList}>
              <ContactsList
                contacts={this.state.contactsList.size === 0 && !this.state.beforeInitState ?
                  this.props.contacts : this.state.contactsList}
                documents={this.props.documents}
                addContact={this.props.addContact}
                removeContact={this.props.removeContact}
                contactsService={this.props.contactsService}
                ready={this.props.ready}
              />
            </div>
          </div>
        )}
      </div>
    );
  }
}

export default connectService(
  ContactsContext, ({ready, contacts}, contactsService) => ({
    ready, contacts, contactsService,
    addContact(pubKey, name) {
      return contactsService.addContact(pubKey, name);
    },
    removeContact(pubKey, name) {
      return contactsService.removeContact(pubKey, name);
    }
  })
)(
  connectService(
    DocumentsContext, ({ready, documents}, documentsService) => ({
      documentsService, ready, documents,
      quitDocument(documentId) {
        return documentsService.deleteDocument(documentId);
      },
      renameDocument(documentId, newName) {
        return documentsService.renameDocument(documentId, newName);
      }

    })
  )(
    withStyles(sideBarStyles)(SideBar)
  )
);
