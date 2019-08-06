import React from 'react';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import Tabs from '@material-ui/core/Tabs';
import Tab from '@material-ui/core/Tab';
import Typography from '@material-ui/core/Typography';
import Paper from "@material-ui/core/Paper";
import AddContactDialog from "../AddContactDialog/AddContactDialog";
import DocumentsList from "../DocumentsList/DocumentsList";
import ContactsList from "../ContactsList/ContactsList";
import connectService from "../../common/connectService";
import ContactsContext from "../../modules/contacts/ContactsContext";
import IconButton from "@material-ui/core/IconButton";
import SearchIcon from "@material-ui/icons/Search";
import InputBase from "@material-ui/core/InputBase";
import DocumentsContext from "../../modules/documents/DocumentsContext";
import SearchContactsContext from "../../modules/searchContacts/SearchContactsContext";
import ContactItem from "../ContactItem/ContactItem";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import Grow from "@material-ui/core/Grow";

const DOCUMENTS_TAB = 'documents';
const CONTACTS_TAB = 'contacts';

class SideBar extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tabId: DOCUMENTS_TAB,
      showAddDialog: false,
      search: ''
    }
  };

  onChangeTab = (event, nextTabId) => {
    this.setState({
      tabId: nextTabId,
      search: ''
    })
  };

  onSearchChange = event => {
    this.setState({
      search: event.target.value
    }, () => {
      if (this.state.search !== '' && this.state.tabId === CONTACTS_TAB) {
        this.props.search(this.state.search)
      }
    });
  };

  checkSearch() {
    if (/^[0-9a-z:]{5,}:[0-9a-z:]{5,}$/i.test(this.state.search)) {
      if (this.state.tabId !== DOCUMENTS_TAB) {
        this.setState({
          showAddDialog: true
        });
      }
    }
  }

  getFilteredContacts() {
    if (this.state.search === '') {
      return this.props.contacts;
    } else {
      return new Map([...this.props.contacts]
        .filter(([, {name}]) => name
          .toLowerCase()
          .includes(this.state.search.toLowerCase())))
    }
  }

  getFilteredDocuments() {
    if (this.state.search === '') {
      return this.props.documents
    } else {
      return new Map([...this.props.documents]
        .filter(([, {name}]) => name
          .toLowerCase()
          .includes(this.state.search.toLowerCase())))
    }
  }

  closeAddDialog = () => {
    this.setState({
      search: '',
      showAddDialog: false
    });
  };

  render() {
    const {classes} = this.props;

    if (!this.state.showAddDialog) {
      this.checkSearch();
    }

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
            placeholder={this.state.tabId === DOCUMENTS_TAB ? "Documents..." : "People, public keys..."}
            autoFocus
            value={this.state.search}
            onChange={this.onSearchChange}
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
          <div className={classes.documentsList}>
            <DocumentsList
              documents={this.getFilteredDocuments()}
              ready={this.props.documentsReady}
              quitDocument={this.props.deleteDocument}
              renameDocument={this.props.renameDocument}
            />
            {this.getFilteredDocuments().size === 0 && this.state.search !== '' && (
              <Typography
                className={classes.secondaryText}
                color="textSecondary"
                variant="body1"
              >
                Nothing found
              </Typography>
            )}
          </div>
        )}
        {this.state.tabId === CONTACTS_TAB && (
          <>
            <AddContactDialog
              open={this.state.showAddDialog}
              onClose={this.closeAddDialog}
              publicKey={this.props.publicKey}
              contactPublicKey={this.state.search}
              addContact={this.props.addContact}
            />
            <div className={classes.documentsList}>
              <ContactsList
                contacts={this.getFilteredContacts()}
                documents={this.props.documents}
                addContact={this.props.addContact}
                removeContact={this.props.removeContact}
                contactsService={this.props.contactsService}
                ready={this.props.contactsReady}
              />
              {this.state.search !== '' && (
                <>
                  <Paper square className={classes.paperDivider}>
                    <Typography className={classes.dividerText}>
                      People
                    </Typography>
                  </Paper>
                  {!this.props.searchReady && this.props.error === '' && (
                    <Grow in={!this.props.searchReady}>
                      <div className={this.props.classes.progressWrapper}>
                        <CircularProgress/>
                      </div>
                    </Grow>
                  )}
                  {this.props.error !== '' && (
                    <Paper square className={classes.paperError}>
                      <Typography className={classes.dividerText}>
                        {this.props.error}
                      </Typography>
                    </Paper>
                  )}
                  {this.props.searchReady && (
                    <>
                      {this.props.searchContacts.size !== 0 && (
                        <List>
                          {[...this.props.searchContacts].map(([publicKey, contact]) => (
                            <>
                              {!this.props.contacts.has(publicKey) && (
                                <ContactItem
                                  contactId={publicKey}
                                  contact={contact}
                                  name={contact.firstName !== '' && contact.lastName !== '' ?
                                    contact.firstName + ' ' + contact.lastName : contact.username
                                  }
                                  publicKey={this.props.publicKey}
                                  onAddContact={this.props.addContact}
                                />
                              )
                              }
                            </>
                          ))}
                        </List>
                      )}
                      {this.props.searchContacts.size === 0 && (
                        <Typography
                          className={classes.secondaryDividerText}
                          color="textSecondary"
                          variant="body1"
                        >
                          Nothing found
                        </Typography>
                      )}
                    </>
                  )}
                </>
              )}
            </div>
          </>
        )}
      </div>
    );
  }
}

export default withStyles(sideBarStyles)(
  connectService(
    ContactsContext, ({contactsReady, contacts}, contactsService) => ({
      contactsReady, contacts, contactsService,
      addContact(pubKey, name) {
        return contactsService.addContact(pubKey, name);
      },
      removeContact(pubKey, name) {
        return contactsService.removeContact(pubKey, name);
      }
    })
  )(
    connectService(
      DocumentsContext, ({documentsReady, documents}, documentsService) => ({
        documentsService, documentsReady, documents,
        quitDocument(documentId) {
          return documentsService.deleteDocument(documentId);
        },
        renameDocument(documentId, newName) {
          return documentsService.renameDocument(documentId, newName);
        }
      })
    )(
      connectService(
        SearchContactsContext, ({searchContacts, searchReady, error}, searchContactsService) => ({
          searchContacts, searchReady, error, searchContactsService,
          search(searchField) {
            return searchContactsService.search(searchField);
          }
        })
      )(SideBar)
    )
  )
);
