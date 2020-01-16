import React, {useState} from "react";
import path from 'path';
import {List, withStyles} from '@material-ui/core';
import contactsListStyles from "./contactsListStyles";
import CircularProgress from "@material-ui/core/CircularProgress";
import ContactItem from "../ContactItem/ContactItem";
import ConfirmDialog from "../ConfirmDialog/ConfirmDialog";
import {getInstance, useSnackbar} from "global-apps-common";
import ContactsService from "../../modules/contacts/ContactsService";
import {withRouter} from "react-router-dom";

function ContactsListView({
                            classes,
                            searchReady,
                            searchContacts,
                            onContactClick,
                            addContactId,
                            onCloseAddDialog,
                            onConfirmAddContact
                          }) {
  if (!searchReady) {
    return (
      <div className={classes.progressWrapper}>
        <CircularProgress/>
      </div>
    )
  }

  return (
    <>
      {searchContacts.size !== 0 && (
        <List>
          {[...searchContacts]
            .map(([publicKey, contact]) => (
              <ContactItem
                username={contact.username}
                firstName={contact.firstName}
                lastName={contact.lastName}
                onClick={onContactClick.bind(this, publicKey)}
              />
            ))}
        </List>
      )}
      {addContactId && (
        <ConfirmDialog
          onClose={onCloseAddDialog.bind(this)}
          title="Add Contact"
          subtitle="Do you want to add this contact?"
          onConfirm={onConfirmAddContact}
        />
      )}
    </>
  );
}

function ContactsList({classes, history, onSearchClear, searchReady, searchContacts}) {
  const contactsService = getInstance(ContactsService);
  const [addContactId, setContactId] = useState(null);
  const {showSnackbar} = useSnackbar();

  const props = {
    classes,
    addContactId,
    searchContacts,
    searchReady,

    onContactClick(publicKey) {
      setContactId(publicKey);
    },

    onCloseAddDialog() {
      setContactId(null);
    },

    onConfirmAddContact() {
      return contactsService.addContact(addContactId)
        .then(({dialogRoomId}) => {
          history.push(path.join('/room', dialogRoomId));
          onSearchClear();
          props.onCloseAddDialog();
        })
        .catch(err => {
          showSnackbar(err.message, 'error');
        });
    }
  };
  return <ContactsListView {...props}/>
}

export default withRouter(
  withStyles(contactsListStyles)(ContactsList)
);
