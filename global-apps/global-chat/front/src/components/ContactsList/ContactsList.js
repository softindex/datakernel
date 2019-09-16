import React, {useState} from "react";
import path from 'path';
import {List, withStyles} from '@material-ui/core';
import contactsListStyles from "./contactsListStyles";
import CircularProgress from "@material-ui/core/CircularProgress";
import ContactItem from "../ContactItem/ContactItem";
import {withSnackbar} from "notistack";
import ConfirmDialog from "../ConfirmDialog/ConfirmDialog";
import {getInstance} from "global-apps-common";
import ContactsService from "../../modules/contacts/ContactsService";
import {withRouter} from "react-router-dom";

function ContactsListView({
                            classes,
                            searchReady,
                            searchContacts,
                            onContactClick,
                            addContactId,
                            closeAddDialog,
                            onConfirmAddContact
                          }) {
  return (
    <>
      {!searchReady && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
      )}
      {searchReady && searchContacts.size !== 0 && (
        <>
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
        </>
      )}
      {addContactId && (
        <ConfirmDialog
          onClose={closeAddDialog.bind(this)}
          title="Add Contact"
          subtitle="Do you want to add this contact?"
          onConfirm={onConfirmAddContact}
        />
      )}
    </>
  );
}

function ContactsList({classes, history, enqueueSnackbar, searchReady, searchContacts}) {
  const contactsService = getInstance(ContactsService);
  const [addContactId, setContactId] = useState(null);

  const props = {
    classes,
    addContactId,
    searchContacts,
    searchReady,

    onContactClick(publicKey) {
      setContactId(publicKey);
    },

    closeAddDialog() {
      setContactId(null);
    },

    onConfirmAddContact() {
      return contactsService.addContact(addContactId)
        .then(({dialogRoomId}) => {
          history.push(path.join('/room', dialogRoomId));
          props.closeAddDialog();
        })
        .catch(err => {
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });
    }
  };
  return <ContactsListView {...props}/>
}

export default withRouter(
  withSnackbar(
    withStyles(contactsListStyles)(ContactsList)
  )
);
