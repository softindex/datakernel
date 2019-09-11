import React from "react";
import {withStyles} from '@material-ui/core';
import ContactItem from "../ContactItem/ContactItem";
import contactsListStyles from "./contactsListStyles";
import List from "@material-ui/core/List";
import CircularProgress from "@material-ui/core/CircularProgress";
import {withRouter} from "react-router-dom";
import {getInstance} from 'global-apps-common'
import {withSnackbar} from "notistack";
import ContactsService from "../../modules/contacts/ContactsService";

function ContactsListView({
                            classes,
                            searchReady,
                            contacts,
                            sortContacts,
                            onClick,
                            documents,
                            onRemoveContact
                          }) {
  return (
    <>
      {!searchReady && (
        <div className={classes.progressWrapper}>
          <CircularProgress/>
        </div>
      )}
      {searchReady && (
        <List>
          {sortContacts([...contacts]).map(([pubKey, {name}]) => {
              let documentId = '';

              for (const [documentPublicKey, {participants}] of documents) {
                if (participants.includes(pubKey) && participants.length === 2) {
                  documentId = documentPublicKey;
                }
              }

              return (
                <ContactItem
                  name={name}
                  showDeleteButton={true}
                  onRemoveContact={onRemoveContact.bind(this, pubKey)}
                  documentId={documentId}
                  onClick={onClick}
                />
              )
            }
          )}
        </List>
      )}
    </>
  );
}

function ContactsList({classes, enqueueSnackbar, closeSnackbar, searchReady, searchContacts, onClick}) {
  const contactsService = getInstance(ContactsService);

  const props = {
    classes,
    searchContacts,
    searchReady,
    onClick,

    onRemoveContact(pubKey, event) {
      event.preventDefault();
      event.stopPropagation();
      return contactsService.removeContact(pubKey)
        .then(() => {
          setTimeout(() => closeSnackbar(), 1000);
        })
        .catch(error => {
          enqueueSnackbar(error.message, {
            variant: 'error'
          })
        })
    },

    sortContacts(contactsList) {
      return [...contactsList].sort((a, b) => a[1].name.localeCompare(b[1].name));
    }
  };
  return <ContactsListView {...props}/>
}

export default withRouter(
  withSnackbar(
    withStyles(contactsListStyles)(ContactsList)
  )
);
