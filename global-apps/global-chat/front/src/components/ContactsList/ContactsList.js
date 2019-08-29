import React, {useState} from "react";
import {List, withStyles} from '@material-ui/core';
import contactsListStyles from "./contactsListStyles";
import Grow from "@material-ui/core/Grow";
import CircularProgress from "@material-ui/core/CircularProgress";
import Paper from "@material-ui/core/Paper";
import Typography from "@material-ui/core/Typography";
import ContactItem from "../ContactItem/ContactItem";
import {withSnackbar} from "notistack";
import ConfirmDialog from "../ConfirmDialog/ConfirmDialog";

function ContactsList(props) {
  const [showAddContactDialog, setAddDialog] = useState(false);
  const [contactId, setContactId] = useState('');

  function onContactClick(publicKey) {
    setAddDialog(true);
    setContactId(publicKey);
  }

  const closeAddDialog = () => {
    setAddDialog(false);
  };

  function onConfirmAddContact(contactId) {
    return props.onAddContact(contactId)
      .catch((err) => {
        props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      });
  }

  return (
    <>
      {!props.searchReady && props.error === undefined && (
        <Grow in={!props.searchReady}>
          <div className={props.classes.progressWrapper}>
            <CircularProgress/>
          </div>
        </Grow>
      )}
      {!(props.error === '' || props.error === undefined) && (
        <Paper square className={props.classes.paperError}>
          <Typography className={props.classes.dividerText}>
            {props.error}
          </Typography>
        </Paper>
      )}
      {props.searchReady && props.searchContacts.size !== 0 && (
        <>
          <List>
            {[...props.searchContacts]
              .filter(([publicKey]) => publicKey !== props.publicKey)
              .map(([publicKey, contact]) => (
                <ContactItem
                  contact={contact}
                  onClick={onContactClick.bind(this, publicKey)}
                  publicKey={props.publicKey}
                  onAddContact={props.onAddContact}
                />
              ))}
          </List>
          <ConfirmDialog
            open={showAddContactDialog}
            onClose={closeAddDialog}
            title="Add Contact"
            subtitle="Do you want to add this contact?"
            onConfirm={() => {
              return onConfirmAddContact(contactId)
            }}
          />
        </>
      )}
    </>
  );
}

export default withSnackbar(withStyles(contactsListStyles)(ContactsList));
