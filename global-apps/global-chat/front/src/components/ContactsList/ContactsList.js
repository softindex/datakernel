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

function ContactsList({classes, searchReady, searchError, searchContacts, onAddContact}) {
  const [showAddContactDialog, setAddDialog] = useState(false);
  const [contactId, setContactId] = useState('');

  function onContactClick(publicKey) {
    setAddDialog(true);
    setContactId(publicKey);
  }

  const closeAddDialog = () => {
    setAddDialog(false);
  };

  return (
    <>
      {!searchReady && searchError === undefined && (
        <Grow in={!searchReady}>
          <div className={classes.progressWrapper}>
            <CircularProgress/>
          </div>
        </Grow>
      )}
      {!(searchError === '' || searchError === undefined) && (
        <Paper square className={classes.paperError}>
          <Typography className={classes.dividerText}>
            {searchError}
          </Typography>
        </Paper>
      )}
      {searchReady && searchContacts.size !== 0 && (
        <>
          <List>
            {[...searchContacts]
              .map(([publicKey, contact]) => (
                <ContactItem
                  contact={contact}
                  onClick={onContactClick.bind(this, publicKey)}
                />
              ))}
          </List>
          {showAddContactDialog && (
            <ConfirmDialog
              open={showAddContactDialog}
              onClose={closeAddDialog}
              title="Add Contact"
              subtitle="Do you want to add this contact?"
              onConfirm={() => {
                return onAddContact(contactId)
              }}
            />
          )}
        </>
      )}
    </>
  );
}

export default withSnackbar(withStyles(contactsListStyles)(ContactsList));
