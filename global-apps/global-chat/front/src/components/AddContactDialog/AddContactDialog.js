import React, {useState} from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import addContactDialogStyles from "./addContactDialogStyles";
import {getInstance, useSnackbar} from "global-apps-common";
import ContactsService from "../../modules/contacts/ContactsService";
import {withRouter} from "react-router-dom";

function AddContactDialogView({classes, onClose, name, loading, onSubmit, onNameChange}) {
  return (
    <Dialog
      onClose={onClose}
      loading={loading}
    >
      <form onSubmit={onSubmit}>
        <DialogTitle>Add Contact</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Enter contact name to start chat
          </DialogContentText>
          <TextField
            required={true}
            className={classes.textField}
            autoFocus
            disabled={loading}
            label="Name"
            margin="normal"
            type="text"
            fullWidth
            variant="outlined"
            onChange={onNameChange}
            value={name}
          />
        </DialogContent>
        <DialogActions>
          <Button
            className={classes.actionButton}
            disabled={loading}
            onClick={onClose}
          >
            Cancel
          </Button>
          <Button
            className={classes.actionButton}
            type="submit"
            disabled={loading}
            color="primary"
            variant="contained"
          >
            Add Contact
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
}

function AddContactDialog({classes, history, contactPublicKey, onClose}) {
  const [name, setName] = useState('');
  const [loading, setLoading] = useState(false);
  const contactsService = getInstance(ContactsService);
  const {showSnackbar} = useSnackbar();

  const props = {
    classes,
    name,
    loading,
    onClose,

    onNameChange(event) {
      setName(event.target.value);
    },

    onSubmit(event) {
      event.preventDefault();
      setLoading(true);
      return contactsService.addContact(contactPublicKey, name)
        .then(({dialogRoomId}) => {
          history.push(path.join('/room', dialogRoomId));
          onClose();
        })
        .catch(err => {
          showSnackbar(err.message, 'error');
        })
        .finally(() => {
          setLoading(false);
        })
    }
  };
  return <AddContactDialogView {...props}/>
}

export default withRouter(
  withStyles(addContactDialogStyles)(AddContactDialog)
);
