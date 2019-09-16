import React, {useState} from "react";
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import addContactDialogStyles from "./addContactDialogStyles";
import {getInstance} from "global-apps-common";
import ContactsService from "../../modules/contacts/ContactsService";
import {withSnackbar} from "notistack";
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
            Enter contact name to start editing documents
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
            loading={loading}
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

function AddContactDialog({classes, enqueueSnackbar, contactPublicKey, onClose}) {
  const [name, setName] = useState('');
  const [loading, setLoading] = useState(false);
  const contactsService = getInstance(ContactsService);

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
        .then(() => {
          onClose();
        })
        .catch(err => {
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        })
        .finally(() => {
          setLoading(false);
        })
    }
  };
  return <AddContactDialogView {...props}/>
}

export default withRouter(
  withSnackbar(
    withStyles(addContactDialogStyles)(AddContactDialog)
  )
);