import React, {useState, useEffect} from 'react';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import noteDialogsStyles from './noteDialogsStyles';
import Dialog from '../Dialog/Dialog'
import {withRouter} from "react-router-dom";
import {getInstance, useSnackbar} from "global-apps-common";
import NotesService from "../../modules/notes/NotesService";

function CreateNoteDialogView({classes, onSubmit, name, rename, loading, onClose, onNameChange}) {
  return (
    <Dialog onClose={onClose} loading={loading}>
      <form onSubmit={onSubmit}>
        <DialogTitle>
          {rename.show ? 'Rename note' : 'Create note'}
        </DialogTitle>
        <DialogContent>
          <TextField
            required={true}
            autoFocus
            value={name}
            disabled={loading}
            margin="normal"
            label="Note name"
            type="text"
            fullWidth
            variant="outlined"
            onChange={onNameChange}
          />
        </DialogContent>
        <DialogActions>
          <Button
            className={classes.actionButton}
            onClick={onClose}
            disabled={loading}
          >
            Close
          </Button>
          <Button
            className={classes.actionButton}
            type="submit"
            color="primary"
            variant="contained"
            disabled={loading}
          >
            {rename.show ? 'Rename' : 'Create'}
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
}

function CreateNoteDialog({classes, onClose, rename, history}) {
  const notesService = getInstance(NotesService);
  const [name, setName] = useState(rename.noteName || '');
  const [loading, setLoading] = useState(false);
  const {showSnackbar, hideSnackbar} = useSnackbar();

  useEffect(
    () => {
      setName(rename.noteName)
    },
    [rename.noteName]
  );

  const props = {
    classes,
    onClose,
    loading,
    name,
    rename,

    onNameChange(event) {
      setName(event.target.value);
    },

    onSubmit(event) {
      event.preventDefault();

      if (!rename.show) {
        setLoading(true);
        notesService.createNote(name)
          .then(newNoteId => {
            onClose();
            history.push('/note/' + newNoteId);
          })
          .catch(err => {
            showSnackbar(err.message, 'error');
          })
          .finally(() => setLoading(false));
      } else {
        showSnackbar('Renaming...', 'loading');
        onClose();
        notesService.renameNote(rename.noteId, name)
          .then(() => hideSnackbar())
          .catch(err => {
            showSnackbar(err.message, 'error');
          });
      }
    }
  };

  return <CreateNoteDialogView {...props}/>
}

export default withRouter(
  withStyles(noteDialogsStyles)(CreateNoteDialog)
);
