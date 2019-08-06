import React from 'react';
import {withSnackbar} from 'notistack';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import DialogContentText from '@material-ui/core/DialogContentText';
import noteDialogsStyles from '../CreateNoteDialog/noteDialogsStyles';
import Dialog from '../Dialog/Dialog'
import connectService from '../../common/connectService';
import NotesContext from '../../modules/notes/NotesContext';

function DeleteNoteDialog(props) {
  const onDelete = () => {
    return props.deleteNote(props.noteId)
      .then(props.onClose)
      .catch(err => {
        props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      })
  };

  return (
    <Dialog
      open={props.open}
      onClose={props.onClose}
    >
      <form>
        <DialogTitle
          onClose={props.onClose}
        >
          Delete note
        </DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to delete note?
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button
            className={props.classes.actionButton}
            onClick={props.onClose}
          >
            No
          </Button>
          <Button
            className={props.classes.actionButton}
            color="primary"
            variant="contained"
            onClick={onDelete}
          >
            Yes
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
}

export default connectService(
  NotesContext,
  (state, notesService) => ({
    deleteNote(noteId) {
      return notesService.deleteNote(noteId);
    }
  })
)(
  withSnackbar(withStyles(noteDialogsStyles)(DeleteNoteDialog))
);
