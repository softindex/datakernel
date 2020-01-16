import React, {useEffect, useState} from "react";
import {withStyles} from '@material-ui/core';
import renameDocumentStyles from "./renameDocumentStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import {getInstance, useSnackbar} from "global-apps-common";
import DocumentsService from "../../modules/documents/DocumentsService";

function RenameDocumentDialogView({classes, name, loading, onNameChange, onClose, onSubmit}) {
  return (
    <Dialog onClose={onClose} loading={loading}>
      <form onSubmit={onSubmit}>
        <DialogTitle>Rename document</DialogTitle>
        <DialogContent>
          <TextField
            required={true}
            autoFocus
            value={name}
            disabled={loading}
            margin="normal"
            label="New name"
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
            Rename
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
}

function RenameDocumentDialog({classes, documentName, documentId, onClose}) {
  const [name, setName] = useState('');
  const [loading, setLoading] = useState(false);
  const documentsService = getInstance(DocumentsService);
  const {showSnackbar} = useSnackbar();

  useEffect(() => {
    setName(documentName);
  }, [documentName]);

  const props = {
    classes,
    documentName,
    onClose,
    name,
    loading,

    onNameChange(event) {
      setName(event.target.value);
    },

    onSubmit(event) {
      event.preventDefault();
      setLoading(true);
      documentsService.renameDocument(documentId, name)
        .then(() => {
          onClose();
        })
        .catch(error => {
          showSnackbar(error.message,  'error');
        })
        .finally(() => {
          setLoading(false);
        })
    }
  };

  return <RenameDocumentDialogView {...props}/>
}

export default withStyles(renameDocumentStyles)(RenameDocumentDialog);
