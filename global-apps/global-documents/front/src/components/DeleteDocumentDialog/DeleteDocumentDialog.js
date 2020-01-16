import React from "react";
import path from 'path';
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import DialogContentText from "@material-ui/core/DialogContentText";
import deleteDocumentStyles from "./deleteDocumentStyles";
import {getInstance, useSnackbar} from "global-apps-common";
import DocumentsService from "../../modules/documents/DocumentsService";
import {withRouter} from "react-router-dom";

function DeleteDocumentDialogView({classes, onClose, onDelete}) {
  return (
    <Dialog onClose={onClose}>
      <DialogTitle>Delete document</DialogTitle>
      <DialogContent>
        <DialogContentText>
          Are you sure you want to delete document?
        </DialogContentText>
      </DialogContent>
      <DialogActions>
        <Button
          className={classes.actionButton}
          onClick={onClose}
        >
          No
        </Button>
        <Button
          className={classes.actionButton}
          color={"primary"}
          variant={"contained"}
          onClick={() => onDelete()}
        >
          Yes
        </Button>
      </DialogActions>
    </Dialog>
  );
}

function DeleteDocumentDialog({classes, documentId, onClose, history, match}) {
  const documentsService = getInstance(DocumentsService);
  const {showSnackbar, hideSnackbar} = useSnackbar();

  const props = {
    classes,
    onClose,

    onDelete() {
      showSnackbar('Deleting...', 'loading');
      onClose();
      return documentsService.deleteDocument(documentId)
        .then((documentKey) => {
          const {documentId} = match.params;
          if (documentKey === documentId) {
            history.push(path.join('/document', ''));
          }
          hideSnackbar();
        })
        .catch(error => {
          showSnackbar(error.message, 'error');
        });
    }
  };

  return <DeleteDocumentDialogView {...props}/>
}

export default withRouter(
  withStyles(deleteDocumentStyles)(DeleteDocumentDialog)
);
