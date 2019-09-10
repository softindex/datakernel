import React from "react";
import {withStyles} from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import {connectService} from "global-apps-common";
import DocumentsContext from "../../modules/documents/DocumentsContext";
import {withSnackbar} from "notistack";
import DialogContentText from "@material-ui/core/DialogContentText";
import deleteDocumentStyles from "./deleteDocumentStyles";

function DeleteDocumentDialog({documentId, deleteDocument, open, onClose, classes}) {
  function onDelete() {
    deleteDocument(documentId);
    onClose();
  }

  return (
    <Dialog
      open={open}
      onClose={onClose}
    >
      <DialogTitle onClose={onClose}>
        Delete document
      </DialogTitle>
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
          onClick={() => {
            onDelete()
          }}
        >
          Yes
        </Button>
      </DialogActions>
    </Dialog>
  );
}

export default connectService(
  DocumentsContext, (state, documentsService) => ({
    deleteDocument(documentId) {
      return documentsService.deleteDocument(documentId);
    }
  })
)(
  withSnackbar(withStyles(deleteDocumentStyles)(DeleteDocumentDialog))
);
