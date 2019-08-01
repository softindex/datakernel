import React from "react";
import {withStyles} from '@material-ui/core';
import formStyles from "./formStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../../common/Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import connectService from "../../../common/connectService";
import DocumentsContext from "../../../modules/documents/DocumentsContext";
import {withSnackbar} from "notistack";
import * as PropTypes from "prop-types";
import DialogContentText from "@material-ui/core/DialogContentText";

class DeleteDocumentForm extends React.Component {

  handleDelete = () => {
    this.props.deleteDocument(this.props.documentId);
    this.props.onClose();
  };

  render() {
    return (
      <Dialog
        open={this.props.open}
        onClose={this.props.onClose}
        aria-labelledby="form-dialog-title"
      >
        <form onSubmit={this.handleSubmit}>
          <DialogTitle
            id="customized-dialog-title"
            onClose={this.props.onClose}
          >
            Delete document
          </DialogTitle>
          <DialogContent>
            <DialogContentText>
              Are you sure you want to delete document?
            </DialogContentText>
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.props.onClose}
            >
              No
            </Button>
            <Button
              className={this.props.classes.actionButton}
              color={"primary"}
              variant={"contained"}
              onClick={this.handleDelete}
            >
              Yes
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

DeleteDocumentForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default connectService(
  DocumentsContext, (state, documentsService) => ({
    deleteDocument(documentId) {
      return documentsService.deleteDocument(documentId);
    }
  })
)(
  withSnackbar(withStyles(formStyles)(DeleteDocumentForm))
);
