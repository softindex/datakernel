import React from "react";
import {withStyles} from '@material-ui/core';
import renameDocumentStyles from "./renameDocumentStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import {connectService} from "global-apps-common";
import DocumentsContext from "../../modules/documents/DocumentsContext";
import {withSnackbar} from "notistack";

class RenameDocumentDialog extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      name: props.documentName,
      loading: false,
    };
  }

  componentDidUpdate(prevProps, prevState, snapshot) {
    if (prevProps.documentName !== this.props.documentName) {
      this.setState({
        ...this.state,
        name: this.props.documentName
      });
    }
  }

  onNameChange = event => {
    this.setState({
      name: event.target.value
    });
  };

  onSubmit = event => {
    event.preventDefault();

    this.setState({
      loading: true
    });

    this.props.renameDocument(this.props.documentId, this.state.name);
    this.props.onClose();
    this.setState({
          loading: false,
          name: ''
        });
  };

  render() {
    return (
      <Dialog
        open={this.props.open}
        onClose={this.props.onClose}
      >
        <form onSubmit={this.onSubmit}>
          <DialogTitle onClose={this.props.onClose}>
            Rename document
          </DialogTitle>
          <DialogContent>
            <TextField
              required={true}
              autoFocus
              value={this.state.name}
              disabled={this.state.loading}
              margin="normal"
              label="New name"
              type="text"
              fullWidth
              variant="outlined"
              onChange={this.onNameChange}
            />
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.props.onClose}
            >
              Close
            </Button>
            <Button
              className={this.props.classes.actionButton}
              type={"submit"}
              color={"primary"}
              variant={"contained"}
            >
              Rename
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

export default connectService(
  DocumentsContext, (state, documentsService) => ({
    renameDocument(documentId, documentName) {
      return documentsService.renameDocument(documentId, documentName);
    }
  })
)(
  withSnackbar(withStyles(renameDocumentStyles)(RenameDocumentDialog))
);
