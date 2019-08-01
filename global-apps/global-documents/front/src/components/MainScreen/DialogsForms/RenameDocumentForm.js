import React from "react";
import {withStyles} from '@material-ui/core';
import formStyles from "./formStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../../common/Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import connectService from "../../../common/connectService";
import DocumentsContext from "../../../modules/documents/DocumentsContext";
import {withSnackbar} from "notistack";
import * as PropTypes from "prop-types";

class RenameDocumentForm extends React.Component {
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

  handleNameChange = (event) => {
    this.setState({
      name: event.target.value
    });
  };

  handleSubmit = (event) => {
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
        aria-labelledby="form-dialog-title"
      >
        <form onSubmit={this.handleSubmit}>
          <DialogTitle
            id="customized-dialog-title"
            onClose={this.props.onClose}
          >
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
              onChange={this.handleNameChange}
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

RenameDocumentForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default connectService(
  DocumentsContext, (state, documentsService) => ({
    renameDocument(documentId, documentName) {
      return documentsService.renameDocument(documentId, documentName);
    }
  })
)(
  withSnackbar(withStyles(formStyles)(RenameDocumentForm))
);
