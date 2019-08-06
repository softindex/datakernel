import React from "react";
import {withStyles} from '@material-ui/core';
import createDocumentStyles from "../CreateDocumentDialog/createDocumentStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../Dialog/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import {withSnackbar} from 'notistack';

class AddContactDialog extends React.Component {
  state = {
    name: '',
    loading: false,
  };

  onNameChange = event => {
    this.setState({name: event.target.value});
  };

  onSubmit = event => {
    event.preventDefault();
    this.setState({
      loading: true
    });
    this.props.addContact(this.props.contactPublicKey, this.state.name);
    this.props.onClose();
    this.setState({
      loading: false
    })
  };

  render() {
    return (
      <Dialog
        open={this.props.open}
        onClose={this.props.onClose}
        loading={this.state.loading}
      >
        <form onSubmit={this.onSubmit}>
          <DialogTitle>Add Contact</DialogTitle>
          <DialogContent>
            <DialogContentText>
              Enter new contact
            </DialogContentText>
            <TextField
              required={true}
              autoFocus
              disabled={this.state.loading}
              margin="normal"
              label="Name"
              type="text"
              fullWidth
              variant="outlined"
              onChange={this.onNameChange}
            />
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              disabled={this.state.loading}
              onClick={this.props.onClose}
            >
              Cancel
            </Button>
            <Button
              className={this.props.classes.actionButton}
              loading={this.state.loading}
              type="submit"
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
}

export default withSnackbar(withStyles(createDocumentStyles)(AddContactDialog));
