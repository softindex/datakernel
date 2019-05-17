import React from "react";
import Button from '@material-ui/core/Button';
import TextField from '@material-ui/core/TextField';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';

class ShareDialog extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      dirName: '',
      other: '' // TODO: change to list of participants
    }
  }

  handleDirNameChange = (event) => {
    this.setState({
      dirName: event.currentTarget.value
    });
  };

  handleOtherChange = (event) => {
    this.setState({
      other: event.currentTarget.value
    });
  };

  submit = () => {
    this.props.onSubmit(this.state.dirName, [this.state.other]);
  };

  render() {
    return (
      <Dialog
        open={this.props.open}
        onClose={this.state.handleClose}
        aria-labelledby="form-dialog-title"
      >
        <DialogTitle id="form-dialog-title">{this.props.title}</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            margin="dense"
            label="Folder name"
            type="text"
            onChange={this.handleDirNameChange}
            fullWidth
          />
          <TextField
            margin="dense"
            label="Other user"
            type="text"
            onChange={this.handleOtherChange}
            fullWidth
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={this.props.onClose} color="primary">
            Cancel
          </Button>
          <Button onClick={this.submit} color="primary" variant="contained">
            Share
          </Button>
        </DialogActions>
      </Dialog>
    )
  }
}

export default ShareDialog;
