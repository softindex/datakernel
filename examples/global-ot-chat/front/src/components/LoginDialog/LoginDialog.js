import React from 'react';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import Button from '@material-ui/core/Button';
import connectService from '../../common/connectService';
import AccountContext from "../../modules/account/AccountContext";

class LoginDialog extends React.Component {
  state = {
    value: null
  };

  handleChange = (e) => {
    this.setState({
      value: e.currentTarget.value
    });
  };

  onSubmit = (event) => {
    event.preventDefault();
    this.props.onSubmit(this.state.value)
  };

  render() {
    return (
      <Dialog
        open={!this.props.authorized}
        onClose={this.handleClose}
        aria-labelledby="form-dialog-title"
      >
        <form onSubmit={this.onSubmit}>
          <DialogTitle id="form-dialog-title">Login</DialogTitle>
          <DialogContent>
            <DialogContentText>
              Enter your username to start chat
            </DialogContentText>
            <TextField
              required={true}
              autoFocus
              margin="normal"
              label="Login"
              type="text"
              fullWidth
              variant="outlined"
              onChange={this.handleChange}
            />
          </DialogContent>
          <DialogActions>
            <Button type="submit" color="primary">
              Enter
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    )
  }
}

export default connectService(AccountContext, ({authorized}, accountService) => ({
  authorized,
  onSubmit: login => accountService.auth(login)
}))(LoginDialog);
