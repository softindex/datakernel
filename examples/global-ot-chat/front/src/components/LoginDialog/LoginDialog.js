import React from 'react';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import TextField from '@material-ui/core/TextField';
import Button from '@material-ui/core/Button';
import connectService from '../../common/connectService';
import ChatContext from "../../modules/chat/ChatContext";
import darkTheme from '../theme/darkTheme';
import {MuiThemeProvider} from "@material-ui/core";

class LoginDialog extends React.Component {

  state = {
    value: null
  };

  handleChange = (e) => {
    this.setState({
      value: e.currentTarget.value
    })
  };

  onSubmit = () => {
    this.props.onSubmit(this.state.value)
  };

  render() {
    return (
      <MuiThemeProvider theme={darkTheme}>
        <Dialog
          open={Boolean(this.props.isOpen)}
          onClose={this.handleClose}
          aria-labelledby="form-dialog-title"
        >
          <DialogTitle id="form-dialog-title">Subscribe</DialogTitle>
          <DialogContent>
            <DialogContentText>
              Enter your username to start chat
            </DialogContentText>
            <TextField
              autoFocus
              margin="normal"
              id="name"
              label="Login"
              type="email"
              fullWidth
              variant="outlined"
              onChange={this.handleChange}
            />
          </DialogContent>
          <DialogActions>
            <Button onClick={this.onSubmit} color="primary">
              Enter
            </Button>
          </DialogActions>
        </Dialog>
      </MuiThemeProvider>
    )
  }
}

export default connectService(ChatContext, (state, chatService) => ({
  async sendMessage(login, message) {
    await chatService.sendMessage(login, message);
  }
}))(LoginDialog);


