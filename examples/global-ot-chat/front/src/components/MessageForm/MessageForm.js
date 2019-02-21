import React from 'react';
import connectService from '../../common/connectService';
import ChatContext from '../../modules/chat/ChatContext';
import AccountContext from '../../modules/account/AccountContext';
import Paper from '@material-ui/core/Paper';
import SendIcon from '@material-ui/icons/Send';
import messageFormStyles from './messageFormStyles';
import {withStyles} from '@material-ui/core';
import InputBase from '@material-ui/core/InputBase';
import Divider from '@material-ui/core/Divider';
import IconButton from '@material-ui/core/IconButton';

class MessageForm extends React.Component {
  state = {
    message: ''
  };

  onChangeMessage = (event) => {
    this.setState({
      message: event.target.value
    });
  };

  onSubmit = (event) => {
    event.preventDefault();

    if (!this.state.message) {
      return;
    }

    this.props.sendMessage(this.props.login, this.state.message);
    this.setState({
      message: ''
    });
  };

  render() {
    return (
      <form onSubmit={this.onSubmit}>
        <Paper className={this.props.classes.root} elevation={2}>
          <InputBase
            inputProps={{
              className: this.props.classes.inputText,
              required: true
            }}
            className={this.props.classes.input}
            placeholder="Message"
            onChange={this.onChangeMessage}
            value={this.state.message}
          />
          <Divider className={this.props.classes.divider}/>
          <IconButton
            color="primary"
            aria-label="Send"
            type="submit"
          >
            <SendIcon/>
          </IconButton>
        </Paper>
      </form>
    );
  }
}

export default withStyles(messageFormStyles)(
  connectService(ChatContext, (state, chatService) => ({
    async sendMessage(login, message) {
      await chatService.sendMessage(login, message);
    }
  }))(
    connectService(AccountContext, ({login}) => ({login}))(
      MessageForm
    )
  )
);
