import React, {useState} from 'react';
import Paper from '@material-ui/core/Paper';
import SendIcon from '@material-ui/icons/Send';
import messageFormStyles from './messageFormStyles';
import {withStyles} from '@material-ui/core';
import InputBase from '@material-ui/core/InputBase';
import Divider from '@material-ui/core/Divider';
import IconButton from '@material-ui/core/IconButton';
import {getInstance, useService} from "global-apps-common/lib";
import ChatRoomService from "../../modules/chatroom/ChatRoomService";

function MessageFormView(props) {
  return (
    <form className={props.classes.form} onSubmit={props.onSubmit}>
      <Paper className={props.classes.root} elevation={2}>
        <InputBase
          inputProps={{
            className: props.classes.inputText,
            required: true
          }}
          className={props.classes.input}
          placeholder="Message"
          onChange={props.onChangeMessage}
          value={props.message}
        />
        <Divider className={props.classes.divider}/>
        <IconButton color="primary" type="submit">
          <SendIcon/>
        </IconButton>
      </Paper>
    </form>
  );
}

function MessageForm(props) {
  const chatRoomService = getInstance(ChatRoomService);
  const [message, setMessage] = useState('');

  const onChangeMessage = event => {
    setMessage(event.target.value);
  };

  const onSubmit = event => {
    event.preventDefault();
    if (!message) {
      return;
    }
    props.sendMessage(message);
    setMessage('');
  };

  props = {
    ...props,
    onChangeMessage,
    onSubmit,
    async sendMessage(message) {
      await chatRoomService.sendMessage(message);
    }
  };

  return <MessageFormView {...props} />
}

export default withStyles(messageFormStyles)(MessageForm);
